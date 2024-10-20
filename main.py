import yahooquery
import datetime
import polars as pl
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor
from itertools import batched

BATCH_SIZE = 100


def _load_price_data(batch):
    price_data = (
        yahooquery.Ticker(batch).history(period="max", adj_ohlc=True).reset_index()
    )
    price_data = price_data[
        [not isinstance(row, datetime.datetime) for row in price_data["date"]]
    ].copy()
    price_data["date"] = pd.to_datetime(price_data["date"])
    print(f"LOADED PRICES for {batch}")
    return price_data


def load_prices():
    q = pl.scan_csv("nasdaqtraded.txt", separator="|")
    symbols = (
        q.filter(pl.col("Nasdaq Traded") == "Y").select(pl.col("Symbol")).collect()
    )
    batches = batched(symbols["Symbol"], BATCH_SIZE)
    to_concat = []
    executor = ThreadPoolExecutor()
    for price_data in executor.map(_load_price_data, batches):
        to_concat.append(pl.from_pandas(price_data))
    all_prices = pl.concat(to_concat, how="diagonal_relaxed")
    all_prices.write_parquet("prices.parquet")


def get_prices():
    if not os.path.exists("prices.parquet"):
        load_prices()  # will write prices if not available
    return pl.scan_parquet("prices.parquet")


def main():
    prices = get_prices()
    min_max = (
        prices.group_by(["symbol"])
        .agg(
            [
                pl.col("date").min().name.prefix("min_"),
                pl.col("date").max().name.prefix("max_"),
            ]
        )
        .collect()
    )
    min_max = min_max.with_columns(
        pl.date_ranges(min_max["min_date"], min_max["max_date"], closed="right").alias(
            "effective_date"
        )
    )
    security_df = min_max.explode("effective_date")
    security_df = security_df.with_columns(
        (security_df["effective_date"] - pl.duration(days=1)).alias("previous_date")
    ).cast({pl.Date: pl.Datetime("ns")})
    security_df_lazy = security_df.lazy()
    with_end_price = security_df_lazy.join_asof(
        prices.select(
            [
                pl.col("symbol"),
                pl.col("date").alias("effective_date"),
                pl.col("close").alias("end_price"),
            ]
        ),
        on="effective_date",
        by="symbol",
        tolerance="28d",
        allow_parallel=False,
    )
    with_start_price = with_end_price.join_asof(
        prices.select(
            [
                pl.col("symbol"),
                pl.col("date").alias("previous_date"),
                pl.col("close").alias("start_price"),
            ]
        ),
        on="previous_date",
        by="symbol",
        tolerance="28d",
        allow_parallel=False,
    )
    with_start_price = (
        with_start_price.with_columns(
            norm_return=1.0
            + (pl.col("end_price").sub(pl.col("start_price"))).truediv(
                pl.col("start_price")
            )
        )
        .sort([pl.col("symbol"), pl.col("effective_date")])
        .collect()
    )
    with_start_price = with_start_price.with_columns(
        norm_return_cumulative=with_start_price.group_by("symbol", maintain_order=True)
        .agg(pl.col("norm_return").cum_prod().alias("norm_return_cumulative"))
        .explode("norm_return_cumulative")["norm_return_cumulative"]
    )
    with_start_price.write_parquet("returns.parquet")


if __name__ == "__main__":
    main()
