import polars as pl
import hashlib


def filter_top_n_by_response_time(df: pl.DataFrame, target_referrer: str, top_n: int = 10) -> pl.DataFrame:
    return (
        df.filter(pl.col("referrerHeader") == target_referrer)
          .sort("responseTime", descending=True)
          .head(top_n)
    )


def aggregate_requests_per_day(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(pl.col("httpTimestamp").dt.truncate("1d").cast(pl.Date).alias("date"))
          .group_by("date")
          .agg(pl.count().alias("count"))
          .sort("count", descending=True)
    )

def get_last_request_by_ip(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("remoteHost")
          .agg(pl.max("httpTimestamp").alias("lastRequest"))
    )

def md5_hash(string: str) -> str:
    return hashlib.md5(string.encode('utf-8')).hexdigest()


def add_extra_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns([
        pl.col("httpTimestamp").dt.strftime("%Y-%m-%d %H:%M:%S").alias("httpTimestampUnixStyle"),
        pl.col("remoteHost").map_elements(md5_hash, return_dtype=str).alias("remoteHostMd5Hashed")
    ])