from pathlib import Path
import re
import polars as pl

LOG_PATTERN = re.compile(
    r'(?P<remoteHost>[0-9.]+) '
    r'(?P<userIdentity>\S+) '
    r'(?P<authUser>\S+) '
    r'\[(?P<httpTimestamp>[^\]]+)\] '
    r'"(?P<request>[^"]+)" '
    r'(?P<statusCode>\d{3}) '
    r'(?P<responseTime>\S+) '
    r'"(?P<referrerHeader>[^"]*)" '
    r'"(?P<userAgent>[^"]*)"'
)

def parse_log_file(file_path: Path) -> pl.DataFrame:
    rows = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            match = LOG_PATTERN.match(line)
            if match:
                rows.append(match.groupdict())

    return pl.DataFrame(rows)


def cast_log_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("remoteHost").cast(pl.Utf8),
        pl.col("userIdentity").cast(pl.Utf8),
        pl.col("authUser").cast(pl.Utf8),
        pl.col("httpTimestamp").str.strptime(pl.Datetime, "%d/%b/%Y %H:%M:%S %z"),
        pl.col("request").cast(pl.Utf8),
        pl.col("statusCode").cast(pl.Int16),
        pl.col("responseTime").cast(pl.Int32, strict=False),
        pl.col("referrerHeader").cast(pl.Utf8),
        pl.col("userAgent").cast(pl.Utf8),
    )