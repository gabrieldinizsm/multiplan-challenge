import polars as pl
import re
import os
import hashlib

def parse_log_file(file_path: str) -> pl.DataFrame:

    log_pattern = re.compile(
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

    parsed_rows = []

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            match = log_pattern.match(line)
            if match:
                parsed_rows.append(match.groupdict())
    return pl.DataFrame(parsed_rows)


def filter_top10_df_by_response_time(df: pl.DataFrame, target_referrer: str, top_n: int = 10) -> pl.DataFrame:
    return (
        df.filter(pl.col("referrerHeader") == target_referrer)  
        .sort("responseTime", descending=True)                    
        .head(top_n)                                    
    )

def md5_hash(s: str) -> str:
    return hashlib.md5(s.encode('utf-8')).hexdigest()

def aggregate_dataframe_by_requests_per_day(df: pl.DataFrame) -> pl.DataFrame:

    df_daily = (
        df.with_columns(
            pl.col("httpTimestamp").dt.truncate("1d").cast(pl.Date).alias("date")  
        )
        .group_by("date")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True) 
    )

    return df_daily

def get_last_request_date_by_ip (df: pl.DataFrame) -> pl.DataFrame:

    df_sorted = df.sort("httpTimestamp", descending=True).unique(subset="remoteHost", keep="first").select(pl.col('remoteHost', 'httpTimestamp'))
    return df_sorted

def main () -> None:

    df = parse_log_file(os.path.join('data/', 'test-access-001-1.log'))

    df = df.with_columns(
        pl.col("remoteHost").cast(pl.Utf8),
        pl.col("userIdentity").cast(pl.Utf8),
        pl.col("authUser").cast(pl.Utf8),
        pl.col("httpTimestamp").str.strptime(pl.Datetime, '%d/%b/%Y %H:%M:%S %z'),
        pl.col("request").cast(pl.Utf8),
        pl.col("statusCode").cast(pl.Int16),
        pl.col("responseTime").cast(pl.Int32, strict=False),
        pl.col("referrerHeader").cast(pl.Utf8),
        pl.col("userAgent").cast(pl.Utf8),
    )

    df.write_json(os.path.join('output/', 'test-acess-001-1.json'))
    
    target_referrer = "http://localhost/svnview?repos=devel&rev=latest&root=SVNview/tmpl&list_revs=1"

    top10_df = filter_top10_df_by_response_time(df, target_referrer)

    top10_df.write_json(os.path.join('output/', 'top10_requests_by_response_time.json'))

    df = df.with_columns(
        pl.col('httpTimestamp').dt.strftime('%Y-%m-%d %H:%M:%S').alias('httpTimestampUnixStyle'), 
        pl.col('remoteHost').map_elements(md5_hash, return_dtype=str).alias('remoteHostMd5Hashed')
    )   

    df.write_csv(os.path.join('output/', 'test-access-hashed.txt'))

    df_daily = aggregate_dataframe_by_requests_per_day(df)

    df_daily.write_csv(os.path.join('output/', 'total-requests-by-day.csv'))

    df_unique_ip_last_request = get_last_request_date_by_ip(df)

    df_unique_ip_last_request.write_csv(os.path.join('output/', 'unique-ips-by-last-request.csv'))

if __name__ == '__main__':
    main()