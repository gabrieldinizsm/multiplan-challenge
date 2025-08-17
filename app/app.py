import polars as pl
import re
import os
import hashlib

def parse_log_file(file_path: str) -> list:

    log_pattern = re.compile(
        r'(?P<remoteHost>\S+) '             
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
    return parsed_rows


def filter_top10_df_by_response_time(df: pl.DataFrame) -> pl.DataFrame:

    target_referrer = "http://localhost/svnview?repos=devel&rev=latest&root=SVNview/tmpl&list_revs=1"

    top10 = (
        df.filter(pl.col("referrerHeader") == target_referrer)  
        .sort("responseTime", descending=True)                    
        .head(10)                                    
    )

    return top10

def md5_hash(s: str) -> str:
    return hashlib.md5(s.encode('utf-8')).hexdigest()

def main () -> None:

    parsed_rows = parse_log_file(os.path.join('data/', 'test-access-001-1.log'))
    df = pl.DataFrame(parsed_rows)

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

  
if __name__ == '__main__':
    main()