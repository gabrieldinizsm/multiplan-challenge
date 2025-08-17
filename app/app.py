import polars as pl
import re
import os

def parse_log_file(file_path: str) -> list:

    log_pattern = re.compile(
        r'(?P<remoteHost>\S+) '             
        r'(?P<userIdentity>\S+) '
        r'(?P<authUser>\S+) ' 
        r'\[(?P<httpTimestamp>[^\]]+)\] ' 
        r'"(?P<request>\S+) (?P<path>\S+) (?P<protocol>\S+)" '  
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

def main () -> None:

    parsed_rows = parse_log_file(os.path.join('data/', 'test-access-001-1.log'))
    df = pl.DataFrame(parsed_rows)

if __name__ == '__main__':
    main()