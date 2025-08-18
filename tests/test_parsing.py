from __future__ import annotations

from pathlib import Path

import polars as pl

import app.parsing as parsing


def test_parse_log_file(tmp_path: Path):
    log_content = (
        '127.0.0.1 - - [12/Jan/2020 02:57:26 +0200] '
        '"GET /apache_pb.gif HTTP/1.1" 200 2326 '
        '"http://localhost/manual" "Mozilla/5.0 (Windows NT 10.0; WOW64)"\n'
    )

    file_path = tmp_path / 'test.log'
    file_path.write_text(log_content)

    df = parsing.parse_log_file(file_path)

    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] == 1
    assert df['remoteHost'][0] == '127.0.0.1'
    assert df.width == 9


def test_cast_log_dataframe():
    raw_df = pl.DataFrame({
        'remoteHost': ['127.0.0.1'],
        'userIdentity': ['-'],
        'authUser': ['-'],
        'httpTimestamp': ['12/Jan/2020 02:57:26 +0200'],
        'request': ['GET /apache_pb.gif HTTP/1.1'],
        'statusCode': ['200'],
        'responseTime': ['2326'],
        'referrerHeader': ['http://localhost/manual'],
        'userAgent': ['Mozilla/5.0 (Windows NT 10.0; WOW64)']
    })

    mock_df = parsing.cast_log_dataframe(raw_df)

    assert mock_df['statusCode'].dtype == pl.Int16
    assert mock_df['httpTimestamp'].dtype == pl.Datetime
    assert mock_df['responseTime'].dtype == pl.Int32
