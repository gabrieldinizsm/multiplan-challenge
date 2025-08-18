import polars as pl
import app.transformation as tf
from datetime import datetime

def test_filter_top_n_by_response_time():
    target_referrer = "http://xpto.com"

    data = {
        "request": ["GET /manual/page1", "GET /manual/page2", "POST /manual/page3", "GET /other/page"],
        "statusCode": [200, 200, 400, 200],
        "referrerHeader": [target_referrer, target_referrer, target_referrer, target_referrer],
        "responseTime": [100, 300, 200, 400]
    }

    df = pl.DataFrame(data)

    top_df = tf.filter_top_n_by_response_time(df, target_referrer=target_referrer, top_n=2)

    assert top_df.shape[0] == 2
    assert top_df["responseTime"].to_list() == [300, 100]  

    assert all(top_df["request"].to_list()[i].startswith("GET /manual/") for i in range(top_df.shape[0]))
    assert all(top_df["statusCode"] == 200)
    assert all(top_df["referrerHeader"] == target_referrer)

def test_aggregate_requests_per_day():
    df = pl.DataFrame({
        "httpTimestamp": [
            datetime(2025, 8, 17, 10, 0, 0),
            datetime(2025, 8, 17, 12, 0, 0),
            datetime(2025, 8, 18, 9, 0, 0)
        ]
    })

    daily = tf.aggregate_requests_per_day(df)

    assert daily.shape[0] == 2
    assert "count" in daily.columns
    assert daily["count"].sum() == df.shape[0]