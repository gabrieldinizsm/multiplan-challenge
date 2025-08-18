import polars as pl
import pytest
import app.io_utils as io_utils
import json

def test_write_dataframe_output_csv(tmp_path):
    df = pl.DataFrame({
        "col1": [1, 2, 3],
        "col2": ["a", "b", "c"]
    })
    file_path = tmp_path / "test.csv"

    print(df.head())

    io_utils.write_dataframe_output(df, file_path, "csv")

    df_read = pl.read_csv(file_path)

    assert df_read.equals(df)