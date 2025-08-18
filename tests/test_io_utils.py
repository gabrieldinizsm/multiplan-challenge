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

    io_utils.write_dataframe_output(df, file_path, "csv")

    df_read = pl.read_csv(file_path)

    assert file_path.exists()
    assert df_read.equals(df)


def test_test_write_dataframe_output_json(tmp_path):
    df = pl.DataFrame({
        "col1": [1, 2],
        "col2": ["x", "y"]
    })
    file_path = tmp_path / "test.json"

    io_utils.write_dataframe_output(df, file_path, "json")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    expected = [{"col1": 1, "col2": "x"}, {"col1": 2, "col2": "y"}]

    assert file_path.exists()
    assert data == expected
