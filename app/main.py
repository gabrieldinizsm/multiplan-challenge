from pathlib import Path
import parsing
import transformation
import io_utils

DATA_DIR = Path("data")
OUTPUT_DIR = Path("output")
LOG_FILE = DATA_DIR / "test-access-001-1.log"

TARGET_REFERRER = "http://localhost/svnview?repos=devel&rev=latest&root=SVNview/tmpl&list_revs=1"

def main() -> None:
    """
    Função pra gerir o fluxo geral da aplicação.
    """
    
    io_utils.ensure_output_dir_exists(OUTPUT_DIR)

    df = parsing.parse_log_file(LOG_FILE)
    df = parsing.cast_log_dataframe(df)
    io_utils.write_dataframe_output(df, OUTPUT_DIR / "test-access-001-1.json", "json")

    top_n_df = transformation.filter_top_n_by_response_time(
        df, target_referrer=TARGET_REFERRER, top_n=10
    )
    io_utils.write_dataframe_output(top_n_df, OUTPUT_DIR / "top10_requests_by_response_time.csv", "csv")

    df_hashed = transformation.add_extra_columns(df)
    io_utils.write_dataframe_output(df_hashed, OUTPUT_DIR / "test-access-hashed.csv", "csv")

    df_daily = transformation.aggregate_requests_per_day(df)
    io_utils.write_dataframe_output(df_daily, OUTPUT_DIR / "total-requests-by-day.csv", "csv")

    df_last_request = transformation.get_last_request_by_ip(df)
    io_utils.write_dataframe_output(df_last_request, OUTPUT_DIR / "unique-ips-by-last-request.csv", "csv")


if __name__ == "__main__":
    main()