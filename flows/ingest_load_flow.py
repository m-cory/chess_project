import argparse
import logging
from prefect import flow, task

from pipelines.ingest_chess_data import fetch_monthly_games, save_data_to_file
from pipelines.load_data_to_bq import load_data

import config

logger = logging.getLogger(__name__)


@task(retries=3, retry_delay_seconds=10, name="Fetch Monthly Games")
def task_fetch_monthly_games(username: str, year: int, month: int) -> dict:
    return fetch_monthly_games(username, year, month)


@task(name="Save Data to File")
def task_save_data_to_file(data: dict, output_file: str, username: str) -> None:
    save_data_to_file(data, output_file, username)


@task(name="Load Data to BigQuery", retries=2, retry_delay_seconds=15)
def task_load_data_to_bq(file_path: str, table_id: str) -> None:
    """
    Loads data into BigQuery using the 'load_data' function which defaults to WRITE_APPEND.
    """
    load_data(file_path, table_id)


@flow(name="Chess Data Ingestion and Loading Flow", log_prints=True)
def ingest_and_load_flow(
    username: str = config.CHESS_USERNAME,
    year: int = config.CHESS_YEAR,
    month: int = config.CHESS_MONTH,
    output_file: str = config.OUTPUT_FILE,
    table_id: str = f"{config.PROJECT_ID}.{config.RAW_DATASET}.{config.RAW_TABLE}",
):
    """
    Prefect flow to:
      1) Fetch monthly games from Chess.com
      2) Save to a local JSONL file
      3) Load to BigQuery (appending rows by default)
    """
    logger.info("Starting ingestion & load flow...")
    data = task_fetch_monthly_games(username, year, month)
    if data:
        logger.info("Fetched data successfully, now saving to file...")
        task_save_data_to_file(data, output_file, username)
        logger.info("File saved, now loading to BigQuery with WRITE_APPEND.")
        task_load_data_to_bq(output_file, table_id)
    else:
        logger.warning("No data retrieved from Chess.com API.")
    logger.info("Flow completed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(
        description="Prefect flow to ingest Chess.com data and load into BigQuery."
    )
    parser.add_argument(
        "--username",
        type=str,
        default=config.CHESS_USERNAME,
        help="Chess.com username (default from .env if not provided)",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=config.CHESS_YEAR,
        help="Year for game data (default from .env if not provided)",
    )
    parser.add_argument(
        "--month",
        type=int,
        default=config.CHESS_MONTH,
        help="Month for game data (default from .env if not provided)",
    )
    parser.add_argument(
        "--output_file",
        type=str,
        default=config.OUTPUT_FILE,
        help="Output file name (default from .env if not provided)",
    )
    parser.add_argument(
        "--table_id",
        type=str,
        default=f"{config.PROJECT_ID}.{config.RAW_DATASET}.{config.RAW_TABLE}",
        help="Full table ID to load into, e.g. project.dataset.table (default from .env).",
    )

    args = parser.parse_args()

    # Run the flow with either .env defaults or CLI overrides
    ingest_and_load_flow(
        username=args.username,
        year=args.year,
        month=args.month,
        output_file=args.output_file,
        table_id=args.table_id,
    )
