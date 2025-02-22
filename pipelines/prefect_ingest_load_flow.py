import argparse
import datetime
from prefect import flow, task
from ingest_chess_data import fetch_monthly_games, save_data_to_file
from load_data_to_bq import load_data  # Import your BigQuery load function


@task(retries=3, retry_delay_seconds=10, name="Fetch Monthly Games")
def task_fetch_monthly_games(username: str, year: int, month: int) -> dict:
    return fetch_monthly_games(username, year, month)


@task(name="Save Data to File")
def task_save_data_to_file(data: dict, output: str):
    save_data_to_file(data, output)


@task(name="Load Data to BigQuery", retries=2, retry_delay_seconds=15)
def task_load_data_to_bq(file_path: str, table_id: str):
    load_data(file_path, table_id)


@flow(name="Chess Data Ingestion and Loading Flow", log_prints=True)
def ingest_and_load_flow(
    username: str, year: int, month: int, output: str, table_id: str
):
    data = task_fetch_monthly_games(username, year, month)
    if data:
        task_save_data_to_file(data, output)
        task_load_data_to_bq(output, table_id)
    else:
        print("No data retrieved.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Prefect flow to ingest Chess.com data and load it into BigQuery."
    )
    parser.add_argument(
        "--username", type=str, required=True, help="Chess.com username"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=datetime.datetime.now().year,
        help="Year for game data",
    )
    parser.add_argument(
        "--month",
        type=int,
        default=datetime.datetime.now().month,
        help="Month for game data (1-12)",
    )
    parser.add_argument(
        "--output", type=str, default="games_data.jsonl", help="Output file name"
    )
    parser.add_argument(
        "--table_id",
        type=str,
        required=True,
        help="BigQuery table ID in the format: project.dataset.table",
    )
    args = parser.parse_args()

    ingest_and_load_flow(
        args.username, args.year, args.month, args.output, args.table_id
    )
