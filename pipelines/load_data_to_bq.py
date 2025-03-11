import logging
import argparse
from google.cloud import bigquery

logger = logging.getLogger(__name__)

def load_data(file_path: str, table_id: str) -> None:
    """
    Loads a newline-delimited JSON file into a BigQuery table (append mode).
    The schema is auto-detected based on the file contents.
    """
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    logger.info(f"Starting load job for {file_path} into {table_id}...")
    with open(file_path, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, table_id, job_config=job_config
        )
    load_job.result()  # Wait for the job to complete
    logger.info("Load job finished.")

    table = client.get_table(table_id)
    logger.info(
        f"Loaded {table.num_rows} total rows into {table.project}.{table.dataset_id}.{table.table_id}."
    )

def main():
    """
    Optional standalone CLI usage: python load_data_to_bq.py --file_path <...> --table_id <...>
    """
    parser = argparse.ArgumentParser(
        description="Load a newline-delimited JSON file into a BigQuery table."
    )
    parser.add_argument(
        "--file_path",
        type=str,
        default="games_data.jsonl",
        help="Path to the newline-delimited JSON file (default: games_data.jsonl)",
    )
    parser.add_argument(
        "--table_id",
        type=str,
        required=True,
        help="BigQuery table ID in the format: project.dataset.table",
    )
    args = parser.parse_args()
    load_data(args.file_path, args.table_id)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
