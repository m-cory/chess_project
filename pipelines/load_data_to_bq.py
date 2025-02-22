import argparse
from google.cloud import bigquery


def load_data(file_path: str, table_id: str):
    client = bigquery.Client()

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,  # Automatically detect the schema based on your file contents
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append to the existing table
    )

    with open(file_path, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, table_id, job_config=job_config
        )

    print("Starting load job...")
    load_job.result()  # Wait for the job to complete
    print("Load job finished.")

    table = client.get_table(table_id)
    print(
        f"Loaded {table.num_rows} rows into {table.project}.{table.dataset_id}.{table.table_id}."
    )


def main():
    parser = argparse.ArgumentParser(
        description="Load a newline-delimited JSON file into a BigQuery table."
    )
    parser.add_argument(
        "--file_path",
        type=str,
        default="games_data.jsonl",
        help="Path to the newline-delimited JSON file to load (default: games_data.jsonl)",
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
    main()
