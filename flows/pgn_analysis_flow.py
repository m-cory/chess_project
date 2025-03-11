import logging
import pandas as pd
from google.cloud import bigquery
from prefect import flow, task

import config
from engine_analysis.engine_evaluation import analyze_game_user_opponent

logger = logging.getLogger(__name__)

@task
def fetch_games_from_bq(limit: int = None) -> pd.DataFrame:
    """
    Fetches game_uuid, pgn, and side from the fact table in BigQuery.
    If limit is provided and valid, appends a LIMIT clause to the query.
    """
    client = bigquery.Client(project=config.PROJECT_ID)
    query = f"""
    SELECT
        game_uuid,
        pgn,
        side
    FROM `{config.PROJECT_ID}.{config.FACT_DATASET}.{config.FACT_TABLE}`
    WHERE side IS NOT NULL
    """
    # Append the LIMIT clause only if limit is not None (and not the string "None")
    if limit is not None and str(limit).strip().lower() != "none":
        query += f"\nLIMIT {limit}"
    logger.info(f"Running query:\n{query}")
    df = client.query(query).to_dataframe()
    logger.info(f"Fetched {len(df)} rows from BigQuery.")
    return df

@task
def evaluate_games_with_engine(df: pd.DataFrame) -> pd.DataFrame:
    """
    Runs engine analysis on each game's PGN, returning an enriched DataFrame
    with blunder counts and ACPL by phase.
    """
    rows = []
    for _, row in df.iterrows():
        pgn_text = row["pgn"]
        user_side = row["side"]
        analysis_result = analyze_game_user_opponent(
            pgn_text=pgn_text,
            user_side=user_side,
            engine_path=config.STOCKFISH_PATH,
            depth=config.ENGINE_DEPTH,
            blunder_threshold=config.BLUNDER_THRESHOLD,
            opening_threshold=config.OPENING_THRESHOLD,
            endgame_threshold=config.ENDGAME_THRESHOLD
        )
        combined = {**row.to_dict(), **analysis_result}
        rows.append(combined)

    df_enriched = pd.DataFrame(rows)
    logger.info(f"Engine analysis complete for {len(df_enriched)} games.")
    return df_enriched

@task
def load_analysis_to_bq(df: pd.DataFrame):
    """
    Loads the enriched analysis results to the analysis table in BigQuery
    using WRITE_TRUNCATE (overwrite) by default.
    """
    client = bigquery.Client(project=config.PROJECT_ID)
    table_id = f"{config.PROJECT_ID}.{config.ANALYSIS_DATASET}.{config.ANALYSIS_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    logger.info(f"Loading {len(df)} rows to {table_id}...")
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()
    logger.info("Load job finished.")

@flow(name="PGN Engine Analysis Flow", log_prints=True)
def pgn_analysis_flow(limit: int = None):
    """
    Orchestrates:
    1) Fetching games from the fact table (optionally without a limit)
    2) Running engine analysis on the PGNs
    3) Loading the enriched results to the analysis table
    """
    logger.info("Starting PGN Analysis Flow.")
    df = fetch_games_from_bq(limit)
    df_enriched = evaluate_games_with_engine(df)
    load_analysis_to_bq(df_enriched)
    logger.info("PGN Analysis Flow complete.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    pgn_analysis_flow(limit=None)
