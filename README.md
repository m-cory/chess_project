# Chess Analytics Project

This project ingests Chess.com game data into BigQuery, uses dbt to transform it, and performs additional engine-based analysis.

## Folder Overview

- **dbt/**: dbt models for transforming raw data into a fact table.
- **engine_analysis/**: Python modules for analyzing PGNs with Stockfish.
- **flows/**:
  - **ingest_load_flow.py**: Prefect flow that ingests data from Chess.com and loads to BigQuery.
  - **pgn_analysis_flow.py**: Prefect flow that fetches games from BigQuery and runs engine analysis.
- **pipelines/**:
  - **ingest_chess_data.py**: Functions to fetch monthly games and save locally.
  - **load_data_to_bq.py**: Function to load local JSON into BigQuery.
- **config.py**: Loads environment variables from `.env`.
- **.env**: Stores default parameters (username, year/month, table IDs, engine config).
- **requirements.txt**: Python dependencies (Prefect, python-chess, etc.).

## Usage

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
