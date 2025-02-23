# Chess Analytics dbt Project

This project transforms raw Chess.com game data into an analytics-ready fact table from my perspective.

## Project Structure

- **dbt/**: Contains all dbt project files.
  - **models/staging/**: Raw transformations from the raw data table.
  - **models/analytics/**: Refined fact tables for analysis.
- **pipelines/**: Python scripts for ingestion and loading.
- **notebooks/**: Jupyter notebooks for exploratory analysis.

## Testing & Documentation

- Run tests with: `dbt test --models fact_my_games`
- Generate documentation with: `dbt docs generate` then `dbt docs serve`
