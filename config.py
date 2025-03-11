import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CHESS_USERNAME = os.getenv("CHESS_USERNAME", "mcory")

PROJECT_ID = os.getenv("PROJECT_ID", "mcory-chess-project")
RAW_DATASET = os.getenv("RAW_DATASET", "chess_raw")
RAW_TABLE = os.getenv("RAW_TABLE", "raw_chess_games")
FACT_DATASET = os.getenv("FACT_DATASET", "chess_analytics")
FACT_TABLE = os.getenv("FACT_TABLE", "fact_my_games")
ANALYSIS_DATASET = os.getenv("ANALYSIS_DATASET", "chess_analytics")
ANALYSIS_TABLE = os.getenv("ANALYSIS_TABLE", "fact_game_analysis")

STOCKFISH_PATH = os.getenv("STOCKFISH_PATH", "/generic/path/to/stockfish")
ENGINE_DEPTH = int(os.getenv("DEPTH", "12"))
BLUNDER_THRESHOLD = int(os.getenv("BLUNDER_THRESHOLD", "200"))
OPENING_THRESHOLD = int(os.getenv("OPENING_THRESHOLD", "60"))
ENDGAME_THRESHOLD = int(os.getenv("ENDGAME_THRESHOLD", "20"))

CHESS_YEAR = int(os.getenv("CHESS_YEAR", "2023"))
CHESS_MONTH = int(os.getenv("CHESS_MONTH", "1"))
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "games_data.jsonl")

logger.info("Config loaded from .env (or defaults).")
logger.info(f"CHESS_USERNAME={CHESS_USERNAME}")
logger.info(f"RAW TABLE: {PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}")
logger.info(f"PROJECT_ID={PROJECT_ID}, FACT_DATASET={FACT_DATASET}, FACT_TABLE={FACT_TABLE}")
logger.info(f"ANALYSIS_DATASET={ANALYSIS_DATASET}, ANALYSIS_TABLE={ANALYSIS_TABLE}")
logger.info(f"STOCKFISH_PATH={STOCKFISH_PATH}, ENGINE_DEPTH={ENGINE_DEPTH}")
logger.info(f"BLUNDER_THRESHOLD={BLUNDER_THRESHOLD}, OPENING_THRESHOLD={OPENING_THRESHOLD}, ENDGAME_THRESHOLD={ENDGAME_THRESHOLD}")
logger.info(f"CHESS_YEAR={CHESS_YEAR}, CHESS_MONTH={CHESS_MONTH}, OUTPUT_FILE={OUTPUT_FILE}")