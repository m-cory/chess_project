import requests
import json
import datetime
import logging

logger = logging.getLogger(__name__)

def fetch_monthly_games(username: str, year: int, month: int) -> dict:
    """
    Fetches a month's worth of Chess.com games for the given username, year, and month.
    Returns a dict from the Chess.com Published Data API.
    """
    url = f"https://api.chess.com/pub/player/{username}/games/{year}/{month:02d}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }
    logger.info(f"Fetching data from: {url}")
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        logger.info("Data fetched successfully!")
        return response.json()
    else:
        logger.warning(f"Error fetching data: {response.status_code} - {response.text}")
        return {}

def save_data_to_file(data: dict, filename: str, username: str) -> None:
    """
    Saves the 'games' from the API response to a newline-delimited JSON file,
    adding extra fields like 'username' and 'ingestion_timestamp'.
    """
    games = data.get("games", [])
    now = datetime.datetime.utcnow().isoformat()  # UTC ingestion timestamp
    count = 0
    with open(filename, "w") as f:
        for game in games:
            record = {
                "raw_json": json.dumps(game),
                "username": username,
                "ingestion_timestamp": now,
            }
            f.write(json.dumps(record) + "\n")
            count += 1
    logger.info(f"Saved {count} game records to {filename}")
