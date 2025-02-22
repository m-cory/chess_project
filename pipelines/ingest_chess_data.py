import requests
import json
import datetime
import argparse


def fetch_monthly_games(username: str, year: int, month: int) -> dict:
    url = f"https://api.chess.com/pub/player/{username}/games/{year}/{month:02d}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
        "Accept": "application/json",
    }
    print(f"Fetching data from: {url}")
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print("Data fetched successfully!")
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code} - {response.text}")
        return None


def save_data_to_file(data: dict, filename: str):
    games = data.get("games", [])
    now = datetime.datetime.utcnow().isoformat()  # UTC ingestion timestamp
    count = 0
    with open(filename, "w") as f:
        for game in games:
            record = {"raw_json": json.dumps(game), "ingestion_timestamp": now}
            f.write(json.dumps(record) + "\n")
            count += 1
    print(f"Saved {count} game records to {filename}")


def main():
    parser = argparse.ArgumentParser(
        description="Ingest Chess.com games for a given username, year, and month."
    )
    parser.add_argument(
        "--username", type=str, required=True, help="Chess.com username"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=datetime.datetime.now().year,
        help="Year of the games",
    )
    parser.add_argument(
        "--month",
        type=int,
        default=datetime.datetime.now().month,
        help="Month of the games (1-12)",
    )
    parser.add_argument(
        "--output", type=str, default="games_data.jsonl", help="Output file name"
    )

    args = parser.parse_args()

    data = fetch_monthly_games(args.username, args.year, args.month)
    if data:
        save_data_to_file(data, args.output)
    else:
        print("No data retrieved.")


if __name__ == "__main__":
    main()
