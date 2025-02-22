{{ config(
    materialized='table'
) }}

with
    raw as (
        select
            json_extract_scalar(raw_json, '$.uuid') as game_uuid,
            json_extract_scalar(raw_json, '$.url') as url,
            json_extract_scalar(raw_json, '$.pgn') as pgn,
            json_extract_scalar(raw_json, '$.time_control') as time_control,
            json_extract_scalar(raw_json, '$.time_class') as time_class,
            json_extract_scalar(raw_json, '$.rules') as rules,
            cast(json_extract_scalar(raw_json, '$.rated') as bool) as rated,

            timestamp_seconds(
                cast(json_extract_scalar(raw_json, '$.end_time') as int64)
            ) as end_time,
            ingestion_timestamp,

            json_extract_scalar(raw_json, '$.white.username') as white_username,
            cast(
                json_extract_scalar(raw_json, '$.white.rating') as int64
            ) as white_rating,
            json_extract_scalar(raw_json, '$.white.result') as white_result,

            json_extract_scalar(raw_json, '$.black.username') as black_username,
            cast(
                json_extract_scalar(raw_json, '$.black.rating') as int64
            ) as black_rating,
            json_extract_scalar(raw_json, '$.black.result') as black_result,

            cast(
                json_extract_scalar(raw_json, '$.accuracies.white') as float64
            ) as white_accuracy,
            cast(
                json_extract_scalar(raw_json, '$.accuracies.black') as float64
            ) as black_accuracy,

            json_extract_scalar(raw_json, '$.eco') as eco_url,  -- e.g. 'https://www.chess.com/openings/Closed-Sicilian-Defense-2...d6'
            json_extract_scalar(raw_json, '$.fen') as fen

        from `mcory-chess-project.chess_raw.raw_chess_games`
    )
select
    game_uuid,
    url,
    pgn,
    time_control,
    time_class,
    rules,
    rated,
    end_time,
    ingestion_timestamp,
    white_username,
    white_rating,
    white_result,
    black_username,
    black_rating,
    black_result,
    white_accuracy,
    black_accuracy,
    eco_url,
    fen
from raw
