{{ config(materialized="table") }}

with
    base as (
        select
            username,
            game_uuid,
            white_username,
            white_rating,
            white_result,
            white_accuracy,

            black_username,
            black_rating,
            black_result,
            black_accuracy,

            time_control,
            time_class,
            rated,
            end_time,
            ingestion_timestamp,
            eco_url,
            rules
        from {{ ref("stg_chess_games") }}
    ),

    perspective as (
        select
            game_uuid,
            username,

            case
                when white_username = username
                then 'white'
                when black_username = username
                then 'black'
                else null
            end as side,

            case
                when white_username = username
                then white_rating
                when black_username = username
                then black_rating
            end as user_rating,
            case
                when white_username = username
                then black_rating
                when black_username = username
                then white_rating
            end as opponent_rating,

            case
                when white_username = username
                then white_result
                when black_username = username
                then black_result
            end as user_ending_reason,
            case
                when white_username = username
                then black_result
                when black_username = username
                then white_result
            end as opponent_ending_reason,

            case
                when white_username = username
                then white_accuracy
                when black_username = username
                then black_accuracy
            end as user_accuracy,
            case
                when white_username = username
                then black_accuracy
                when black_username = username
                then white_accuracy
            end as opponent_accuracy,

            case
                when user_ending_reason = 'win'
                then 'win'
                when user_ending_reason in ('checkmated', 'resigned', 'timeout')
                then 'lose'
                when user_ending_reason in ('stalemate', 'agreed')
                then 'draw'
                else 'unknown'
            end as user_result,

            time_control,
            time_class,
            rated,
            end_time,
            ingestion_timestamp,
            eco_url
        from base
        where
            (white_username = username or black_username = username)
            and rules = 'chess'
    ),

    final as (
        select
            game_uuid,
            username,
            side,
            user_result,
            user_ending_reason,
            opponent_ending_reason,

            user_rating,
            opponent_rating,
            (user_rating - opponent_rating) as rating_diff,

            user_accuracy,
            opponent_accuracy,

            time_control,
            time_class,
            rated,
            end_time,
            ingestion_timestamp,
            eco_url
        from perspective
    )

select *
from final
