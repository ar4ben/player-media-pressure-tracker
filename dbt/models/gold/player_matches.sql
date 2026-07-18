{{ config(materialized="table") }}

with
    analysis_period as (
        select min(date) as start_date, max(date) as end_date
        from {{ ref("player_daily") }}
    )

select
    matches.match_date as date,
    matches.competition,
    matches.team1,
    matches.score,
    matches.team2,
    matches.total_time as minutes,
    matches.goals,
    matches.penalties,
    matches.missed_penalties,
    matches.yellow_cards,
    matches.red_cards
from {{ source("silver", "football_matches") }} as matches
cross join analysis_period
where
    matches.total_time is not null
    and matches.match_date between analysis_period.start_date and analysis_period.end_date
order by matches.match_date
