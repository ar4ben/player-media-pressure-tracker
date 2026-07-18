{{ config(materialized="table") }}

with
    wikipedia_daily as (
        select
            date,
            max(views) filter (where language = 'en') as wikipedia_en_views,
            max(views) filter (where language = 'fr') as wikipedia_fr_views,
            max(views) filter (where language = 'es') as wikipedia_es_views
        from {{ source("silver", "wikipedia") }}
        group by date
    ),

    football_daily as (
        select
            match_date as date,
            1 as football_appearances,
            total_time as football_minutes,
            goals as football_goals,
            penalties as football_penalties,
            missed_penalties as football_missed_penalties,
            yellow_cards as football_yellow_cards,
            red_cards as football_red_cards
        from {{ source("silver", "football_matches") }}
        where total_time is not null
    )

-- Wikipedia contains one row per requested date and provides the calendar spine.
select
    wikipedia.date,

    gdelt.english_coverage,
    gdelt.english_avg_tone,
    gdelt.english_negative_share,
    gdelt.english_high_salience_coverage,
    gdelt.english_high_salience_avg_tone,
    gdelt.english_high_salience_negative_share,
    gdelt.translated_coverage,
    gdelt.translated_avg_tone,
    gdelt.translated_negative_share,
    gdelt.translated_french_coverage,
    gdelt.translated_french_avg_tone,
    gdelt.translated_french_negative_share,
    gdelt.translated_spanish_coverage,
    gdelt.translated_spanish_avg_tone,
    gdelt.translated_spanish_negative_share,
    gdelt.translated_other_coverage,

    wikipedia.wikipedia_en_views,
    wikipedia.wikipedia_fr_views,
    wikipedia.wikipedia_es_views,

    coalesce(football.football_appearances, 0) as football_appearances,
    coalesce(football.football_minutes, 0) as football_minutes,
    coalesce(football.football_goals, 0) as football_goals,
    coalesce(football.football_penalties, 0) as football_penalties,
    coalesce(football.football_missed_penalties, 0) as football_missed_penalties,
    coalesce(football.football_yellow_cards, 0) as football_yellow_cards,
    coalesce(football.football_red_cards, 0) as football_red_cards
from wikipedia_daily as wikipedia
left join {{ ref("gdelt_daily_metrics") }} as gdelt using (date)
left join football_daily as football using (date)
order by wikipedia.date
