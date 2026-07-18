{{ config(materialized="table") }}

with
    daily_with_week as (
        select
            *,
            (
                date - cast(extract('dayofweek' from date) as integer)
            )::date as week_start
        from {{ ref("player_daily") }}
    ),

    daily_rollup as (
        select
            week_start,
            count(*) as days_in_period,
            count(english_coverage) as gdelt_days_available,
            sum(english_coverage) as english_coverage,
            sum(english_avg_tone * english_coverage)
            / nullif(sum(english_coverage), 0) as english_avg_tone,
            sum(english_negative_share * english_coverage)
            / nullif(sum(english_coverage), 0) as english_negative_share,
            sum(english_high_salience_coverage) as english_high_salience_coverage,
            sum(english_high_salience_avg_tone * english_high_salience_coverage)
            / nullif(
                sum(english_high_salience_coverage), 0
            ) as english_high_salience_avg_tone,
            sum(
                english_high_salience_negative_share
                * english_high_salience_coverage
            )
            / nullif(
                sum(english_high_salience_coverage), 0
            ) as english_high_salience_negative_share,
            sum(translated_coverage) as translated_coverage,
            sum(translated_avg_tone * translated_coverage)
            / nullif(sum(translated_coverage), 0) as translated_avg_tone,
            sum(translated_negative_share * translated_coverage)
            / nullif(sum(translated_coverage), 0) as translated_negative_share,
            sum(translated_french_coverage) as translated_french_coverage,
            sum(translated_french_avg_tone * translated_french_coverage)
            / nullif(
                sum(translated_french_coverage), 0
            ) as translated_french_avg_tone,
            sum(translated_french_negative_share * translated_french_coverage)
            / nullif(
                sum(translated_french_coverage), 0
            ) as translated_french_negative_share,
            sum(translated_spanish_coverage) as translated_spanish_coverage,
            sum(translated_spanish_avg_tone * translated_spanish_coverage)
            / nullif(
                sum(translated_spanish_coverage), 0
            ) as translated_spanish_avg_tone,
            sum(translated_spanish_negative_share * translated_spanish_coverage)
            / nullif(
                sum(translated_spanish_coverage), 0
            ) as translated_spanish_negative_share,
            sum(translated_other_coverage) as translated_other_coverage,
            sum(wikipedia_en_views) as wikipedia_en_views,
            sum(wikipedia_fr_views) as wikipedia_fr_views,
            sum(wikipedia_es_views) as wikipedia_es_views,
            sum(football_appearances) as football_appearances,
            sum(football_minutes) as football_minutes,
            sum(football_goals) as football_goals,
            sum(football_penalties) as football_penalties,
            sum(football_missed_penalties) as football_missed_penalties,
            sum(football_yellow_cards) as football_yellow_cards,
            sum(football_red_cards) as football_red_cards
        from daily_with_week
        group by week_start
    ),

    google_trends_weekly as (
        select
            week_start,
            max(interest) filter (
                where audience_scope = 'global'
            ) as google_trends_global_avg_interest,
            max(interest) filter (
                where audience_scope = 'fr'
            ) as google_trends_fr_avg_interest,
            max(interest) filter (
                where audience_scope = 'es'
            ) as google_trends_es_avg_interest
        from {{ source("silver", "google_trends") }}
        group by week_start
    )

select
    daily_rollup.*,
    google_trends.google_trends_global_avg_interest,
    google_trends.google_trends_fr_avg_interest,
    google_trends.google_trends_es_avg_interest
from daily_rollup
left join google_trends_weekly as google_trends using (week_start)
order by week_start
