{{ config(materialized="table") }}

/*
Detect signal-level spikes using the preceding 52 calendar weeks as a rolling
baseline. The current week is excluded, and at least 12 non-null baseline
observations are required. This keeps the first weeks out of spike detection,
but still allows large early-season events after roughly one quarter of history.
The baseline can be lower than 52 when source data is incomplete (more relevant
to GDELT).

The threshold is defined with Tukey's IQR method:
Q3 + 3 * IQR, where IQR = Q3 - Q1.

Only complete calendar weeks are eligible. GDELT signals additionally require
all seven days of source data.

Each output row represents one signal-level spike and includes its week,
source group, exact metric series, observed value, baseline median, threshold,
and ratio to the baseline median. Weekly media, public-attention, and football
metrics are included as context.
*/

with
    signals as (
        select
            week_start,
            'gdelt_english_coverage' as signal,
            case
                when days_in_period = 7 and gdelt_days_available = 7
                then cast(english_coverage as double)
            end as signal_value
        from {{ ref("player_weekly") }}

        union all

        select
            week_start,
            'gdelt_french_coverage' as signal,
            case
                when days_in_period = 7 and gdelt_days_available = 7
                then cast(translated_french_coverage as double)
            end as signal_value
        from {{ ref("player_weekly") }}

        union all

        select
            week_start,
            'gdelt_spanish_coverage' as signal,
            case
                when days_in_period = 7 and gdelt_days_available = 7
                then cast(translated_spanish_coverage as double)
            end as signal_value
        from {{ ref("player_weekly") }}

        union all

        select
            week_start,
            'wikipedia_en_views' as signal,
            case
                when days_in_period = 7 then cast(wikipedia_en_views as double)
            end as signal_value
        from {{ ref("player_weekly") }}

        union all

        select
            week_start,
            'wikipedia_fr_views' as signal,
            case
                when days_in_period = 7 then cast(wikipedia_fr_views as double)
            end as signal_value
        from {{ ref("player_weekly") }}

        union all

        select
            week_start,
            'wikipedia_es_views' as signal,
            case
                when days_in_period = 7 then cast(wikipedia_es_views as double)
            end as signal_value
        from {{ ref("player_weekly") }}

        union all

        select
            week_start,
            'google_trends_global_interest' as signal,
            case
                when days_in_period = 7
                then cast(google_trends_global_avg_interest as double)
            end as signal_value
        from {{ ref("player_weekly") }}

        union all

        select
            week_start,
            'google_trends_fr_interest' as signal,
            case
                when days_in_period = 7
                then cast(google_trends_fr_avg_interest as double)
            end as signal_value
        from {{ ref("player_weekly") }}

        union all

        select
            week_start,
            'google_trends_es_interest' as signal,
            case
                when days_in_period = 7
                then cast(google_trends_es_avg_interest as double)
            end as signal_value
        from {{ ref("player_weekly") }}
    ),

    baselines as (
        select
            week_start,
            signal,
            signal_value,
            count(signal_value) over baseline_window as baseline_observations,
            median(signal_value) over baseline_window as baseline_median,
            quantile_cont(signal_value, 0.25) over baseline_window as baseline_q1,
            quantile_cont(signal_value, 0.75) over baseline_window as baseline_q3
        from signals
        window baseline_window as (
            partition by signal
            order by week_start
            rows between 52 preceding and 1 preceding
        )
    ),

    scored as (
        select
            *,
            baseline_q3 + 3 * (baseline_q3 - baseline_q1) as spike_threshold
        from baselines
    )

select
    scored.week_start,
    scored.week_start + 6 as week_end,
    case
        when scored.signal like 'gdelt_%' then 'media'
        when scored.signal like 'wikipedia_%' then 'wikipedia'
        else 'google_trends'
    end as signal_group,
    scored.signal,
    scored.signal_value,
    scored.baseline_observations,
    scored.baseline_median,
    scored.spike_threshold,
    scored.signal_value / nullif(scored.baseline_median, 0) as spike_ratio,
    weekly.english_coverage,
    weekly.english_avg_tone,
    weekly.english_negative_share,
    weekly.english_high_salience_coverage,
    weekly.english_high_salience_avg_tone,
    weekly.english_high_salience_negative_share,
    weekly.translated_coverage,
    weekly.translated_avg_tone,
    weekly.translated_negative_share,
    weekly.wikipedia_en_views,
    weekly.wikipedia_fr_views,
    weekly.wikipedia_es_views,
    weekly.google_trends_global_avg_interest,
    weekly.google_trends_fr_avg_interest,
    weekly.google_trends_es_avg_interest,
    weekly.football_appearances,
    weekly.football_minutes,
    weekly.football_goals
from scored
inner join {{ ref("player_weekly") }} as weekly using (week_start)
where
    scored.baseline_observations >= 12
    and scored.signal_value > scored.spike_threshold
order by scored.week_start, scored.signal
