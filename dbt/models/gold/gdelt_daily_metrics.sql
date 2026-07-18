{{ config(materialized="table") }}

select
    gkg_date as date,
    count(*) filter (where stream = 'regular') as english_coverage,
    avg(tone) filter (where stream = 'regular') as english_avg_tone,
    avg(
        case
            when tone < 0 then 1.0
            when tone >= 0 then 0.0
        end
    ) filter (
        where stream = 'regular'
    ) as english_negative_share,
    count(*) filter (
        where stream = 'regular' and salience_score >= 0.70
    ) as english_high_salience_coverage,
    avg(tone) filter (
        where stream = 'regular' and salience_score >= 0.70
    ) as english_high_salience_avg_tone,
    avg(
        case
            when tone < 0 then 1.0
            when tone >= 0 then 0.0
        end
    ) filter (
        where stream = 'regular' and salience_score >= 0.70
    ) as english_high_salience_negative_share,
    count(*) filter (where stream = 'translation') as translated_coverage,
    avg(tone) filter (where stream = 'translation') as translated_avg_tone,
    avg(
        case
            when tone < 0 then 1.0
            when tone >= 0 then 0.0
        end
    ) filter (
        where stream = 'translation'
    ) as translated_negative_share,
    count(*) filter (
        where stream = 'translation' and source_language = 'fra'
    ) as translated_french_coverage,
    avg(tone) filter (
        where stream = 'translation' and source_language = 'fra'
    ) as translated_french_avg_tone,
    avg(
        case
            when tone < 0 then 1.0
            when tone >= 0 then 0.0
        end
    ) filter (
        where stream = 'translation' and source_language = 'fra'
    ) as translated_french_negative_share,
    count(*) filter (
        where stream = 'translation' and source_language = 'spa'
    ) as translated_spanish_coverage,
    avg(tone) filter (
        where stream = 'translation' and source_language = 'spa'
    ) as translated_spanish_avg_tone,
    avg(
        case
            when tone < 0 then 1.0
            when tone >= 0 then 0.0
        end
    ) filter (
        where stream = 'translation' and source_language = 'spa'
    ) as translated_spanish_negative_share,
    count(*) filter (
        where
            stream = 'translation'
            and source_language not in ('fra', 'spa')
    ) as translated_other_coverage
from {{ source("silver", "gdelt_articles") }}
group by gkg_date
order by gkg_date
