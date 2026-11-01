{{ config(materialized='table') }}

SELECT
    repo_name,
    ROUND(AVG(release_count), 2) AS avg_release_count,
    ROUND(AVG(avg_lead_time_hours), 2) AS avg_lead_time_hours,
    ROUND(AVG(cfr_revert_rate), 4) AS avg_cfr,
    ROUND(AVG(avg_restore_days), 2) AS avg_restore_days
FROM (
    SELECT
        df.repo_name,
        df.period,
        df.release_count,
        lt.avg_lead_time_hours,
        cfr.cfr_revert_rate,
        ttr.avg_restore_days
    FROM {{ ref('core_deployment_frequency') }} df
    LEFT JOIN {{ ref('core_lead_time') }} lt
        ON df.repo_name = lt.repo_name
        AND df.period = lt.period
    LEFT JOIN {{ ref('core_cfr') }} cfr
        ON df.repo_name = cfr.repo_name
        AND df.period = cfr.period
    LEFT JOIN {{ ref('core_time_to_restore') }} ttr
        ON df.repo_name = ttr.repo_name
        AND df.period = ttr.period
)
GROUP BY repo_name
