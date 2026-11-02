{{ config(alias='core_time_to_restore') }}

-- @description Core DORA metric: Time to Restore Service. Calculates the average time from bug creation to closure for issues labeled as 'bug', broken down by repository and month. Includes P50 and P90 percentiles.
SELECT
    repo_name,
    DATE_TRUNC('month', closed_at) as period,
    COUNT(*) as total_bugs,
    ROUND(AVG(EXTRACT(EPOCH FROM (try_cast(closed_at as timestamp) - try_cast(created_at as timestamp)))/86400), 1) as avg_restore_days,
    ROUND(APPROX_QUANTILE(EXTRACT(EPOCH FROM (try_cast(closed_at as timestamp) - try_cast(created_at as timestamp)))/86400, 0.5), 1) as bug_fix_p50,
    ROUND(APPROX_QUANTILE(EXTRACT(EPOCH FROM (try_cast(closed_at as timestamp) - try_cast(created_at as timestamp)))/86400, 0.9), 1) as bug_fix_p90,
    current_timestamp AS loaded_at
FROM {{ ref('stg_issues') }}
WHERE closed_at IS NOT NULL
    AND LOWER(labels) LIKE '%bug%'
    AND repo_name IS NOT NULL
GROUP BY repo_name, DATE_TRUNC('month', closed_at)
