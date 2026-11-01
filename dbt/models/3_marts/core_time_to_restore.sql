{{ config(alias='core_time_to_restore') }}

SELECT 
    repo_name,
    DATE_TRUNC('month', closed_at) as period,
    COUNT(*) as total_bugs,
    ROUND(AVG(EXTRACT(EPOCH FROM (try_cast(closed_at as timestamp) - try_cast(created_at as timestamp)))/86400), 1) as avg_restore_days,
    ROUND(APPROX_QUANTILE(EXTRACT(EPOCH FROM (try_cast(closed_at as timestamp) - try_cast(created_at as timestamp)))/86400, 0.5), 1) as bug_fix_p50,
    ROUND(APPROX_QUANTILE(EXTRACT(EPOCH FROM (try_cast(closed_at as timestamp) - try_cast(created_at as timestamp)))/86400, 0.9), 1) as bug_fix_p90
FROM {{ ref('stg_issues') }}
WHERE closed_at IS NOT NULL
    AND LOWER(labels) LIKE '%bug%'
    AND repo_name IS NOT NULL
GROUP BY repo_name, DATE_TRUNC('month', closed_at)
