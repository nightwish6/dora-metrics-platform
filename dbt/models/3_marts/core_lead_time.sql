{{ config(alias='core_lead_time') }}

SELECT 
    repo_name,
    DATE_TRUNC('month', created_at) as period,
    COUNT(*) as total_prs,
    ROUND(AVG(EXTRACT(EPOCH FROM (try_cast(merged_at as timestamp) - try_cast(created_at as timestamp)))/3600), 1) as avg_lead_time_hours,
    ROUND(APPROX_QUANTILE(EXTRACT(EPOCH FROM (try_cast(merged_at as timestamp) - try_cast(created_at as timestamp)))/3600, 0.5), 1) as p50_lead_time_hours,
    ROUND(APPROX_QUANTILE(EXTRACT(EPOCH FROM (try_cast(merged_at as timestamp) - try_cast(created_at as timestamp)))/3600, 0.9), 1) as p90_lead_time_hours
FROM {{ ref('stg_pull_requests') }}
WHERE merged_at IS NOT NULL AND repo_name IS NOT NULL
GROUP BY repo_name, DATE_TRUNC('month', created_at)
