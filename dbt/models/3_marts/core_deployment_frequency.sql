{{ config(alias='core_deployment_frequency') }}

-- @description Core DORA metric: Deployment Frequency. Counts the number of releases per repository per month to measure how frequently the team ships code to production.
SELECT
    repo_name,
    DATE_TRUNC('month', CAST(created_at AS TIMESTAMP)) as period,
    COUNT(*) as release_count,
    current_timestamp AS loaded_at
FROM {{ ref('stg_releases') }}
WHERE repo_name IS NOT NULL
GROUP BY repo_name, DATE_TRUNC('month', CAST(created_at AS TIMESTAMP))
