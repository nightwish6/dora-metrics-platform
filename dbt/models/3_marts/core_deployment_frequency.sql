{{ config(alias='core_deployment_frequency') }}

SELECT
    repo_name,
    DATE_TRUNC('month', CAST(created_at AS TIMESTAMP)) as period,
    COUNT(*) as release_count
FROM {{ ref('stg_releases') }}
WHERE repo_name IS NOT NULL
GROUP BY repo_name, DATE_TRUNC('month', CAST(created_at AS TIMESTAMP))
