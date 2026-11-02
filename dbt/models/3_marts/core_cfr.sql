{{ config(alias='core_cfr') }}

-- @description Core DORA metric: Change Failure Rate (CFR). Calculates the percentage of commits that result in a revert (commit message contains 'revert'), indicating the stability of changes deployed to production.
SELECT
    repo_name,
    DATE_TRUNC('month', commit_author_date) as period,
    COUNT(*) as total_commits,
    ROUND(100.0 * SUM(CASE WHEN LOWER(commit_message) LIKE '%revert%' THEN 1 ELSE 0 END) / COUNT(*), 2) as cfr_revert_rate,
    current_timestamp AS loaded_at
FROM {{ ref('stg_commits') }}
WHERE repo_name IS NOT NULL
GROUP BY repo_name, DATE_TRUNC('month', commit_author_date)
