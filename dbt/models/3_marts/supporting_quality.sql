{{ config(alias='supporting_quality') }}

-- @description Supporting metric: Code Quality. Measures revert rate, GPG signature verification, author/committer mismatch, bug ratio, comment density, quick-win ratio, and backlog aging.
WITH commits AS (
    SELECT repo_name, DATE_TRUNC('month', commit_author_date) as period,
        ROUND(100.0 * SUM(CASE WHEN LOWER(commit_message) LIKE '%revert%' THEN 1 ELSE 0 END) / COUNT(*), 2) as revert_rate,
        ROUND(100.0 * SUM(CASE WHEN commit_verification_verified THEN 1 ELSE 0 END) / COUNT(*), 2) as gpg_verification,
        ROUND(100.0 * SUM(CASE WHEN author_login != committer_login AND author_login IS NOT NULL AND committer_login IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as author_mismatch
    FROM {{ ref('stg_commits') }} WHERE repo_name IS NOT NULL GROUP BY 1, 2
),
issues AS (
    SELECT repo_name, DATE_TRUNC('month', created_at) as period,
        ROUND(100.0 * SUM(CASE WHEN LOWER(labels) LIKE '%bug%' THEN 1 ELSE 0 END) / COUNT(*), 2) as bug_ratio,
        ROUND(AVG(comments), 1) as comment_density,
        ROUND(100.0 * SUM(CASE WHEN closed_at IS NOT NULL AND try_cast(closed_at as timestamp) - try_cast(created_at as timestamp) < INTERVAL '1' DAY THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN LOWER(labels) LIKE '%bug%' THEN 1 ELSE 0 END), 0), 2) as quick_win_ratio,
        ROUND(100.0 * SUM(CASE WHEN state = 'open' AND try_cast(created_at as timestamp) < CURRENT_DATE - INTERVAL '30' DAY THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN LOWER(labels) LIKE '%bug%' THEN 1 ELSE 0 END), 0), 2) as backlog_aging
    FROM {{ ref('stg_issues') }} WHERE repo_name IS NOT NULL GROUP BY 1, 2
)

SELECT
    COALESCE(c.repo_name, i.repo_name) as repo_name,
    COALESCE(c.period, i.period) as period,
    c.revert_rate, c.gpg_verification, c.author_mismatch,
    i.bug_ratio, i.comment_density, i.quick_win_ratio, i.backlog_aging,
    current_timestamp AS loaded_at
FROM commits c
FULL OUTER JOIN issues i ON c.repo_name = i.repo_name AND c.period = i.period
