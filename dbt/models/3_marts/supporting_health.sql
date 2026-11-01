{{ config(alias='supporting_health') }}

WITH base_commits AS (
    SELECT repo_name, author_login, DATE_TRUNC('month', CAST(commit_author_date AS TIMESTAMP)) as period
    FROM {{ ref('stg_commits') }}
    WHERE repo_name IS NOT NULL
),
commits AS (
    SELECT 
        repo_name,
        period,
        COUNT(*) as commit_count,
        ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT author_login), 1) as commit_velocity,
        ROUND(100.0 * MAX(cnt) / COUNT(*), 1) as top_author_share
    FROM (
        SELECT repo_name, author_login, period,
            COUNT(*) as cnt
        FROM base_commits
        GROUP BY repo_name, author_login, period
    ) author_counts
    GROUP BY repo_name, period
),
milestones AS (
    SELECT repo_name, DATE_TRUNC('month', CAST(created_at AS TIMESTAMP)) as period,
        ROUND(100.0 * SUM(CASE WHEN try_cast(closed_at as timestamp) <= try_cast(due_on as timestamp) THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN closed_at IS NOT NULL THEN 1 ELSE 0 END), 0), 2) as on_time_milestone,
        ROUND(100.0 * SUM(CASE WHEN title IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as milestone_coverage
    FROM {{ ref('stg_milestones') }} WHERE repo_name IS NOT NULL GROUP BY 1, 2
),
releases AS (
    SELECT repo_name, DATE_TRUNC('month', CAST(created_at AS TIMESTAMP)) as period, COUNT(*) as cnt FROM {{ ref('stg_releases') }} WHERE repo_name IS NOT NULL GROUP BY 1, 2
),
closed_prs AS (
    SELECT repo_name, DATE_TRUNC('month', CAST(created_at AS TIMESTAMP)) as period, COUNT(*) as cnt FROM {{ ref('stg_pull_requests') }} WHERE state = 'closed' AND repo_name IS NOT NULL GROUP BY 1, 2
)

SELECT 
    COALESCE(c.repo_name, m.repo_name, r.repo_name) as repo_name, 
    COALESCE(c.period, m.period, r.period) as period,
    c.commit_count, c.commit_velocity, c.top_author_share,
    m.on_time_milestone, m.milestone_coverage,
    ROUND(1.0 * COALESCE(cp.cnt, 0) / NULLIF(r.cnt, 0), 1) as release_pr_size
FROM commits c
FULL OUTER JOIN milestones m ON c.repo_name = m.repo_name AND c.period = m.period
FULL OUTER JOIN releases r ON c.repo_name = r.repo_name AND c.period = r.period
LEFT JOIN closed_prs cp ON c.repo_name = cp.repo_name AND c.period = cp.period
