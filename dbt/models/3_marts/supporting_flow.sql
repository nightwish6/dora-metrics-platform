{{ config(alias='supporting_flow') }}

WITH pr AS (
    SELECT repo_name, DATE_TRUNC('month', created_at) as period,
        ROUND(100.0 * SUM(CASE WHEN merged_at IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pr_merged_rate,
        ROUND(AVG(EXTRACT(EPOCH FROM (try_cast(closed_at as timestamp) - try_cast(created_at as timestamp)))/3600), 1) as pr_closed_time,
        ROUND(100.0 * SUM(CASE WHEN assignee_login IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as assignee_coverage,
        ROUND(100.0 * SUM(CASE WHEN state = 'open' AND try_cast(created_at as timestamp) < CURRENT_DATE - INTERVAL '7' DAY THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN state = 'open' THEN 1 ELSE 0 END), 0), 2) as stale_pr_rate,
        ROUND(100.0 * SUM(CASE WHEN draft = true THEN 1 ELSE 0 END) / COUNT(*), 2) as pr_draft_rate,
        ROUND(100.0 * SUM(CASE WHEN locked = true THEN 1 ELSE 0 END) / COUNT(*), 2) as locked_pr_ratio
    FROM {{ ref('stg_pull_requests') }} WHERE repo_name IS NOT NULL GROUP BY 1, 2
),
closed_issues AS (
    SELECT repo_name, DATE_TRUNC('month', created_at) as period, COUNT(*) as cnt FROM {{ ref('stg_issues') }} WHERE state = 'closed' AND repo_name IS NOT NULL GROUP BY 1, 2
),
closed_prs AS (
    SELECT repo_name, DATE_TRUNC('month', created_at) as period, COUNT(*) as cnt FROM {{ ref('stg_pull_requests') }} WHERE state = 'closed' AND repo_name IS NOT NULL GROUP BY 1, 2
),
locked_issues AS (
    SELECT repo_name, DATE_TRUNC('month', created_at) as period,
        ROUND(100.0 * SUM(CASE WHEN locked = true THEN 1 ELSE 0 END) / COUNT(*), 2) as locked_issue_ratio
    FROM {{ ref('stg_issues') }} WHERE repo_name IS NOT NULL GROUP BY 1, 2
)

SELECT 
    COALESCE(p.repo_name, ci.repo_name, cp.repo_name, li.repo_name) as repo_name, 
    COALESCE(p.period, ci.period, cp.period, li.period) as period,
    p.pr_merged_rate, p.pr_closed_time, p.assignee_coverage, p.stale_pr_rate,
    ROUND(1.0 * COALESCE(cp.cnt, 0) / NULLIF(ci.cnt, 0), 2) as issue_to_pr_ratio,
    p.pr_draft_rate, p.locked_pr_ratio, li.locked_issue_ratio
FROM pr p
LEFT JOIN closed_issues ci ON p.repo_name = ci.repo_name AND p.period = ci.period
LEFT JOIN closed_prs cp ON p.repo_name = cp.repo_name AND p.period = cp.period
LEFT JOIN locked_issues li ON p.repo_name = li.repo_name AND p.period = li.period
