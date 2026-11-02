-- @description Raw issues data loaded from JSON files. Contains GitHub issue/PR information including state, labels, assignees, milestones, and timestamps. Each row represents a unique issue identified by `id` (MD5 of issue number).
-- @parameters:
--   json_path: Path to JSON file with issue data
--   repo_name: Repository name (defaults to 'vscode')
{{ config(
    alias='issues',
    unique_key='id'
) }}
SELECT DISTINCT
    md5(CAST(t.number AS VARCHAR)) AS id,
    CAST(t AS JSON) AS data,
    'issues' AS source_name,
    '{{ var("repo_name", "vscode") }}' AS repo_name,
    current_timestamp AS loaded_at
FROM read_json_auto('{{ var("json_path", "data/raw/vscode_issues.json") }}', ignore_errors = true) t
