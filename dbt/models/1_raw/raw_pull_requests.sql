-- @description Raw pull request data loaded from JSON files. Contains PR metadata including URLs, labels, states, and diff statistics. Each row represents a unique PR identified by `id` (MD5 of PR number).
-- @parameters:
--   json_path: Path to JSON file with PR data
--   repo_name: Repository name (defaults to 'vscode')
{{ config(
    alias='pull_requests',
    unique_key='id'
) }}
SELECT DISTINCT
    md5(CAST(t.number AS VARCHAR)) AS id,
    CAST(t AS JSON) AS data,
    'pull_requests' AS source_name,
    '{{ var("repo_name", "vscode") }}' AS repo_name,
    current_timestamp AS loaded_at
FROM read_json_auto('{{ var("json_path", "data/raw/vscode_pulls.json") }}', ignore_errors = true) t
