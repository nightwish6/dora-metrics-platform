-- Raw commits model
-- Parameters:
--   json_path: Path to JSON file with commit data
{{ config(
    alias='commits',
    unique_key='id'
) }}
SELECT DISTINCT
    md5(CAST(t.sha AS VARCHAR)) AS id,
    CAST(t AS JSON) AS data,
    'commits' AS source_name,
    '{{ var("repo_name", "vscode") }}' AS repo_name,
    current_timestamp AS loaded_at
FROM read_json_auto('{{ var("json_path", "data/raw/vscode_commits.json") }}', ignore_errors = true) t
