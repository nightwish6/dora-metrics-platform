-- Raw issues model
-- Parameters:
--   json_path: Path to JSON file with issue data
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
