-- Raw pull requests model
-- Parameters:
--   json_path: Path to JSON file with pull request data
{{ config(
    alias='pull_requests',
    unique_key='id'
) }}
SELECT
    md5(CAST(t.number AS VARCHAR)) AS id,
    CAST(t AS JSON) AS data,
    'pull_requests' AS source_name,
    '{{ var("repo_name", "vscode") }}' AS repo_name,
    now() AS loaded_at
FROM read_json_auto('{{ var("json_path", "data/raw/vscode_pulls.json") }}', ignore_errors = true) t
