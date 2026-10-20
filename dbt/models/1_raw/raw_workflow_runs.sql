-- Raw workflow runs model
-- Parameters:
--   json_path: Path to JSON file with workflow run data
{{ config(
    alias='workflow_runs',
    unique_key='id'
) }}
SELECT
    md5(CAST(t AS VARCHAR)) AS id,
    CAST(t AS JSON) AS data,
    'workflow_runs' AS source_name,
    '{{ var("repo_name", "vscode") }}' AS repo_name,
    now() AS loaded_at
FROM read_json_auto('{{ var("json_path", "data/raw/vscode_workflow_runs.json") }}', ignore_errors = true) t
