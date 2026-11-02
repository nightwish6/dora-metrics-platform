-- @description Raw GitHub Actions workflow run data loaded from JSON files. Contains CI/CD pipeline execution information including status, conclusion, head SHA, and timestamps. Each row represents a unique workflow run identified by `id`.
-- @parameters:
--   json_path: Path to JSON file with workflow run data
--   repo_name: Repository name (defaults to 'vscode')
{{ config(
    alias='workflow_runs',
    unique_key='id'
) }}
SELECT DISTINCT
    md5(CAST(t AS VARCHAR)) AS id,
    CAST(t AS JSON) AS data,
    'workflow_runs' AS source_name,
    '{{ var("repo_name", "vscode") }}' AS repo_name,
    current_timestamp AS loaded_at
FROM read_json_auto('{{ var("json_path", "data/raw/vscode_workflow_runs.json") }}', ignore_errors = true) t
