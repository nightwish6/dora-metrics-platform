-- Raw milestones model
-- Parameters:
--   json_path: Path to JSON file with milestone data
{{ config(
    alias='milestones',
    unique_key='id'
) }}
SELECT
    md5(CAST(t.id AS VARCHAR)) AS id,
    CAST(t AS JSON) AS data,
    'milestones' AS source_name,
    '{{ var("repo_name", "vscode") }}' AS repo_name,
    now() AS loaded_at
FROM read_json_auto('{{ var("json_path", "data/raw/vscode_milestones.json") }}', ignore_errors = true) t
