-- Raw releases model
-- Parameters:
--   json_path: Path to JSON file with release data
{{ config(
    alias='releases',
    unique_key='id'
) }}
SELECT
    md5(CAST(t.id AS VARCHAR)) AS id,
    CAST(t AS JSON) AS data,
    'releases' AS source_name,
    '{{ var("repo_name", "vscode") }}' AS repo_name,
    now() AS loaded_at
FROM read_json_auto('{{ var("json_path", "data/raw/vscode_releases.json") }}') t
