-- @description Raw releases data loaded from JSON files. Contains GitHub release information including tag names, asset URLs, and author details. Each row represents a unique release identified by `id`.
-- @parameters:
--   json_path: Path to JSON file with release data
--   repo_name: Repository name (defaults to 'vscode')
{{ config(
    alias='releases',
    unique_key='id'
) }}
SELECT DISTINCT
    md5(CAST(t.id AS VARCHAR)) AS id,
    CAST(t AS JSON) AS data,
    'releases' AS source_name,
    '{{ var("repo_name", "vscode") }}' AS repo_name,
    current_timestamp AS loaded_at
FROM read_json_auto('{{ var("json_path", "data/raw/vscode_releases.json") }}', ignore_errors = true) t
