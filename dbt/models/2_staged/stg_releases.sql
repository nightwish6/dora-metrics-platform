-- @description Staging model for releases. Flattens the raw JSON data into structured columns including tag name, release name, asset info, and author details. Supports full refresh only (no incremental strategy).
{{ config(
    alias='releases',
    primary_key='id',
    unique_key='id'
) }}
SELECT DISTINCT
    id,
    repo_name,
    data->>'$.url' as url,
    data->>'$.html_url' as html_url,
    data->>'$.assets_url' as assets_url,
    data->>'$.upload_url' as upload_url,
    data->>'$.tarball_url' as tarball_url,
    data->>'$.zipball_url' as zipball_url,
    data->>'$.discussion_url' as discussion_url,
    data->>'$.id' as release_id,
    data->>'$.node_id' as node_id,
    data->>'$.tag_name' as tag_name,
    data->>'$.target_commitish' as target_commitish,
    data->>'$.name' as name,
    data->>'$.body' as body,
    try_cast(data->>'$.draft' as boolean) as draft,
    try_cast(data->>'$.prerelease' as boolean) as prerelease,
    data->>'$.created_at' as created_at,
    data->>'$.published_at' as published_at,
    data->>'$.author.login' as author_login,
    data->>'$.author.id' as author_id,
    data->>'$.author.url' as author_url,
    data->>'$.author.html_url' as author_html_url,
    data->>'$.author.avatar_url' as author_avatar_url,
    data->>'$.author.type' as author_type,
    data->>'$.assets' as assets,
    data->>'$.assets_count' as assets_count,
    current_timestamp AS loaded_at
FROM {{ ref('raw_releases') }}
