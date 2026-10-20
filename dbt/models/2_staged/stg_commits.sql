-- Staging model for commits
-- Extracts flattened data from raw.commits JSON

{{ config(
    alias='commits',
    unique_key='id'
) }}
SELECT DISTINCT
    id,
    repo_name,
    data->>'sha' as sha,
    data->>'url' as url,
    data->>'html_url' as html_url,
    data->>'node_id' as node_id,
    data->>'comments_url' as comments_url,
    data->>'$.commit.message' as commit_message,
    data->>'$.commit.tree.sha' as commit_tree_sha,
    data->>'$.commit.tree.url' as commit_tree_url,
    data->>'$.commit.url' as commit_url,
    try_cast(data->>'$.commit.comment_count' as int) as commit_comment_count,
    data->>'$.commit.author.name' as commit_author_name,
    data->>'$.commit.author.email' as commit_author_email,
    try_cast(data->>'$.commit.author.date' as timestamp) as commit_author_date,
    data->>'$.commit.committer.name' as commit_committer_name,
    data->>'$.commit.committer.email' as commit_committer_email,
    try_cast(data->>'$.commit.committer.date' as timestamp) as commit_committer_date,
    try_cast(data->>'$.commit.verification.verified' as boolean) as commit_verification_verified,
    data->>'$.commit.verification.reason' as commit_verification_reason,
    data->>'$.commit.verification.signature' as commit_verification_signature,
    data->>'$.commit.verification.payload' as commit_verification_payload,
    try_cast(data->>'$.commit.verification.verified_at' as timestamp) as commit_verification_verified_at,
    data->>'author.login' as author_login,
    try_cast(data->>'author.id' as bigint) as author_id,
    data->>'author.node_id' as author_node_id,
    data->>'author.avatar_url' as author_avatar_url,
    data->>'author.html_url' as author_html_url,
    data->>'author.type' as author_type,
    try_cast(data->>'author.site_admin' as boolean) as author_site_admin,
    data->>'committer.login' as committer_login,
    try_cast(data->>'committer.id' as bigint) as committer_id,
    data->>'committer.node_id' as committer_node_id,
    data->>'committer.avatar_url' as committer_avatar_url,
    data->>'committer.html_url' as committer_html_url,
    data->>'committer.type' as committer_type,
    try_cast(data->>'committer.site_admin' as boolean) as committer_site_admin,
    data->>'parents' as parents,
    now() AS loaded_at
FROM {{ ref('raw_commits') }}
