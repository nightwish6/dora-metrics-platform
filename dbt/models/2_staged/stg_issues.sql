-- @description Staging model for issues. Flattens the raw JSON data into structured columns including state, labels, assignees, milestones, comments, and timestamps. Supports incremental refresh based on `updated_at`.
{{ config(
    alias='issues',
    unique_key='id',
    primary_key='id'
) }}
SELECT DISTINCT
    id,
    repo_name,
    data->>'$.number' as issue_number,
    data->>'$.state' as state,
    data->>'$.title' as title,
    data->>'$.body' as body,
    data->>'$.type' as type,
    data->>'$.labels' as labels,
    data->>'$.assignee.login' as assignee,
    data->>'$.assignees' as assignees,
    data->>'$.milestone.title' as milestone_title,
    try_cast(data->>'$.comments' as int) as comments,
    try_cast(data->>'$.created_at' as timestamp) as created_at,
    try_cast(data->>'$.updated_at' as timestamp) as updated_at,
    try_cast(data->>'$.closed_at' as timestamp) as closed_at,
    try_cast(data->>'$.closed_at_epoch' as bigint) as closed_at_epoch,
    try_cast(data->>'$.created_at_epoch' as bigint) as created_at_epoch,
    data->>'$.url' as url,
    data->>'$.html_url' as html_url,
    data->>'$.user.login' as user_login,
    try_cast(data->>'$.user.id' as bigint) as user_id,
    data->>'$.user.avatar_url' as user_avatar_url,
    data->>'$.user.html_url' as user_html_url,
    data->>'$.user.type' as user_type,
    data->>'$.comments_url' as comments_url,
    data->>'$.repository_url' as repository_url,
    data->>'$.repository.name' as repository_name,
    data->>'$.repository.owner.login' as repository_owner,
    try_cast(data->>'$.locked' as boolean) as locked,
    try_cast(data->>'$.draft' as boolean) as draft,
    data->>'$.active_lock_reason' as active_lock_reason,
    data->>'$.timeline_url' as timeline_url,
    data->>'$.performed_via_github_app' as performed_via_github_app,
    data->>'$.reactions' as reactions,
    data->>'$.author_association' as author_association,
    data->>'$.loaded_at' as loaded_at
FROM {{ ref('raw_issues') }}
{% if is_incremental() %}
WHERE try_cast(data->>'$.updated_at' as timestamp) > (
    SELECT COALESCE(MAX(try_cast(updated_at as timestamp)), '1900-01-01') FROM {{ this }}
)
{% endif %}
