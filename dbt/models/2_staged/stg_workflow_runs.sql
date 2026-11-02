-- @description Staging model for workflow runs. Flattens the raw JSON data into structured columns including CI/CD status, conclusion, head SHA, event type, and trigger info. Supports incremental refresh based on `updated_at`.
{{ config(
    alias='workflow_runs',
    primary_key='id',
    unique_key='id'
) }}
SELECT DISTINCT
    id,
    repo_name,
    data->>'$.url' as url,
    data->>'$.database_id' as database_id,
    data->>'$.name' as name,
    data->>'$.node_id' as node_id,
    data->>'$.head_sha' as head_sha,
    data->>'$.head_branch' as head_branch,
    data->>'$.event' as event,
    data->>'$.status' as status,
    data->>'$.conclusion' as conclusion,
    data->>'$.workflow_id' as workflow_id,
    data->>'$.check_suite_id' as check_suite_id,
    data->>'$.check_suite_node_id' as check_suite_node_id,
    data->>'$.pull_requests' as pull_requests,
    data->>'$.created_at' as created_at,
    try_cast(data->>'$.updated_at' as timestamp) as updated_at,
    data->>'$.run_attempt' as run_attempt,
    data->>'$.run_number' as run_number,
    data->>'$.attempt' as attempt,
    data->>'$.run_started_at' as run_started_at,
    data->>'$.triggering_actor.login' as triggering_actor_login,
    try_cast(data->>'$.triggering_actor.id' as bigint) as triggering_actor_id,
    data->>'$.triggering_actor.url' as triggering_actor_url,
    data->>'$.triggering_actor.html_url' as triggering_actor_html_url,
    data->>'$.jobs_url' as jobs_url,
    data->>'$.logsUrl' as logs_url,
    data->>'$.check_suite_url' as check_suite_url,
    data->>'$.artifacts_url' as artifacts_url,
    data->>'$.cancel_url' as cancel_url,
    data->>'$.rerun_url' as rerun_url,
    data->>'$.previous_attempt_url' as previous_attempt_url,
    data->>'$.workflow_url' as workflow_url,
    data->>'$.head_commit.message' as head_commit_message,
    data->>'$.head_commit.id' as head_commit_id,
    data->>'$.head_commit.tree_id' as head_commit_tree_id,
    data->>'$.head_commit.timestamp' as head_commit_timestamp,
    data->>'$.head_commit.author.name' as head_commit_author_name,
    data->>'$.head_commit.author.email' as head_commit_author_email,
    data->>'$.head_commit.committer.name' as head_commit_committer_name,
    data->>'$.head_commit.committer.email' as head_commit_committer_email,
    current_timestamp AS loaded_at
FROM {{ ref('raw_workflow_runs') }}
{% if is_incremental() %}
WHERE try_cast(data->>'$.updated_at' as timestamp) > (
    SELECT COALESCE(MAX(try_cast(updated_at as timestamp)), '1900-01-01') FROM {{ this }}
)
{% endif %}
