-- Staging model for milestones
-- Extracts flattened data from raw.milestones JSON

{{ config(
    alias='milestones',
    unique_key='id',
    primary_key='id'
) }}
SELECT DISTINCT
    id,
    repo_name,
    data->>'$.url' as url,
    data->>'$.html_url' as html_url,
    data->>'$.labels_url' as labels_url,
    data->>'$.id' as milestone_id,
    data->>'$.node_id' as node_id,
    data->>'$.number' as number,
    data->>'$.state' as state,
    data->>'$.title' as title,
    data->>'$.description' as description,
    data->>'$.creator.login' as creator_login,
    data->>'$.creator.id' as creator_id,
    data->>'$.creator.url' as creator_url,
    data->>'$.creator.html_url' as creator_html_url,
    data->>'$.creator.avatar_url' as creator_avatar_url,
    data->>'$.open_issues' as open_issues,
    data->>'$.closed_issues' as closed_issues,
    data->>'$.created_at' as created_at,
    data->>'$.updated_at' as updated_at,
    data->>'$.closed_at' as closed_at,
    data->>'$.due_on' as due_on,
    current_timestamp AS loaded_at
FROM {{ ref('raw_milestones') }}
