{#--
Test: Commit message minimum length
Description: Ensures commit messages have meaningful content (minimum length)
Usage: {{ config(tests=['commit_message_min_length']) }}
--#}
{% test commit_message_min_length(model, column_name, min_length=1) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NULL
   OR LENGTH({{ column_name }}) < {{ min_length }}

{% endtest %}
