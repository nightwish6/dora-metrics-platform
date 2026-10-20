{#--
Test: Commit date after Unix epoch
Description: Ensures commit timestamps are after Unix epoch (1970-01-01)
Usage: {{ config(tests=['commit_date_after_unix_epoch']) }}
--#}
{% test commit_date_after_unix_epoch(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }} < '1970-01-01 00:00:00'

{% endtest %}
