{#--
Test: Valid Git SHA
Description: Ensures a field matches valid Git SHA-1 format (40 lowercase hex characters)
Usage: {{ config(tests=['valid_git_sha']) }}
--#}
{% test valid_git_sha(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NULL
   OR LENGTH({{ column_name }}) != 40
   OR {{ column_name }} ~ '[^0-9a-f]'

{% endtest %}
