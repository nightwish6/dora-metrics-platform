{#--
Test: Not empty string
Description: Ensures a string field is not null and not just whitespace
Usage: {{ config(tests=['not_empty_string']) }}
--#}
{% test not_empty_string(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NULL
   OR trim({{ column_name }}) = ''

{% endtest %}
