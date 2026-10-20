{#--
Test: Valid email format
Description: Ensures a field matches standard email format
Usage: {{ config(tests=['valid_email']) }}
--#}
{% test valid_email(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }} != ''
  AND trim({{ column_name }}) != ''
  AND {{ column_name }} !~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

{#-- Basic email format: user@domain.tld --#}

{% endtest %}
