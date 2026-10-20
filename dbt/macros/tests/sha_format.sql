{#--
Test: SHA format
Description: Ensures a field matches Git SHA-1 format (40 hex characters)
Usage: {{ config(tests=['sha_format']) }}
--#}
{% test sha_format(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NULL
   OR LENGTH({{ column_name }}) != 40
   OR {{ column_name }} ~ '[^0-9a-fA-F]'

{#-- Git SHA-1: exactly 40 hexadecimal characters (case-insensitive) --#}

{% endtest %}
