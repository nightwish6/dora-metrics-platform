{#--
Test: Not in future
Description: Ensures a timestamp field is not in the future
Usage: {{ config(tests=['not_in_future']) }}
--#}
{% test not_in_future(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} > {{ current_timestamp() }}

{% endtest %}
