{#--
Test: Commit date not too old
Description: Ensures commit dates are reasonable (not before Git creation in 2005)
Usage: {{ config(tests=['commit_date_not_too_old']) }}
--#}
{% test commit_date_not_too_old(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }} < '2005-04-03 00:00:00'

{#-- Git was created in April 2005. Any timestamp before that is suspicious --#}

{% endtest %}
