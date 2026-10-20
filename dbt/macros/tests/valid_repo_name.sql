{#--
Test: Valid repo name
Description: Ensures repo_name follows expected format (owner/repo or just repo)
Usage: {{ config(tests=['valid_repo_name']) }}
--#}
{% test valid_repo_name(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NULL
   OR {{ column_name }} = ''
   OR trim({{ column_name }}) = ''
   OR {{ column_name }} !~ '^[a-zA-Z0-9._-]+(/[a-zA-Z0-9._-]+)?$'

{#-- Accepts both "repo" and "owner/repo" formats --#}

{% endtest %}
