{#--
Test: Valid GitHub login format
Description: Ensures author_login follows GitHub login conventions
Usage: {{ config(tests=['valid_github_login']) }}
--#}
{% test valid_github_login(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NULL
   OR {{ column_name }} = ''
   OR trim({{ column_name }}) = ''
   OR LENGTH({{ column_name }}) > 39
   OR {{ column_name }} !~ '^[a-z0-9]([a-z0-9-]*[a-z0-9])?$'
   OR {{ column_name }} ~ '-$'
   OR {{ column_name }} ~ '^-{2}'

{#-- GitHub login rules: lowercase, alphanumeric + hyphens, max 39 chars
     no leading/trailing hyphens, no consecutive hyphens at start --#}

{% endtest %}
