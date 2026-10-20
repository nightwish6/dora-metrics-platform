{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}

        {# If no custom schema is specified, use the one from profiles.yml (main) #}
        {{ target.schema }}

    {%- else -%}

        {# If a schema is specified explicitly (e.g. raw), use the trimmed name without the target.schema prefix #}
        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
