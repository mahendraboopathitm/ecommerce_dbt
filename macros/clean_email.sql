{% macro clean_email(column_name) %}
    lower(trim(regexp_replace({{ column_name }}, '\\s+', '')))
{% endmacro %}