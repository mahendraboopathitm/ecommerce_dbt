{% macro is_weekend(date_column) %}
    case when dayofweek({{ date_column }}) in (1, 7) then true else false end
{% endmacro %}