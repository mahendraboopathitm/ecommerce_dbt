{% macro date_spine(start_date, end_date) %}
    select explode(sequence(
        to_date('{{ start_date }}'),
        to_date('{{ end_date }}'),
        interval 1 day
    )) as date_day
{% endmacro %}