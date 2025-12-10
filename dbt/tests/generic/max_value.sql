-- tests/generic/max_value.sql
{% test max_value(model, column_name, max_allowed) %}
    -- Fails if the column has values greater than max_allowed.
    select
        {{ column_name }}
    from {{ model }}
    where {{ column_name }} > {{ max_allowed }}
{% endtest %}