{% test non_negative(model, column_name) %}
    -- Generic dbt test: fails if the column has negative or NULL values.
    -- dbt marks the test as failed when this query returns one or more rows.

    SELECT
        {{ column_name }}
    FROM {{ model }}
    WHERE
        {{ column_name }} < 0
        OR {{ column_name }} IS NULL
{% endtest %}