{% test non_zero(model, column_name) %}
    -- Generic dbt test: fails if the column is 0 or NULL.
    -- dbt considers the test "failed" when this query returns one or more rows.

    SELECT
        {{ column_name }}
    FROM {{ model }}
    WHERE
        {{ column_name }} = 0
        OR {{ column_name }} IS NULL
{% endtest %}