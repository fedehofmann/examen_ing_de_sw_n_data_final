{% set clean_dir = var('clean_dir') %}
{% set ds_nodash = var('ds_nodash') %}

WITH source AS (
    SELECT 
        *
    FROM read_parquet(
        '{{ clean_dir }}/transactions_{{ ds_nodash }}_clean.parquet'
    )
)
-- Final select: cast columns to the proper types for the staging table
SELECT
    CAST(transaction_id AS bigint) AS transaction_id,
    CAST(customer_id AS bigint) AS customer_id,
    CAST(amount AS double) AS amount,
    CAST(status AS varchar) AS status,
    CAST(transaction_ts AS timestamp) AS transaction_ts,
    CAST(transaction_date AS date) AS transaction_date
FROM source