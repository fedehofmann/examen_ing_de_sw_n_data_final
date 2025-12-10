WITH base AS (
    -- Silver layer: use the cleaned staging transactions table
    SELECT *
    FROM {{ ref('stg_transactions') }}
)

SELECT
    customer_id,

    -- Total number of transactions per customer
    COUNT(*) AS transaction_count,
    
    -- Sum of amounts for COMPLETED transactions only
    SUM(
        CASE
            WHEN status = 'completed' THEN amount
            ELSE 0
        END
    ) AS total_amount_completed,

    -- We found an alternative way of calculating the sum of amounts with status = 'completed' in dbt
    -- COALESCE(SUM(amount) FILTER (WHERE status = 'completed'), 0) AS total_amount_completed,

    -- Sum of amounts for ALL transactions (any status)
    SUM(amount) AS total_amount_all

FROM base
GROUP BY customer_id