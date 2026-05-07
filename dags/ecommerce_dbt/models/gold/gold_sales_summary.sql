{{ config(materialized='table', tags=['gold']) }}

SELECT
    customer_id,
    COUNT(transaction_id) AS total_transactions,
    SUM(amount) AS gross_sales,
    SUM(net_revenue) AS net_sales,
    AVG(amount) AS avg_order_value,

    SUM(
        CASE WHEN success_flag = 1 THEN 1 ELSE 0 END
    ) AS successful_payments

FROM {{ ref('silver_payments') }}
GROUP BY customer_id