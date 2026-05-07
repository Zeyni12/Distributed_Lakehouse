{{ config(materialized='table', tags=['silver']) }}

SELECT
    transaction_id,
    order_id,
    customer_id,
    LOWER(payment_method) AS payment_method,
    LOWER(payment_status) AS payment_status,
    amount,
    currency,
    transaction_timestamp,
    processor_response_code,
    gateway_fee,
    merchant_id,
    billing_country,
    risk_score,

    CASE
        WHEN payment_status = 'success' THEN 1
        ELSE 0
    END AS success_flag,

    CASE
        WHEN risk_score >= 0.80 THEN 'high'
        WHEN risk_score >= 0.50 THEN 'medium'
        ELSE 'low'
    END AS risk_bucket,

    amount - gateway_fee AS net_revenue

FROM {{ ref('bronze_payment_transactions') }}