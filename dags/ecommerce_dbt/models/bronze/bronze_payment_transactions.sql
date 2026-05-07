-- {{ config(materialized='table', tags=['bronze']) }}

-- SELECT
--     transaction_id, 
--     order_id, 
--     customer_id, 
--     payment_method, 
--     payment_status, 
--     CAST(amount AS DECIMAL(10,2)) as amount, 
--     currency, 
--     CAST(transaction_timestamp as timestamp) as transaction_timestamp
--     CAST(processor_responce_code AS VARCHAR(2)) as processor_responce_code, 
--     CAST(gateway_fee AS decimal(10,2)) as gateway_fee, 
--     merchant_id, 
--     billing_country, 
--     risk_score,
--     CURRENT_TIMESTAMP as ingested_at,
--     'raw_payment_transactions' as source_system

-- FROM {{ ref('raw_payment_transactions') }}    

{{ config(
    materialized='table',
    tags=['bronze']
) }}

SELECT
    transaction_id,
    order_id,
    customer_id,
    LOWER(TRIM(payment_method)) AS payment_method,
    LOWER(TRIM(payment_status)) AS payment_status,
    CAST(amount AS DECIMAL(10,2)) AS amount,
    currency,
    CAST(transaction_timestamp AS TIMESTAMP) AS transaction_timestamp,
    CAST(processor_response_code AS VARCHAR) AS processor_response_code,
    CAST(gateway_fee AS DECIMAL(10,2)) AS gateway_fee,
    merchant_id,
    billing_country,
    CAST(risk_score AS DOUBLE) AS risk_score,
    CURRENT_TIMESTAMP AS ingested_at,
    'raw_payment_transactions' AS source_system

FROM {{ source('raw', 'raw_payment_transactions') }}

WHERE transaction_id IS NOT NULL
  AND amount IS NOT NULL