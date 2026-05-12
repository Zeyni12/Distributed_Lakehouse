{{ config(materialized='table', tags=['bronze']) }}

SELECT
    transaction_id, 
    order_id, 
    customer_id, 
    payment_method, 
    payment_status, 
    CAST(amount AS DECIMAL(10,2)) as amount, 
    currency, 
    CAST(transaction_timestamp as timestamp) as transaction_timestamp,
    CAST(processor_response_code AS VARCHAR(2)) as processor_response_code, 
    CAST(gateway_fee AS decimal(10,2)) as gateway_fee, 
    merchant_id, 
    billing_country, 
    risk_score,
    CURRENT_TIMESTAMP as ingested_at,
    'raw_payment_transactions' as source_system

FROM {{ ref('raw_payment_transactions') }}    

