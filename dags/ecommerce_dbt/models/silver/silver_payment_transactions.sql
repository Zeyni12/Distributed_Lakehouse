
{{ config(materialized='table', tags=['silver']) }}

WITH source AS (
    SELECT * FROM {{ ref('bronze_payment_transactions') }}
),

cleaned AS (
    SELECT
        transaction_id,
        order_id,
        customer_id,
        merchant_id,
        billing_country,

        -- Normalize categoricals
        LOWER(TRIM(payment_method))                             AS payment_method,
        LOWER(TRIM(payment_status))                             AS payment_status,
        UPPER(TRIM(currency))                                   AS currency,
        UPPER(TRIM(processor_response_code))                    AS processor_response_code,

        transaction_timestamp,
        DATE(transaction_timestamp)                             AS transaction_date,
        DATE_TRUNC('month', transaction_timestamp)              AS transaction_month,

        -- Financials
        GREATEST(amount, 0)                                     AS amount,
        GREATEST(gateway_fee, 0)                                AS gateway_fee,
        ROUND(GREATEST(amount, 0) - GREATEST(gateway_fee, 0), 2) AS net_amount,

        -- Risk
        CAST(risk_score AS INTEGER)                             AS risk_score,
        CASE
            WHEN CAST(risk_score AS INTEGER) >= 80             THEN 'high'
            WHEN CAST(risk_score AS INTEGER) >= 50             THEN 'medium'
            ELSE 'low'
        END                                                     AS risk_tier,

        -- Status flags
        CASE WHEN LOWER(TRIM(payment_status)) = 'completed'    THEN TRUE ELSE FALSE END AS is_successful,
        CASE WHEN LOWER(TRIM(payment_status)) = 'refunded'     THEN TRUE ELSE FALSE END AS is_refunded,
        CASE WHEN LOWER(TRIM(payment_status)) = 'failed'       THEN TRUE ELSE FALSE END AS is_failed,
        CASE WHEN CAST(risk_score AS INTEGER) >= 80            THEN TRUE ELSE FALSE END AS is_high_risk,

        ingested_at,
        source_system,
        CURRENT_TIMESTAMP                                       AS transformed_at

    FROM source
    WHERE
        transaction_id      IS NOT NULL
        AND amount          IS NOT NULL
        AND amount          >= 0
        AND transaction_timestamp IS NOT NULL
)

SELECT * FROM cleaned
