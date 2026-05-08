

{{ config(materialized='table', tags=['gold']) }}

-- Gold: Daily Revenue Summary
-- One row per day × currency for finance/BI dashboards

WITH daily AS (
    SELECT
        transaction_date,
        currency,
        payment_method,
        billing_country,

        COUNT(*)                                                            AS total_transactions,
        COUNT(CASE WHEN is_successful THEN 1 END)                           AS successful_transactions,
        COUNT(CASE WHEN is_failed     THEN 1 END)                           AS failed_transactions,
        COUNT(CASE WHEN is_refunded   THEN 1 END)                           AS refunded_transactions,
        COUNT(CASE WHEN is_high_risk  THEN 1 END)                           AS high_risk_transactions,

        ROUND(SUM(CASE WHEN is_successful THEN amount       ELSE 0 END), 2) AS gross_revenue,
        ROUND(SUM(CASE WHEN is_refunded   THEN amount       ELSE 0 END), 2) AS refunded_amount,
        ROUND(SUM(CASE WHEN is_successful THEN gateway_fee  ELSE 0 END), 2) AS total_gateway_fees,
        ROUND(SUM(CASE WHEN is_successful THEN net_amount   ELSE 0 END), 2) AS net_revenue,

        COUNT(DISTINCT customer_id)                                         AS unique_customers,
        ROUND(AVG(CASE WHEN is_successful THEN amount END), 2)              AS avg_transaction_value,

        ROUND(
            COUNT(CASE WHEN is_successful THEN 1 END) * 100.0
            / NULLIF(COUNT(*), 0), 2
        )                                                                   AS success_rate_pct

    FROM {{ ref('silver_payment_transactions') }}
    GROUP BY 1, 2, 3, 4
)

SELECT
    *,
    ROUND(refunded_amount * 100.0 / NULLIF(gross_revenue, 0), 2)            AS refund_rate_pct,
    ROUND(high_risk_transactions * 100.0 / NULLIF(total_transactions, 0), 2) AS high_risk_rate_pct,
    CURRENT_TIMESTAMP                                                       AS updated_at
FROM daily