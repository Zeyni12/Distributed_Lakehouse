{{ config(materialized='table', tags=['gold']) }}

-- Gold: Customer 360
-- One row per customer summarising behaviour across all domains

WITH events AS (
    SELECT
        customer_id,
        COUNT(*)                                                                AS total_events,
        COUNT(DISTINCT session_id)                                              AS total_sessions,
        COUNT(DISTINCT event_date)                                              AS active_days,
        MIN(event_timestamp)                                                    AS first_seen_at,
        MAX(event_timestamp)                                                    AS last_seen_at,
        COUNT(DISTINCT CASE WHEN is_product_event THEN event_id END)            AS product_view_events,
        COUNT(DISTINCT CASE WHEN device_category = 'mobile' THEN session_id END)  AS mobile_sessions,
        COUNT(DISTINCT CASE WHEN device_category = 'desktop' THEN session_id END) AS desktop_sessions,
        -- Most used device (Trino-compatible approximation)
        CASE
            WHEN COUNT(CASE WHEN device_category = 'mobile' THEN 1 END) >=
                 COUNT(CASE WHEN device_category = 'desktop' THEN 1 END)
            THEN 'mobile'
            ELSE 'desktop'
        END                                                                     AS preferred_device
    FROM {{ ref('silver_customer_events') }}
    GROUP BY 1
),

payments AS (
    SELECT
        customer_id,
        COUNT(*)                                                                AS total_transactions,
        COUNT(CASE WHEN is_successful   THEN 1 END)                             AS successful_transactions,
        COUNT(CASE WHEN is_failed       THEN 1 END)                             AS failed_transactions,
        COUNT(CASE WHEN is_refunded     THEN 1 END)                             AS refunded_transactions,
        ROUND(SUM(CASE WHEN is_successful THEN amount ELSE 0 END), 2)           AS total_revenue,
        ROUND(AVG(CASE WHEN is_successful THEN amount END), 2)                  AS avg_order_value,
        MAX(CASE WHEN is_successful THEN transaction_date END)                  AS last_purchase_date,
        MIN(CASE WHEN is_successful THEN transaction_date END)                  AS first_purchase_date,
        COUNT(DISTINCT CASE WHEN is_high_risk THEN transaction_id END)          AS high_risk_transactions,
        -- Most used payment method (Trino-compatible)
        MAX_BY(payment_method, COUNT(payment_method)) OVER (
            PARTITION BY customer_id
        )                                                                       AS preferred_payment_method
    FROM {{ ref('silver_payment_transactions') }}
    GROUP BY 1, payment_method
),

payments_agg AS (
    SELECT
        customer_id,
        MAX(total_transactions)         AS total_transactions,
        MAX(successful_transactions)    AS successful_transactions,
        MAX(failed_transactions)        AS failed_transactions,
        MAX(refunded_transactions)      AS refunded_transactions,
        MAX(total_revenue)              AS total_revenue,
        MAX(avg_order_value)            AS avg_order_value,
        MAX(last_purchase_date)         AS last_purchase_date,
        MIN(first_purchase_date)        AS first_purchase_date,
        MAX(high_risk_transactions)     AS high_risk_transactions,
        MAX(preferred_payment_method)   AS preferred_payment_method
    FROM payments
    GROUP BY 1
),

tickets AS (
    SELECT
        customer_id,
        COUNT(*)                                                                AS total_tickets,
        COUNT(CASE WHEN is_resolved THEN 1 END)                                 AS resolved_tickets,
        COUNT(CASE WHEN is_open     THEN 1 END)                                 AS open_tickets,
        COUNT(CASE WHEN is_sla_breached THEN 1 END)                             AS sla_breached_tickets,
        ROUND(AVG(satisfaction_score), 2)                                       AS avg_satisfaction_score,
        ROUND(AVG(resolution_minutes), 2)                                       AS avg_resolution_minutes,
        MAX(created_timestamp)                                                  AS last_ticket_date,
        -- Most common ticket type (Trino-compatible)
        MAX_BY(ticket_type, cnt) AS most_common_ticket_type,
        MAX_BY(channel, cnt)     AS preferred_support_channel
    FROM (
        SELECT
            customer_id,
            ticket_type,
            channel,
            is_resolved,
            is_open,
            is_sla_breached,
            satisfaction_score,
            resolution_minutes,
            created_timestamp,
            COUNT(*) OVER (PARTITION BY customer_id, ticket_type) AS cnt
        FROM {{ ref('silver_support_tickets') }}
    )
    GROUP BY 1
)

SELECT
    COALESCE(p.customer_id, e.customer_id, t.customer_id)                       AS customer_id,

    -- Engagement
    COALESCE(e.total_events,        0)                                          AS total_events,
    COALESCE(e.total_sessions,      0)                                          AS total_sessions,
    COALESCE(e.active_days,         0)                                          AS active_days,
    e.first_seen_at,
    e.last_seen_at,
    e.preferred_device,
    COALESCE(e.product_view_events, 0)                                          AS product_view_events,

    -- Purchase behaviour
    COALESCE(p.total_transactions,      0)                                      AS total_transactions,
    COALESCE(p.successful_transactions, 0)                                      AS successful_transactions,
    COALESCE(p.failed_transactions,     0)                                      AS failed_transactions,
    COALESCE(p.refunded_transactions,   0)                                      AS refunded_transactions,
    COALESCE(p.total_revenue,           0)                                      AS total_revenue,
    p.avg_order_value,
    p.first_purchase_date,
    p.last_purchase_date,
    p.preferred_payment_method,
    COALESCE(p.high_risk_transactions,  0)                                      AS high_risk_transactions,

    -- Support behaviour
    COALESCE(t.total_tickets,           0)                                      AS total_tickets,
    COALESCE(t.resolved_tickets,        0)                                      AS resolved_tickets,
    COALESCE(t.open_tickets,            0)                                      AS open_tickets,
    COALESCE(t.sla_breached_tickets,    0)                                      AS sla_breached_tickets,
    t.avg_satisfaction_score,
    t.avg_resolution_minutes,
    t.last_ticket_date,
    t.most_common_ticket_type,
    t.preferred_support_channel,

    -- Customer health score (0-100)
    ROUND(
        LEAST(100, GREATEST(0,
            LEAST(40, COALESCE(p.total_revenue, 0) / 100.0)
            + LEAST(20, COALESCE(e.active_days, 0) * 1.0)
            + COALESCE(t.avg_satisfaction_score, 3) / 5.0 * 20
            + CASE WHEN COALESCE(p.high_risk_transactions, 0) = 0 THEN 10 ELSE 0 END
            + CASE WHEN COALESCE(t.open_tickets, 0) = 0 THEN 10 ELSE 0 END
        )), 2
    )                                                                           AS customer_health_score,

    CASE
        WHEN COALESCE(p.total_revenue, 0) >= 1000   THEN 'high_value'
        WHEN COALESCE(p.total_revenue, 0) >= 200    THEN 'mid_value'
        WHEN COALESCE(p.total_revenue, 0) > 0       THEN 'low_value'
        ELSE 'no_purchase'
    END                                                                         AS value_segment,

    CASE
        WHEN p.last_purchase_date < CURRENT_DATE - INTERVAL '90' DAY           THEN TRUE
        WHEN p.last_purchase_date IS NULL                                       THEN TRUE
        ELSE FALSE
    END                                                                         AS is_churn_risk,

    CURRENT_TIMESTAMP                                                           AS updated_at

FROM payments_agg   p
FULL OUTER JOIN events  e ON e.customer_id = p.customer_id
FULL OUTER JOIN tickets t ON t.customer_id = COALESCE(p.customer_id, e.customer_id)