{{ config(materialized='table', tags=['gold']) }}

SELECT
    customer_id,
    COUNT(*) AS total_events,
    COUNT(DISTINCT session_id) AS total_sessions,
    COUNT(DISTINCT product_id) AS unique_products_viewed,
    MAX(event_timestamp) AS last_activity
FROM {{ ref('silver_customer_events') }}
GROUP BY customer_id