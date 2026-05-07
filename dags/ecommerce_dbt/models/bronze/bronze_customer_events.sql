{{ config(materialized='table', tags=['bronze']) }}


SELECT
    event_id,
    customer_id,
    session_id,
    event_type,
    CAST(event_timestamp AS TIMESTAMP) as event_timestamp,
    page_url,
    NULLIF(product_id, '') as product_id,
    NULLIF(category_id, '') as category_id,
    referrer_source,
    device_type,
    user_agent,
    ip_address,
    CURRENT_TIMESTAMP as ingested_at,
    'raw_customer_events' as source_system
FROM {{ ref('raw_customer_events') }}  
--WHERE event_id IS NOT NULL
--AND customer_id IS NOT NULL
--AND event_timestamp IS NOT NULL  
