{{ config(materialized='table', tags=['silver']) }}

WITH cleaned AS (
    SELECT
        event_id,
        customer_id,
        session_id,
        LOWER(TRIM(event_type)) AS event_type,
        event_timestamp,
        page_url,
        product_id,
        category_id,
        LOWER(TRIM(referrer_source)) AS referrer_source,
        LOWER(TRIM(device_type)) AS device_type,
        user_agent,
        ip_address,
        ingested_at
    FROM {{ ref('bronze_customer_events') }}
),

sessionized AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY session_id
            ORDER BY event_timestamp
        ) AS event_sequence,

        COUNT(*) OVER (
            PARTITION BY session_id
        ) AS total_session_events
    FROM cleaned
)

SELECT * FROM sessionized