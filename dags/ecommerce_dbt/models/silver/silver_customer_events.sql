

{{ config(materialized='table', tags=['silver']) }}

WITH source AS (
    SELECT * FROM {{ ref('bronze_customer_events') }}
),

cleaned AS (
    SELECT
        event_id,
        customer_id,
        session_id,
        event_type,
        event_timestamp,
        page_url,
        product_id,
        category_id,

        -- Normalize referrer source
        LOWER(TRIM(referrer_source))                            AS referrer_source,

        -- Normalize device type
        LOWER(TRIM(device_type))                                AS device_type,

        -- Mask PII
        MD5(ip_address)                                         AS ip_address_hashed,

        -- Derived fields
        DATE(event_timestamp)                                   AS event_date,
        DATE_TRUNC('hour', event_timestamp)                     AS event_hour,
        EXTRACT(DOW FROM event_timestamp)                       AS day_of_week,
        EXTRACT(HOUR FROM event_timestamp)                      AS hour_of_day,

        CASE
            WHEN LOWER(device_type) IN ('mobile', 'tablet')    THEN 'mobile'
            WHEN LOWER(device_type) = 'desktop'                THEN 'desktop'
            ELSE 'unknown'
        END                                                     AS device_category,

        CASE
            WHEN product_id IS NOT NULL                        THEN TRUE
            ELSE FALSE
        END                                                     AS is_product_event,

        ingested_at,
        source_system,
        CURRENT_TIMESTAMP                                       AS transformed_at

    FROM source
    WHERE
        event_id        IS NOT NULL
        AND customer_id IS NOT NULL
        AND event_timestamp IS NOT NULL
        -- Remove future-dated events (data quality guard)
        AND event_timestamp <= CURRENT_TIMESTAMP
)

SELECT * FROM cleaned