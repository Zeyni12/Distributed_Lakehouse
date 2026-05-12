{{ config(materialized='table', tags=['silver']) }}

WITH source AS (
    SELECT * FROM {{ ref('bronze_support_tickets') }}
),

cleaned AS (
    SELECT
        ticket_id,
        customer_id,
        order_id,
        agent_id,
        LOWER(TRIM(ticket_type))                                AS ticket_type,
        LOWER(TRIM(priority))                                   AS priority,
        LOWER(TRIM(status))                                     AS status,
        LOWER(TRIM(channel))                                    AS channel,
        subject,
        created_timestamp,
        DATE(created_timestamp)                                 AS created_date,
        CASE
            WHEN first_response_timestamp > created_timestamp   THEN first_response_timestamp
            ELSE NULL
        END                                                     AS first_response_timestamp,
        CAST(resolution_timestamp AS TIMESTAMP)                 AS resolution_timestamp,

        -- SLA & duration metrics (in minutes)
        CASE
            WHEN first_response_timestamp > created_timestamp
            THEN DATE_DIFF('second', created_timestamp, first_response_timestamp) / 60.0
            ELSE NULL
        END                                                     AS first_response_minutes,

        CASE
            WHEN CAST(resolution_timestamp AS TIMESTAMP) IS NOT NULL
            THEN DATE_DIFF('second', created_timestamp, CAST(resolution_timestamp AS TIMESTAMP)) / 60.0
            ELSE NULL
        END                                                     AS resolution_minutes,

        -- SLA breach flags
        CASE
            WHEN LOWER(TRIM(priority)) = 'high'
                AND DATE_DIFF('second', created_timestamp, first_response_timestamp) / 60.0 > 60
                THEN TRUE
            WHEN LOWER(TRIM(priority)) = 'medium'
                AND DATE_DIFF('second', created_timestamp, first_response_timestamp) / 60.0 > 240
                THEN TRUE
            WHEN LOWER(TRIM(priority)) = 'low'
                AND DATE_DIFF('second', created_timestamp, first_response_timestamp) / 60.0 > 1440
                THEN TRUE
            ELSE FALSE
        END                                                     AS is_sla_breached,

        satisfaction_score,
        CASE
            WHEN satisfaction_score >= 4                        THEN 'satisfied'
            WHEN satisfaction_score = 3                         THEN 'neutral'
            WHEN satisfaction_score <= 2                        THEN 'dissatisfied'
            ELSE NULL
        END                                                     AS satisfaction_category,

        CASE WHEN LOWER(TRIM(status)) = 'resolved'             THEN TRUE ELSE FALSE END AS is_resolved,
        CASE WHEN LOWER(TRIM(status)) = 'open'                 THEN TRUE ELSE FALSE END AS is_open,
        CASE WHEN order_id IS NOT NULL                         THEN TRUE ELSE FALSE END AS is_order_related,

        ingested_at,
        source_system,
        CURRENT_TIMESTAMP                                       AS transformed_at

    FROM source
    WHERE
        ticket_id       IS NOT NULL
        AND customer_id IS NOT NULL
        AND created_timestamp IS NOT NULL
)

SELECT * FROM cleaned