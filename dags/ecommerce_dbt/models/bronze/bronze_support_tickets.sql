-- {{ config(materialized='table', tags=['bronze']) }}


-- SELECT
--     ticket_id, 
--     customer_id, 
--     NULLIF(order_id, '') as order_id, 
--     ticket_type, 
--     priority, 
--     status, 
--     CAST(created_timestamp AS TIMESTAMP) as  created_timestamp
--     CASE 
--        WHEN first_response_timestamp IS NULL OR CAST(first_response_timestamp as VARCHAR) = '' THEN CURRENT_TIMESTAMP
--        ELSE CAST(first_response_timestamp AS TIMESTAMP) END as first_response_timestamp
--     resolution_timestamp, 
--     agent_id, 
--     CAST(NULLIF(satisfaction_score, 0) AS INTEGER) as satisfaction_score,
--     subject,
--     channel,
--     CURRENT_TIMESTAMP as ingested_at,
--     'raw_support_tickets' as source_system

-- FROM {{ ref('raw_support_tickets') }}    

{{ config(
    materialized='table',
    tags=['bronze']
) }}

SELECT
    ticket_id,
    customer_id,
    NULLIF(order_id, '') AS order_id,
    LOWER(TRIM(ticket_type)) AS ticket_type,
    LOWER(TRIM(priority)) AS priority,
    LOWER(TRIM(status)) AS status,
    CAST(created_timestamp AS TIMESTAMP) AS created_timestamp,

    CASE
        WHEN first_response_timestamp IS NULL
             OR CAST(first_response_timestamp AS VARCHAR) = ''
        THEN NULL
        ELSE CAST(first_response_timestamp AS TIMESTAMP)
    END AS first_response_timestamp,

    CASE
        WHEN resolution_timestamp IS NULL
             OR CAST(resolution_timestamp AS VARCHAR) = ''
        THEN NULL
        ELSE CAST(resolution_timestamp AS TIMESTAMP)
    END AS resolution_timestamp,

    agent_id,
    CAST(NULLIF(CAST(satisfaction_score AS VARCHAR), '') AS INTEGER) AS satisfaction_score,
    subject,
    LOWER(TRIM(channel)) AS channel,
    CURRENT_TIMESTAMP AS ingested_at,
    'raw_support_tickets' AS source_system

FROM {{ ref('raw_support_tickets') }}

WHERE ticket_id IS NOT NULL
  AND customer_id IS NOT NULL