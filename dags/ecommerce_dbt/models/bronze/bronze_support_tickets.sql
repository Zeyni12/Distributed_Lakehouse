{{ config(materialized='table', tags=['bronze']) }}

SELECT
    ticket_id, 
    customer_id, 
    NULLIF(order_id, '') as order_id, 
    ticket_type, 
    priority, 
    status, 
    CAST(created_timestamp AS TIMESTAMP) as created_timestamp,
    CASE 
        WHEN first_response_timestamp IS NULL 
          OR CAST(first_response_timestamp as VARCHAR) = '' 
        THEN CURRENT_TIMESTAMP
        ELSE CAST(first_response_timestamp AS TIMESTAMP) 
    END as first_response_timestamp,
    resolution_timestamp, 
    agent_id, 
    CAST(NULLIF(satisfaction_score, 0) AS INTEGER) as satisfaction_score,
    subject,
    channel,
    CURRENT_TIMESTAMP as ingested_at,
    'raw_support_tickets' as source_system
FROM {{ ref('raw_support_tickets') }}