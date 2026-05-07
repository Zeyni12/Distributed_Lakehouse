{{ config(materialized='table', tags=['silver']) }}

SELECT
    ticket_id,
    customer_id,
    order_id,
    LOWER(ticket_type) AS ticket_type,
    LOWER(priority) AS priority,
    LOWER(status) AS status,
    created_timestamp,
    first_response_timestamp,
    resolution_timestamp,
    agent_id,
    satisfaction_score,
    subject,
    channel,

    date_diff(
        'minute',
        created_timestamp,
        first_response_timestamp
    ) AS first_response_minutes,

    date_diff(
        'hour',
        created_timestamp,
        resolution_timestamp
    ) AS resolution_hours

FROM {{ ref('bronze_support_tickets') }}