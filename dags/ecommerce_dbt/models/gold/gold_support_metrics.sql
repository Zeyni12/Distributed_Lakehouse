{{ config(materialized='table', tags=['gold']) }}

SELECT
    ticket_type,
    priority,
    COUNT(*) AS total_tickets,
    AVG(first_response_minutes) AS avg_response_minutes,
    AVG(resolution_hours) AS avg_resolution_hours,
    AVG(satisfaction_score) AS avg_satisfaction
FROM {{ ref('silver_support') }}
GROUP BY ticket_type, priority