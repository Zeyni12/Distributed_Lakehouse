
  
    

    create table "iceberg"."bronze_raw_gold"."gold_support_team_performance"
      
      
    as (
      

-- Gold: Support Team Performance

WITH agent_daily AS (
    SELECT
        agent_id,
        created_date,
        channel,

        COUNT(*)                                                                AS tickets_handled,
        COUNT(CASE WHEN is_resolved THEN 1 END)                                 AS tickets_resolved,
        COUNT(CASE WHEN is_open     THEN 1 END)                                 AS tickets_open,
        COUNT(CASE WHEN is_sla_breached THEN 1 END)                             AS sla_breaches,

        ROUND(AVG(first_response_minutes), 2)                                   AS avg_first_response_minutes,
        ROUND(AVG(resolution_minutes), 2)                                       AS avg_resolution_minutes,
        ROUND(MIN(resolution_minutes), 2)                                       AS min_resolution_minutes,
        ROUND(MAX(resolution_minutes), 2)                                       AS max_resolution_minutes,

        ROUND(AVG(satisfaction_score), 2)                                       AS avg_satisfaction_score,
        COUNT(CASE WHEN satisfaction_category = 'satisfied'    THEN 1 END)      AS satisfied_count,
        COUNT(CASE WHEN satisfaction_category = 'neutral'      THEN 1 END)      AS neutral_count,
        COUNT(CASE WHEN satisfaction_category = 'dissatisfied' THEN 1 END)      AS dissatisfied_count,

        COUNT(DISTINCT customer_id)                                             AS unique_customers_served,
        COUNT(CASE WHEN is_order_related THEN 1 END)                            AS order_related_tickets

    FROM "iceberg"."bronze_raw_silver"."silver_support_tickets"
    WHERE agent_id IS NOT NULL
    GROUP BY 1, 2, 3
)

SELECT
    agent_id,
    created_date,
    channel,
    tickets_handled,
    tickets_resolved,
    tickets_open,
    sla_breaches,

    ROUND(tickets_resolved * 100.0 / NULLIF(tickets_handled, 0), 2)            AS resolution_rate_pct,
    -- Fixed: compute sla_breach_rate_pct inline instead of referencing alias
    ROUND(sla_breaches * 100.0 / NULLIF(tickets_handled, 0), 2)                AS sla_breach_rate_pct,

    avg_first_response_minutes,
    avg_resolution_minutes,
    min_resolution_minutes,
    max_resolution_minutes,
    avg_satisfaction_score,
    satisfied_count,
    neutral_count,
    dissatisfied_count,
    ROUND(satisfied_count * 100.0 / NULLIF(tickets_handled, 0), 2)             AS csat_pct,
    unique_customers_served,
    order_related_tickets,

    -- Fixed: reference sla_breaches directly instead of alias sla_breach_rate_pct
    CASE
        WHEN avg_satisfaction_score >= 4.5
             AND sla_breaches = 0                                               THEN 'top_performer'
        WHEN avg_satisfaction_score >= 3.5
             AND ROUND(sla_breaches * 100.0 / NULLIF(tickets_handled, 0), 2) <= 10  THEN 'solid_performer'
        WHEN avg_satisfaction_score < 3
             OR  ROUND(sla_breaches * 100.0 / NULLIF(tickets_handled, 0), 2) > 25   THEN 'needs_support'
        ELSE 'average'
    END                                                                         AS performance_tier,

    CURRENT_TIMESTAMP                                                           AS updated_at

FROM agent_daily
    );

  