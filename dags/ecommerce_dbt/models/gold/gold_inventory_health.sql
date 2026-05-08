

{{ config(materialized='table', tags=['gold']) }}

-- Gold: Inventory Health
-- Latest snapshot per product × warehouse with health indicators

WITH latest_snapshot AS (
    -- Take the most recent snapshot per product × warehouse
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id, warehouse_id
            ORDER BY snapshot_date DESC
        ) AS rn
    FROM {{ ref('silver_inventory_snapshots') }}
),

current_stock AS (
    SELECT * FROM latest_snapshot WHERE rn = 1
),

-- 30-day average to detect trend
avg_30d AS (
    SELECT
        product_id,
        warehouse_id,
        ROUND(AVG(quantity_on_hand), 2)     AS avg_qty_30d,
        ROUND(AVG(inventory_value), 2)      AS avg_value_30d,
        MIN(snapshot_date)                  AS window_start,
        MAX(snapshot_date)                  AS window_end
    FROM {{ ref('silver_inventory_snapshots') }}
    WHERE snapshot_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY 1, 2
)

SELECT
    c.snapshot_id,
    c.product_id,
    c.warehouse_id,
    c.supplier_id,
    c.snapshot_date                                                         AS latest_snapshot_date,
    c.last_received_date,

    c.quantity_on_hand,
    c.quantity_reserved,
    c.quantity_available,
    c.net_available_quantity,
    c.reorder_point,
    c.reorder_quantity,
    c.unit_cost,
    c.inventory_value,

    c.stock_status,
    c.needs_reorder,

    -- 30-day trend
    a.avg_qty_30d,
    a.avg_value_30d,
    ROUND(c.quantity_on_hand - a.avg_qty_30d, 2)                           AS qty_vs_30d_avg,

    CASE
        WHEN c.quantity_on_hand > a.avg_qty_30d * 1.2                      THEN 'increasing'
        WHEN c.quantity_on_hand < a.avg_qty_30d * 0.8                      THEN 'decreasing'
        ELSE 'stable'
    END                                                                     AS stock_trend,

    -- Days of supply estimate (requires avg daily sales — placeholder using reorder logic)
    CASE
        WHEN c.reorder_quantity > 0
        THEN ROUND(c.quantity_available::FLOAT / NULLIF(c.reorder_quantity, 0) * 30, 1)
        ELSE NULL
    END                                                                     AS estimated_days_of_supply,

    -- Overstock flag: more than 3× reorder point
    CASE
        WHEN c.quantity_on_hand > c.reorder_point * 3                      THEN TRUE
        ELSE FALSE
    END                                                                     AS is_overstocked,

    CURRENT_TIMESTAMP                                                       AS updated_at

FROM current_stock      c
LEFT JOIN avg_30d       a
    ON  a.product_id    = c.product_id
    AND a.warehouse_id  = c.warehouse_id