
  
    

    create table "iceberg"."bronze_raw_gold"."gold_inventory_health"
      
      
    as (
      

-- Gold: Inventory Health

WITH latest_snapshot AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id, warehouse_id
            ORDER BY snapshot_date DESC
        ) AS rn
    FROM "iceberg"."bronze_raw_silver"."silver_inventory_snapshots"
),

current_stock AS (
    SELECT * FROM latest_snapshot WHERE rn = 1
),

avg_30d AS (
    SELECT
        product_id,
        warehouse_id,
        ROUND(AVG(quantity_on_hand), 2)     AS avg_qty_30d,
        ROUND(AVG(inventory_value), 2)      AS avg_value_30d,
        MIN(snapshot_date)                  AS window_start,
        MAX(snapshot_date)                  AS window_end
    FROM "iceberg"."bronze_raw_silver"."silver_inventory_snapshots"
    WHERE snapshot_date >= CURRENT_DATE - INTERVAL '30' DAY
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

    a.avg_qty_30d,
    a.avg_value_30d,
    ROUND(c.quantity_on_hand - a.avg_qty_30d, 2)                           AS qty_vs_30d_avg,

    CASE
        WHEN c.quantity_on_hand > a.avg_qty_30d * 1.2                      THEN 'increasing'
        WHEN c.quantity_on_hand < a.avg_qty_30d * 0.8                      THEN 'decreasing'
        ELSE 'stable'
    END                                                                     AS stock_trend,

    CASE
        WHEN c.reorder_quantity > 0
        -- Fixed: use CAST instead of :: syntax
        THEN ROUND(CAST(c.quantity_available AS DOUBLE) / NULLIF(c.reorder_quantity, 0) * 30, 1)
        ELSE NULL
    END                                                                     AS estimated_days_of_supply,

    CASE
        WHEN c.quantity_on_hand > c.reorder_point * 3                      THEN TRUE
        ELSE FALSE
    END                                                                     AS is_overstocked,

    CURRENT_TIMESTAMP                                                       AS updated_at

FROM current_stock  c
LEFT JOIN avg_30d   a
    ON  a.product_id   = c.product_id
    AND a.warehouse_id = c.warehouse_id
    );

  