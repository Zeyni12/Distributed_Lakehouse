

WITH source AS (
    SELECT * FROM "iceberg"."bronze_raw_bronze"."bronze_inventory_snapshots"
),

cleaned AS (
    SELECT
        snapshot_id,
        product_id,
        warehouse_id,
        snapshot_date,
        supplier_id,

        -- Clamp negative quantities to 0
        GREATEST(quantity_on_hand,   0)                                             AS quantity_on_hand,
        GREATEST(quantity_reserved,  0)                                             AS quantity_reserved,
        GREATEST(quantity_available, 0)                                             AS quantity_available,
        GREATEST(reorder_point,      0)                                             AS reorder_point,
        GREATEST(reorder_quantity,   0)                                             AS reorder_quantity,

        last_received_date,
        unit_cost,

        -- Derived fields
        GREATEST(quantity_on_hand, 0) - GREATEST(quantity_reserved, 0)             AS net_available_quantity,

        CASE
            WHEN GREATEST(quantity_on_hand, 0) = 0                                 THEN 'out_of_stock'
            WHEN GREATEST(quantity_on_hand, 0) <= GREATEST(reorder_point, 0)       THEN 'low_stock'
            ELSE 'in_stock'
        END                                                                         AS stock_status,

        CASE
            WHEN GREATEST(quantity_on_hand, 0) <= GREATEST(reorder_point, 0)       THEN TRUE
            ELSE FALSE
        END                                                                         AS needs_reorder,

        -- Inventory value
        ROUND(
            GREATEST(quantity_on_hand, 0) * COALESCE(unit_cost, 0), 2
        )                                                                           AS inventory_value,

        ingested_at,
        source_system,
        CURRENT_TIMESTAMP                                                           AS transformed_at

    FROM source
    WHERE
        snapshot_id  IS NOT NULL
        AND product_id   IS NOT NULL
        AND snapshot_date IS NOT NULL
)

SELECT * FROM cleaned