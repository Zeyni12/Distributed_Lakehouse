{{ config(materialized='table', tags=['silver']) }}

SELECT
    snapshot_id,
    product_id,
    warehouse_id,
    snapshot_date,
    quantity_on_hand,
    quantity_reserved,
    quantity_available,
    reorder_point,
    supplier_id,
    last_received_date,
    unit_cost,

    CASE
        WHEN quantity_available <= reorder_point
        THEN TRUE
        ELSE FALSE
    END AS needs_restock,

    quantity_on_hand * unit_cost AS inventory_value,

    CURRENT_TIMESTAMP AS transformed_at

FROM {{ ref('bronze_inventory_snapshots') }}