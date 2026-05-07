{{ config(materialized='table', tags=['gold']) }}

SELECT
    warehouse_id,
    COUNT(*) AS total_products,
    SUM(inventory_value) AS total_inventory_value,

    SUM(
        CASE WHEN needs_restock THEN 1 ELSE 0 END
    ) AS low_stock_items

FROM {{ ref('silver_inventory') }}
GROUP BY warehouse_id