
  
    

    create table "iceberg"."bronze_raw_bronze"."bronze_inventory_snapshots__dbt_tmp"
      
      
    as (
      

SELECT
   snapshot_id, 
   product_id, 
   warehouse_id, 
   CAST(snapshot_date AS DATE) as snapshot_date,
   CAST(quantity_on_hand AS INTEGER) as quantity_on_hand,
   CAST(quantity_reserved AS INTEGER) as quantity_reserved,
   CAST(quantity_available AS INTEGER) as quantity_available,
   CAST(reorder_point AS INTEGER) as reorder_point,
   CAST(reorder_quantity AS INTEGER) as reorder_quantity,
   supplier_id, 
   CAST(last_received_date AS DATE) as  last_received_date,
   CAST(unit_cost AS DECIMAL(10,2)) as unit_cost,
   CURRENT_TIMESTAMP as ingested_at,
   'raw_inventory_snapshots' as source_system

FROM "iceberg"."bronze_raw_raw"."raw_inventory_snapshots"
    );

  