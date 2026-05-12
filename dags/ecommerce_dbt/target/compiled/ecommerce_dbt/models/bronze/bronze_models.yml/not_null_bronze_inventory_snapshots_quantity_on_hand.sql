
    
    



select quantity_on_hand
from "iceberg"."bronze_raw_bronze"."bronze_inventory_snapshots"
where quantity_on_hand is null


