
    
    



select needs_reorder
from "iceberg"."bronze_raw_silver"."silver_inventory_snapshots"
where needs_reorder is null


