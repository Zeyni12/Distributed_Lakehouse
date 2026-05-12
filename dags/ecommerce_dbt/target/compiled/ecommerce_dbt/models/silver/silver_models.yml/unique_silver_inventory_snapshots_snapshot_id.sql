
    
    

select
    snapshot_id as unique_field,
    count(*) as n_records

from "iceberg"."bronze_raw_silver"."silver_inventory_snapshots"
where snapshot_id is not null
group by snapshot_id
having count(*) > 1


