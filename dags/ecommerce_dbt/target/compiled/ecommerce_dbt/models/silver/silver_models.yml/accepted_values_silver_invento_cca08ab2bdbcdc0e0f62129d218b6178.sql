
    
    

with all_values as (

    select
        stock_status as value_field,
        count(*) as n_records

    from "iceberg"."bronze_raw_silver"."silver_inventory_snapshots"
    group by stock_status

)

select *
from all_values
where value_field not in (
    'in_stock','low_stock','out_of_stock'
)


