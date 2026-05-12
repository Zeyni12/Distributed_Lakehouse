
    
    

with all_values as (

    select
        stock_status as value_field,
        count(*) as n_records

    from "iceberg"."bronze_raw_gold"."gold_inventory_health"
    group by stock_status

)

select *
from all_values
where value_field not in (
    'in_stock','low_stock','out_of_stock'
)


