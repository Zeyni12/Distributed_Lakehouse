
    
    

with all_values as (

    select
        stock_trend as value_field,
        count(*) as n_records

    from "iceberg"."bronze_raw_gold"."gold_inventory_health"
    group by stock_trend

)

select *
from all_values
where value_field not in (
    'increasing','stable','decreasing'
)


