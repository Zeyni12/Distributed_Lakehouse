
    
    

with all_values as (

    select
        device_category as value_field,
        count(*) as n_records

    from "iceberg"."bronze_raw_silver"."silver_customer_events"
    group by device_category

)

select *
from all_values
where value_field not in (
    'mobile','desktop','unknown'
)


