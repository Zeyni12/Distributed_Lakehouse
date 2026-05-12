
    
    

with all_values as (

    select
        value_segment as value_field,
        count(*) as n_records

    from "iceberg"."bronze_raw_gold"."gold_customer_360"
    group by value_segment

)

select *
from all_values
where value_field not in (
    'high_value','mid_value','low_value','no_purchase'
)


