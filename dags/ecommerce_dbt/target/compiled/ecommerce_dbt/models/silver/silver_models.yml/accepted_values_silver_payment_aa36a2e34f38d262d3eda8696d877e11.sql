
    
    

with all_values as (

    select
        risk_tier as value_field,
        count(*) as n_records

    from "iceberg"."bronze_raw_silver"."silver_payment_transactions"
    group by risk_tier

)

select *
from all_values
where value_field not in (
    'low','medium','high'
)


