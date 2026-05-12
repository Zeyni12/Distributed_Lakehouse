
    
    

with all_values as (

    select
        satisfaction_category as value_field,
        count(*) as n_records

    from (select * from "iceberg"."bronze_raw_silver"."silver_support_tickets" where satisfaction_category IS NOT NULL) dbt_subquery
    group by satisfaction_category

)

select *
from all_values
where value_field not in (
    'satisfied','neutral','dissatisfied'
)


