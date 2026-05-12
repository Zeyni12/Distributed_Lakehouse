
    
    

with all_values as (

    select
        performance_tier as value_field,
        count(*) as n_records

    from "iceberg"."bronze_raw_gold"."gold_support_team_performance"
    group by performance_tier

)

select *
from all_values
where value_field not in (
    'top_performer','solid_performer','average','needs_support'
)


