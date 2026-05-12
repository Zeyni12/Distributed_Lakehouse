
    
    



select customer_health_score
from "iceberg"."bronze_raw_gold"."gold_customer_360"
where customer_health_score is null


