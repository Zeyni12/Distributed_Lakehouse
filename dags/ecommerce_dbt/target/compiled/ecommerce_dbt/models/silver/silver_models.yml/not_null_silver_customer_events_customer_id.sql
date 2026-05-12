
    
    



select customer_id
from "iceberg"."bronze_raw_silver"."silver_customer_events"
where customer_id is null


