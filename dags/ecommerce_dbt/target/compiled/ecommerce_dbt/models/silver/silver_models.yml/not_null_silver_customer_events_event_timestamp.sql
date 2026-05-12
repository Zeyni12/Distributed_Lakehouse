
    
    



select event_timestamp
from "iceberg"."bronze_raw_silver"."silver_customer_events"
where event_timestamp is null


