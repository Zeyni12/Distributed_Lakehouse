
    
    



select event_timestamp
from "iceberg"."bronze_raw_bronze"."bronze_customer_events"
where event_timestamp is null


