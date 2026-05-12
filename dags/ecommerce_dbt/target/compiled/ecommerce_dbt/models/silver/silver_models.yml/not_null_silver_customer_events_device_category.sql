
    
    



select device_category
from "iceberg"."bronze_raw_silver"."silver_customer_events"
where device_category is null


