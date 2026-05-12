
    
    



select is_product_event
from "iceberg"."bronze_raw_silver"."silver_customer_events"
where is_product_event is null


