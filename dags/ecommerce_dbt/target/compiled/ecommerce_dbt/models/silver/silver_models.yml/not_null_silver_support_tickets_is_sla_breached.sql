
    
    



select is_sla_breached
from "iceberg"."bronze_raw_silver"."silver_support_tickets"
where is_sla_breached is null


