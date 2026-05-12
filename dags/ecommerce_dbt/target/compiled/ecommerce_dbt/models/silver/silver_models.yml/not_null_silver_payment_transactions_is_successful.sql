
    
    



select is_successful
from "iceberg"."bronze_raw_silver"."silver_payment_transactions"
where is_successful is null


