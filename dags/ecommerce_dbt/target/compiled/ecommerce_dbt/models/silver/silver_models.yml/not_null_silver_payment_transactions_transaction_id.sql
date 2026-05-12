
    
    



select transaction_id
from "iceberg"."bronze_raw_silver"."silver_payment_transactions"
where transaction_id is null


