
    
    

select
    transaction_id as unique_field,
    count(*) as n_records

from "iceberg"."bronze_raw_silver"."silver_payment_transactions"
where transaction_id is not null
group by transaction_id
having count(*) > 1


