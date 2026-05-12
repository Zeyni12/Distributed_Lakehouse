
    
    



select is_churn_risk
from "iceberg"."bronze_raw_gold"."gold_customer_360"
where is_churn_risk is null


