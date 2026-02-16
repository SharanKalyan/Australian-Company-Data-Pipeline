
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select normalized_name
from "firmable_db"."staging"."abr_clean"
where normalized_name is null



  
  
      
    ) dbt_internal_test