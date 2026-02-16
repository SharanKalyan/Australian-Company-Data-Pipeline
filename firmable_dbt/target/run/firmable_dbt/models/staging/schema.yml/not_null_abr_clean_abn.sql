
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select abn
from "firmable_db"."staging"."abr_clean"
where abn is null



  
  
      
    ) dbt_internal_test