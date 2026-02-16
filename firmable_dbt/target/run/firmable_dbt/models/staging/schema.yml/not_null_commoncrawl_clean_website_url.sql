
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select website_url
from "firmable_db"."staging"."commoncrawl_clean"
where website_url is null



  
  
      
    ) dbt_internal_test