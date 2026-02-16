-- Use the `ref` function to select from other models

select *
from "firmable_db"."staging"."my_first_dbt_model"
where id = 1