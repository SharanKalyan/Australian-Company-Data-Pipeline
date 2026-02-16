
    
    

select
    abn as unique_field,
    count(*) as n_records

from "firmable_db"."staging"."abr_clean"
where abn is not null
group by abn
having count(*) > 1


