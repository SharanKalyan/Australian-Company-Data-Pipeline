
  create view "firmable_db"."staging"."abr_clean__dbt_tmp"
    
    
  as (
    with source as (

    select *
    from "firmable_db"."staging"."abr_raw"

)

select
    abn,
    initcap(trim(entity_name)) as entity_name,
    lower(regexp_replace(entity_name, '[^a-zA-Z0-9 ]', '', 'g')) as normalized_name,
    entity_type,
    entity_status,
    state,
    postcode
from source
where entity_status = 'ACT'
  and entity_name is not null
  and trim(entity_name) <> ''
  );