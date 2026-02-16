with source as (

    select *
    from {{ source('staging', 'commoncrawl_raw') }}

)

select
    website_url,
    initcap(trim(company_name)) as company_name,
    lower(regexp_replace(company_name, '[^a-zA-Z0-9 ]', '', 'g')) as normalized_name,
    industry
from source
where company_name is not null
