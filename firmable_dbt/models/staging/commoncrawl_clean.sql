{{ config(materialized='table') }}

with source as (

    select *
    from {{ source('staging', 'commoncrawl_raw') }}

),

cleaned as (

    select
        trim(website_url) as website_url,
        trim(company_name) as company_name,
        trim(industry) as industry
    from source
    where company_name is not null
      and trim(company_name) <> ''

)

select
    website_url,
    initcap(company_name) as company_name,
    lower(regexp_replace(company_name, '[^a-zA-Z0-9 ]', '', 'g')) as normalized_name,
    industry
from cleaned
