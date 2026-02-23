CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;


CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

SELECT current_database();

SELECT COUNT(*) FROM staging.commoncrawl_raw;


SELECT * FROM staging.commoncrawl_raw LIMIT 50;

SELECT COUNT(*) FROM staging.commoncrawl_raw;

SELECT DISTINCT company_name 
FROM staging.commoncrawl_raw;

select * from staging.commoncrawl_clean;

SELECT COUNT(*) FROM staging.abr_raw;

select * from staging.abr_raw
limit 100
;

DROP TABLE staging.abr_raw;

DROP TABLE staging.abr_clean;


SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'staging'
AND table_name = 'abr_raw';

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'staging'
AND table_name = 'abr_clean';


select count(*) from staging.abr_clean;


select * from staging.commoncrawl_raw limit 500;

TRUNCATE TABLE staging.commoncrawl_raw;
TRUNCATE TABLE staging.commoncrawl_clean;

select count(*) from staging.commoncrawl_raw;

select count(*) from staging.commoncrawl_clean;


SELECT website_url, normalized_name FROM staging.commoncrawl_clean LIMIT 10;


SELECT normalized_name FROM staging.abr_clean LIMIT 50;

select * from staging.abr_clean limit 100;

SELECT * FROM staging.commoncrawl_clean LIMIT 100;

SELECT c.normalized_name
FROM staging.commoncrawl_clean c
JOIN staging.abr_clean a
ON c.normalized_name = a.normalized_name;

SELECT COUNT(*) FROM core.company_master;

SELECT * FROM staging.commoncrawl_clean
LIMIT 20;

TRUNCATE TABLE staging.abr_raw;
TRUNCATE TABLE staging.abr_clean;


select * from staging.abr_clean limit 100;

select * from staging.commoncrawl_clean limit 100;

SELECT company_name FROM staging.commoncrawl_raw LIMIT 20;

select * from staging.commoncrawl_clean limit 50;



-- TRUNCATE TABLE staging.commoncrawl_raw;
-- TRUNCATE TABLE staging.commoncrawl_clean;

-- TRUNCATE TABLE core.company_master;

SELECT COUNT(*) FROM staging.commoncrawl_raw;
SELECT COUNT(*) FROM staging.commoncrawl_clean;

SELECT COUNT(*) FROM staging.abr_raw;
SELECT COUNT(*) FROM staging.abr_clean;

SELECT COUNT(*) FROM core.company_master;

SELECT match_method, COUNT(*)
FROM core.company_master
GROUP BY match_method;

select * from staging.commoncrawl_raw limit 200;

select * from staging.commoncrawl_clean limit 200;


-- truncate table core.company_master;

-- truncate table core.ai_mat

select * from core.ai_match_log
where decision = true
;

select * from core.company_master
where match_method = 'ai_validated';
;

SELECT uuid_generate_v4();

select count(*) from staging.commoncrawl_clean;

select count(*) from staging.commoncrawl_raw;

select * from staging.abr_raw limit 100;



SELECT table_type
FROM information_schema.tables
WHERE table_schema = 'staging'
AND table_name = 'commoncrawl_clean';

DROP VIEW staging.commoncrawl_clean;

SELECT company_name
FROM staging.commoncrawl_raw
WHERE company_name != trim(company_name)
LIMIT 10;


-- TRUNCATE staging.commoncrawl_raw;
TRUNCATE staging.commoncrawl_clean;


-- truncate table core.company_master;

-- truncate table core.ai_match_log;

select count(*) from core.company_master;

select count(*) from core.ai_match_log;

select * from staging.commoncrawl_raw 
order by loaded_at desc 
limit 100;

select count(*) from staging.commoncrawl_clean;

select count(*) from staging.commoncrawl_raw
where industry is null;

select count(*) from staging.commoncrawl_raw;

select count(*) from staging.abr_clean;

select * from staging.commoncrawl_clean 
limit 100 ; 

select count(*) from core.company_master;

select count(*) from core.ai_match_log;

select * from core.company_master
order by created_at desc
limit 1000
;


select company_name, string_agg(distinct website_url, ' | ') as "listagg" , count(website_url) as "counts" from core.company_master
group by 1
order by counts desc
;

select distinct count(abn) from core.company_master;

select count(*) from core.company_master;

select * from core.ai_match_log;

SELECT * FROM core.company_master WHERE postcode IS NULL OR state IS NULL;


truncate table core.company_master;
truncate table core.ai_match_log;

-- truncate table staging.commoncrawl_clean;

select count(*) from staging.commoncrawl_raw;

select count(*) from staging.commoncrawl_clean;

--------------------------------------------------------------



--------------------------------------

-- Staging.commoncrawl_raw
select count(*) from staging.commoncrawl_raw;
select * from staging.commoncrawl_raw limit 10; 
-- website_url, company_name,industry,loaded_at


--------------------------------------
-- Staging.abr_raw
select count(*) from staging.abr_raw;
select * from staging.abr_raw limit 10; 
-- abn, entity_name,entity_type,entity_status,address_line,postcode,state,start_date,loaded_at

-- stats
select count(distinct(entity_type)) from staging.abr_raw; --47 unique
select distinct(entity_type) from staging.abr_raw; 

select count(distinct(entity_status)) from staging.abr_raw; --2 unique
select distinct(entity_status) from staging.abr_raw; -- ACT(ive) | CAN(celled)

select count(distinct(raw.state)) from staging.abr_raw as raw; --9 unique (1 [null] | 1 " ")
select distinct(raw.state) from staging.abr_raw as raw; -- ACT, NSW, NT, QLD, SA, TAS, VIC, WA, "", [null] 

select start_date from staging.abr_raw 
order by 1 asc
limit 1; -- 1999-01-11

select start_date from staging.abr_raw 
order by 1 desc
limit 1; -- 2026-04-01


select state , count(distinct abn) as "counts"
from staging.abr_raw
group by 1
order by "counts" desc;

select entity_status , count(distinct abn) as "counts"
from staging.abr_raw
group by 1
order by "counts" desc;

--------------------------------------

-- Staging.commoncrawl_clean

select count(*) from staging.commoncrawl_clean;
select * from staging.commoncrawl_clean limit 10; 
-- website_url, company_name, normalized_name, industry

select company_name, count(website_url) as "counts"
from staging.commoncrawl_clean
group by 1
order by "counts" desc;

--------------------------------------

-- Staging.abr_clean

select count(*) from staging.abr_clean;
select * from staging.abr_clean limit 10; 
-- website_url, company_name, normalized_name, industry

-- stats
select entity_status, count(abn) as "counts"
from staging.abr_clean
group by 1
order by "counts" desc; -- All are ACT(ive)

--------------------------------------

-- core.company_master
select count(*) from core.company_master;
select * from core.company_master limit 10;
-- company_id, abn, website_url, company_name, industry, 
-- entity_type, entity_status, state, postcode, match_method, match_confidence, created_at

-- stats
select distinct match_method , count(*) from core.company_master
group by 1
order by 2 desc
;


select company_name, count(website_url) as "counts"
from staging.commoncrawl_clean
group by 1
order by "counts" desc;


---------------------------------------

select * from core.ai_match_log ;
select * from core.company_master;

-- Joining the 2 core tables is only possible through company names. 
select m.company_name , website_url, a.company_a, a.company_b from core.company_master as m
left join core.ai_match_log as a
on a.company_b = m.company_name;












