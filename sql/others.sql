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
--TRUNCATE staging.commoncrawl_clean;


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

select * from core.company_master;

select * from core.ai_match_log;

SELECT * FROM core.company_master WHERE postcode IS NULL OR state IS NULL;


-- truncate table core.company_master;
-- truncate table core.ai_match_log;
