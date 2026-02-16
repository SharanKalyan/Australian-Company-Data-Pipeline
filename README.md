# Australian-Company-Data-Pipeline
End-to-end data pipeline to extract, transform, and unify Australian company data from Common Crawl and the Australian Business Register (ABR) into PostgreSQL with advanced entity resolution and matching.


End-to-end data engineering pipeline that extracts, transforms, and integrates Australian company data from:

- Common Crawl (March 2025 index)
- Australian Business Register (ABR)

The pipeline performs hybrid entity resolution using fuzzy matching and AI validation to create a unified company view stored in PostgreSQL.

## Project Overview

This project demonstrates a scalable data pipeline that:

1. Extracts structured company data from large-scale public sources
2. Cleans and normalizes entity data
3. Applies hybrid entity matching (fuzzy + LLM validation)
4. Enforces data quality using dbt
5. Stores integrated records in a governed PostgreSQL schema

The architecture separates ingestion, transformation, matching, and core integration layers to reflect production-ready design principles.

## Architecture (screenshot attached in Architecture folder) 

<img width="826" height="2986" alt="architecture" src="https://github.com/user-attachments/assets/720990af-2993-427c-b248-dd72268c21e8" />

## Dataset Statistics

ABR Raw Records Loaded	500,000
ABR Clean Records	118,882
Common Crawl Raw Records	799
Common Crawl Clean Records	584
Total Matches Identified	50
Fuzzy Auto Matches	42
AI Validated Matches	8

## Note: The architecture supports scaling to 100k+ Common Crawl records with parallel execution or cloud deployment.

## Hybrid Entity Matching Strategy

The matching engine combines deterministic logic with semantic AI validation.

Step 1: Blocking
Reduce search space by grouping on normalized name prefixes.

Step 2: Fuzzy Matching
RapidFuzz token_sort_ratio scoring.

Step 3: Threshold Logic

| Score Range   | Action        |
|---------------|---------------|
| >= 85	        | Auto-accept   |
| 75–84	        | AI validation |
| < 75	        | Reject        |



Step 4: AI Semantic Validation
Local LLM via Ollama (phi3:mini)
Structured JSON response:
{
  "same_entity": true,
  "confidence": 0.92,
  "reason": "Minor formatting difference only"
}

## Data Cleaning & Normalisation

#### ABR:
1. Filter active entities
2. Trim whitespace
3. Remove punctuation
4. Normalize casing
5. Enforce unique ABN

#### Common Crawl:
1. Extract company name from:
  og:site_name
  og:title
  application-name
  twitter:title
  h1
  title fallback
2. Remove junk titles
3. Normalize for matching

## Technology Stack

| Component      | Technology         | Rationale                              |
| -------------- | ------------------ | -------------------------------------- |
| Ingestion      | Python             | Best for XML + WARC parsing            |
| Database       | PostgreSQL         | ACID compliance, indexing, constraints |
| Transformation | dbt                | Declarative SQL models + testing       |
| Matching       | RapidFuzz          | Efficient fuzzy matching               |
| AI Validation  | Ollama (phi3:mini) | Local LLM for semantic matching        |
| API Layer      | FastAPI            | Lightweight service exposure           |
| ORM            | SQLAlchemy         | Clean DB interaction                   |

## Database Design

#### Schemas 
1. staging
   - abr_raw
   - commonclean_raw
   - abr_clean
   - commoncrawl_clean
2. core
   - company_master
   - ai_match_log

## Indexing Strategy

CREATE INDEX idx_abr_clean_normalized_name
ON staging.abr_clean(normalized_name);

CREATE INDEX idx_commoncrawl_clean_normalized_name
ON staging.commoncrawl_clean(normalized_name);

CREATE INDEX idx_company_master_abn
ON core.company_master(abn);

CREATE INDEX idx_company_master_name
ON core.company_master(company_name);

Indexes support:
- Faster matching lookups
- Optimized joins
- Scalable search performance

## Roles & Permissions

Role-based access implemented:
1. analyst_readonly    
- SELECT on staging & core
- No write access
- Restricted modification privileges

2. Engineer Role
- Full access to staging and core
  
This enforces governance and production-style access control.

## Data Quality

Implemented:
not_null on ABN
unique on ABN
not_null on normalized names
not_null on website_url

Lineage graph and column documentation generated via: (Screenshots attached inside "architecture" folder)
dbt docs generate
dbt docs serve

