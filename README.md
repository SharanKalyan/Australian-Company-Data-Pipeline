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

## Architecture (screenshot attached in Architecture folder for better clarity and visibility) 

<img width="826" height="3686" alt="architecture-darkmode" src="https://github.com/user-attachments/assets/a24f4fc7-72d5-4e1f-b9af-ef3877eb3c80" />


## Dataset Statistics

| Table Name                 | Counts   |
|----------------------------|----------|
| ABR Raw Records Loaded	   | 500,000  |
| ABR Clean Records          | 118,882  |
| Common Crawl Raw Records	 | 799      |
| Common Crawl Clean Records | 799      |
| Total Matches Identified   | 50       |
| Fuzzy Auto Matches	       | 42       |
| AI Validated Matches	     | 8        |

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
- not_null on ABN
- unique on ABN
- not_null on normalized names
- not_null on website_url

Lineage graph and column documentation generated via: (Screenshots attached inside "architecture" folder)
- dbt docs generate
- dbt docs serve


## API Layer (FastAPI Serving Layer)

The project exposes a lightweight REST API built using FastAPI to serve unified and validated company records from the core.company_master table.
This layer demonstrates how the data pipeline transitions from batch processing into a queryable serving layer.
The API only exposes matched and validated entities, ensuring consumers interact with clean, integrated data.

Running the API: 
- python -m uvicorn src.api.api:app --reload
- Open http://127.0.0.1:8000/docs

Available Endpoints:
1. GET/
- Health check endpoint.
- Returns service status to confirm the API is running.
- {"status": "running", "service": "Firmable Company API"}

2. GET/companies?limit=10
- Returns a list of unified (matched) company records from core.company_master.
- Query Parameters: limit (optional) Number of records to return. Default is 10.
- example: /companies?limit=5
  
3. GET/company/{abn}
- Returns a single unified company record by ABN.
- /company/12644536729
- If the ABN does not exist in the unified layer, a 404 response is returned.


## Setup & Running Instructions

1. Create Environment
  - conda create -n firmable-pipeline-311 python=3.11
  - conda activate firmable-pipeline-311 
  - pip install -r requirements.txt

2. setup PostgreSQL
   - Create database "firmable_db"
   - Run "schema.sql"

3. Run Ingestion 
  - python -m src.ingestion.commoncrawl_extractor
  - python -m src.ingestion.abr_parser

4. Run dbt
  - cd firmable_dbt
  - dbt run
  - dbt test

5. Run Matching
  - python -m src.matching.entity_matcher

## Design Decisions

- Python for procedural parsing (XML/WARC)
- dbt for warehouse-native transformations and testing
- Hybrid matching instead of purely fuzzy
- AI only for borderline cases to control cost
- Prompt logging for auditability
- Role-based database security

## IDE Used

- Visual Studio Code
- pgAdmin 4
- dbt CLI
- Ollama (local LLM runtime)

## Repository Structure
- data/
- src/
- firmable_dbt/
- sql/
- architecture/
- README.md

# Final Notes

This project demonstrates:
- Data engineering fundamentals
- Entity resolution strategy
- AI-assisted validation
- Warehouse modeling with dbt
- Governance & security
- Scalable architecture thinking

It is designed to simulate a production-grade integration pipeline under realistic constraints.
