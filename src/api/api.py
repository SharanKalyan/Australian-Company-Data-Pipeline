from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import text
from src.db.connection import PostgresConnector
from typing import List
import os

# -----------------------------
# Configuration
# -----------------------------

DB_PASSWORD = os.getenv("DB_PASSWORD", "firmable")

app = FastAPI(
    title="Firmable Australian Company API",
    description="API for querying unified Australian company data",
    version="1.0.0"
)


# -----------------------------
# Health Check
# -----------------------------

@app.get("/")
def health_check():
    return {
        "status": "running",
        "service": "Firmable Company API"
    }


# -----------------------------
# Get Companies (Paginated)
# -----------------------------

@app.get("/companies")
def get_companies(
    limit: int = Query(10, ge=1, le=1000)
) -> List[dict]:

    db = PostgresConnector(password=DB_PASSWORD)

    query = text("""
        SELECT 
            abn,
            company_name,
            website_url,
            state,
            postcode,
            match_method,
            match_confidence
        FROM core.company_master
        LIMIT :limit
    """)

    try:
        with db.engine.connect() as conn:
            result = conn.execute(query, {"limit": limit})
            rows = [dict(row._mapping) for row in result]

        return rows

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )


# -----------------------------
# Get Company by ABN
# -----------------------------

@app.get("/company/{abn}")
def get_company_by_abn(abn: str):

    db = PostgresConnector(password=DB_PASSWORD)

    query = text("""
        SELECT *
        FROM core.company_master
        WHERE abn = :abn
    """)

    try:
        with db.engine.connect() as conn:
            result = conn.execute(query, {"abn": abn})
            row = result.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Company not found")

        return dict(row._mapping)

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )