import asyncio
import os
import sys
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Query, HTTPException, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from stock_scraper.app.db.database import get_pool, close_pool
from stock_scraper.app.utils.logger import get_logger

logger = get_logger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_pool()
    logger.info("API server started, database pool initialized")
    yield
    await close_pool()
    logger.info("API server shutting down")


app = FastAPI(
    title="Stock Scraper API",
    description="API for querying Indian stock market data scraped from Screener.in",
    version="1.0.0",
    lifespan=lifespan,
    root_path="/stock-api",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "stock-scraper-api"}


@app.get("/companies")
async def list_companies(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
    search: Optional[str] = Query(None),
    sector: Optional[str] = Query(None),
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        offset = (page - 1) * per_page
        conditions = []
        params = []
        param_idx = 1

        if search:
            conditions.append(f"(c.name ILIKE ${param_idx} OR c.symbol ILIKE ${param_idx})")
            params.append(f"%{search}%")
            param_idx += 1

        if sector:
            conditions.append(f"c.sector ILIKE ${param_idx}")
            params.append(f"%{sector}%")
            param_idx += 1

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        count_query = f"SELECT COUNT(*) FROM companies c {where_clause}"
        total = await conn.fetchval(count_query, *params)

        query = f"""
            SELECT c.id, c.name, c.symbol, c.sector, c.industry, c.market_cap, c.current_price,
                   c.screener_url, c.created_at, c.updated_at
            FROM companies c
            {where_clause}
            ORDER BY c.name
            LIMIT ${param_idx} OFFSET ${param_idx + 1}
        """
        params.extend([per_page, offset])

        rows = await conn.fetch(query, *params)
        companies = [dict(r) for r in rows]

    return {
        "data": companies,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "total_pages": (total + per_page - 1) // per_page,
        },
    }


@app.get("/companies/{company_id}")
async def get_company_detail(company_id: int = Path(..., ge=1)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        company = await conn.fetchrow(
            "SELECT * FROM companies WHERE id = $1", company_id
        )
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")

        fundamentals = await conn.fetchrow(
            "SELECT * FROM fundamentals WHERE company_id = $1", company_id
        )
        financials = await conn.fetchrow(
            "SELECT * FROM financials WHERE company_id = $1", company_id
        )
        ratios = await conn.fetchrow(
            "SELECT * FROM ratios WHERE company_id = $1", company_id
        )
        insights = await conn.fetchrow(
            "SELECT * FROM insights WHERE company_id = $1", company_id
        )
        news = await conn.fetch(
            "SELECT * FROM news WHERE company_id = $1 ORDER BY created_at DESC LIMIT 50", company_id
        )

    return {
        "company": dict(company),
        "fundamentals": dict(fundamentals) if fundamentals else None,
        "financials": dict(financials) if financials else None,
        "ratios": dict(ratios) if ratios else None,
        "insights": dict(insights) if insights else None,
        "news": [dict(n) for n in news],
    }


@app.get("/companies/{company_id}/news")
async def get_company_news(
    company_id: int = Path(..., ge=1),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        company = await conn.fetchrow("SELECT id FROM companies WHERE id = $1", company_id)
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")

        offset = (page - 1) * per_page
        total = await conn.fetchval(
            "SELECT COUNT(*) FROM news WHERE company_id = $1", company_id
        )
        news = await conn.fetch(
            "SELECT * FROM news WHERE company_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
            company_id, per_page, offset,
        )

    return {
        "data": [dict(n) for n in news],
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "total_pages": (total + per_page - 1) // per_page,
        },
    }


@app.get("/scrape-status")
async def get_scrape_status():
    pool = await get_pool()
    async with pool.acquire() as conn:
        total_companies = await conn.fetchval("SELECT COUNT(*) FROM companies")

        success_count = await conn.fetchval(
            "SELECT COUNT(DISTINCT company_id) FROM scrape_logs WHERE status = 'success'"
        )
        failure_count = await conn.fetchval(
            """
            SELECT COUNT(DISTINCT company_id) FROM scrape_logs
            WHERE status = 'failure'
            AND company_id NOT IN (SELECT company_id FROM scrape_logs WHERE status = 'success')
            """
        )
        not_found_count = await conn.fetchval(
            """
            SELECT COUNT(DISTINCT company_id) FROM scrape_logs
            WHERE status = 'not_found'
            AND company_id NOT IN (SELECT company_id FROM scrape_logs WHERE status = 'success')
            """
        )

        last_scrape = await conn.fetchrow(
            "SELECT scraped_at, status FROM scrape_logs ORDER BY scraped_at DESC LIMIT 1"
        )

        recent_failures = await conn.fetch(
            """
            SELECT c.name, sl.error_message, sl.scraped_at
            FROM scrape_logs sl
            JOIN companies c ON c.id = sl.company_id
            WHERE sl.status = 'failure'
            ORDER BY sl.scraped_at DESC
            LIMIT 10
            """
        )

    return {
        "total_companies": total_companies,
        "successfully_scraped": success_count,
        "failed": failure_count,
        "not_found": not_found_count,
        "unscraped": total_companies - success_count - failure_count - not_found_count,
        "last_scrape": dict(last_scrape) if last_scrape else None,
        "recent_failures": [dict(r) for r in recent_failures],
    }


@app.get("/sectors")
async def list_sectors():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT sector, COUNT(*) as company_count
            FROM companies
            WHERE sector IS NOT NULL AND sector != ''
            GROUP BY sector
            ORDER BY company_count DESC
            """
        )
    return {"sectors": [dict(r) for r in rows]}


def run_api_server():
    import uvicorn
    from stock_scraper.app.config import settings
    uvicorn.run(
        "stock_scraper.app.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    run_api_server()
