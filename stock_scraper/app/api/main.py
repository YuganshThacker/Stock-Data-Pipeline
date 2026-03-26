from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Query, HTTPException, Path, APIRouter
from fastapi.middleware.cors import CORSMiddleware

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
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

router = APIRouter(prefix="/stock-api")


@router.get("/health")
async def health_check():
    return {"status": "ok", "service": "stock-scraper-api"}


@router.get("/companies")
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
            conditions.append(f"(name ILIKE ${param_idx} OR symbol ILIKE ${param_idx})")
            params.append(f"%{search}%")
            param_idx += 1

        if sector:
            conditions.append(f"sector ILIKE ${param_idx}")
            params.append(f"%{sector}%")
            param_idx += 1

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        total = await conn.fetchval(f"SELECT COUNT(*) FROM company_data {where_clause}", *params)

        query = f"""
            SELECT id, name, symbol, sector, industry, market_cap, current_price,
                   pe, roe, roce, screener_url, data_quality, scraped_at, updated_at
            FROM company_data
            {where_clause}
            ORDER BY name
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


@router.get("/companies/by-id/{company_id}")
async def get_company_by_id(company_id: int = Path(..., ge=1)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM company_data WHERE id = $1", company_id)
    if not row:
        raise HTTPException(status_code=404, detail="Company not found")
    return dict(row)


@router.get("/companies/{symbol}")
async def get_company_detail(symbol: str = Path(..., min_length=1)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM company_data WHERE UPPER(symbol) = UPPER($1)", symbol
        )
        if not row:
            row = await conn.fetchrow(
                "SELECT * FROM company_data WHERE name ILIKE $1", f"%{symbol}%"
            )
    if not row:
        raise HTTPException(status_code=404, detail="Company not found")
    return dict(row)


@router.get("/companies/{symbol}/news")
async def get_company_news(
    symbol: str = Path(..., min_length=1),
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT news FROM company_data WHERE UPPER(symbol) = UPPER($1)", symbol
        )
        if not row:
            row = await conn.fetchrow(
                "SELECT news FROM company_data WHERE name ILIKE $1", f"%{symbol}%"
            )
    if not row:
        raise HTTPException(status_code=404, detail="Company not found")
    return {"news": row["news"] if row["news"] else []}


@router.get("/scrape-status")
async def get_scrape_status():
    pool = await get_pool()
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM company_data")
        scraped = await conn.fetchval("SELECT COUNT(*) FROM company_data WHERE scraped_at IS NOT NULL")
        pending = total - scraped

        quality_dist = await conn.fetch(
            """
            SELECT data_quality, COUNT(*) as count
            FROM company_data
            WHERE scraped_at IS NOT NULL
            GROUP BY data_quality
            """
        )

        last_scraped = await conn.fetchrow(
            "SELECT name, scraped_at FROM company_data WHERE scraped_at IS NOT NULL ORDER BY scraped_at DESC LIMIT 1"
        )

    return {
        "total_companies": total,
        "scraped": scraped,
        "pending": pending,
        "progress_pct": round(scraped / total * 100, 1) if total > 0 else 0,
        "quality_distribution": {r["data_quality"]: r["count"] for r in quality_dist},
        "last_scraped": dict(last_scraped) if last_scraped else None,
    }


@router.get("/sectors")
async def list_sectors():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT sector, COUNT(*) as company_count,
                   ROUND(AVG(pe), 2) as avg_pe,
                   ROUND(AVG(roe), 2) as avg_roe,
                   ROUND(AVG(roce), 2) as avg_roce
            FROM company_data
            WHERE sector IS NOT NULL AND sector != ''
            GROUP BY sector
            ORDER BY company_count DESC
            """
        )
    return {"sectors": [dict(r) for r in rows]}


@router.get("/search")
async def search_rag(
    q: str = Query(..., min_length=2),
    limit: int = Query(10, ge=1, le=50),
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, name, symbol, sector, market_cap, current_price, pe, roe, roce,
                   about, data_quality,
                   ts_rank(to_tsvector('english', rag_content), plainto_tsquery('english', $1)) as rank
            FROM company_data
            WHERE to_tsvector('english', rag_content) @@ plainto_tsquery('english', $1)
            ORDER BY rank DESC
            LIMIT $2
            """,
            q, limit,
        )
    return {"results": [dict(r) for r in rows], "query": q, "count": len(rows)}


app.include_router(router)


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
