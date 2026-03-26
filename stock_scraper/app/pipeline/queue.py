import asyncio
from typing import List, Dict, Any, Optional
from stock_scraper.app.db.database import get_pool, get_all_companies
from stock_scraper.app.utils.logger import get_logger

logger = get_logger("queue")


async def load_companies(
    limit: Optional[int] = None,
    offset: int = 0,
    company_ids: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        if company_ids:
            rows = await conn.fetch(
                "SELECT id, name, screener_url, symbol FROM companies WHERE id = ANY($1) ORDER BY id",
                company_ids,
            )
        elif limit:
            rows = await conn.fetch(
                "SELECT id, name, screener_url, symbol FROM companies ORDER BY id LIMIT $1 OFFSET $2",
                limit, offset,
            )
        else:
            rows = await conn.fetch(
                "SELECT id, name, screener_url, symbol FROM companies ORDER BY id"
            )
    companies = [dict(r) for r in rows]
    logger.info(f"Loaded {len(companies)} companies from database")
    return companies


async def load_failed_companies() -> List[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT c.id, c.name, c.screener_url, c.symbol
            FROM companies c
            INNER JOIN scrape_logs sl ON c.id = sl.company_id
            WHERE sl.status = 'failure'
            AND c.id NOT IN (
                SELECT company_id FROM scrape_logs WHERE status = 'success'
            )
            ORDER BY c.id
            """
        )
    companies = [dict(r) for r in rows]
    logger.info(f"Loaded {len(companies)} failed companies for retry")
    return companies


async def load_unscraped_companies() -> List[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT c.id, c.name, c.screener_url, c.symbol
            FROM companies c
            LEFT JOIN scrape_logs sl ON c.id = sl.company_id AND sl.status = 'success'
            WHERE sl.id IS NULL
            ORDER BY c.id
            """
        )
    companies = [dict(r) for r in rows]
    logger.info(f"Loaded {len(companies)} unscraped companies")
    return companies


def chunk_list(lst: list, chunk_size: int) -> List[list]:
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
