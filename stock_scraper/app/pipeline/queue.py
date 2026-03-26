import asyncio
from typing import List, Dict, Any, Optional
from stock_scraper.app.db.database import get_pool
from stock_scraper.app.utils.logger import get_logger

logger = get_logger("queue")


class CompanyQueue:
    def __init__(self):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._total_loaded = 0

    async def load_all(
        self,
        limit: Optional[int] = None,
        offset: int = 0,
        company_ids: Optional[List[int]] = None,
    ):
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

        for row in rows:
            await self._queue.put(dict(row))
        self._total_loaded = len(rows)
        logger.info(f"Loaded {self._total_loaded} companies into queue")

    async def load_failed(self):
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
        for row in rows:
            await self._queue.put(dict(row))
        self._total_loaded = len(rows)
        logger.info(f"Loaded {self._total_loaded} failed companies into queue")

    async def load_unscraped(self):
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
        for row in rows:
            await self._queue.put(dict(row))
        self._total_loaded = len(rows)
        logger.info(f"Loaded {self._total_loaded} unscraped companies into queue")

    async def get_batch(self, batch_size: int) -> List[Dict[str, Any]]:
        batch = []
        for _ in range(batch_size):
            if self._queue.empty():
                break
            batch.append(await self._queue.get())
        return batch

    def empty(self) -> bool:
        return self._queue.empty()

    def size(self) -> int:
        return self._queue.qsize()

    @property
    def total_loaded(self) -> int:
        return self._total_loaded
