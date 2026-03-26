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
                    "SELECT id, name, screener_url, symbol FROM company_data WHERE id = ANY($1) ORDER BY id",
                    company_ids,
                )
            elif limit:
                rows = await conn.fetch(
                    "SELECT id, name, screener_url, symbol FROM company_data ORDER BY id LIMIT $1 OFFSET $2",
                    limit, offset,
                )
            else:
                rows = await conn.fetch(
                    "SELECT id, name, screener_url, symbol FROM company_data ORDER BY id"
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
                SELECT DISTINCT cd.id, cd.name, cd.screener_url, cd.symbol
                FROM company_data cd
                INNER JOIN scrape_logs sl ON cd.id = sl.company_id
                WHERE sl.status = 'failure'
                AND cd.id NOT IN (
                    SELECT company_id FROM scrape_logs WHERE status = 'success'
                )
                ORDER BY cd.id
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
                SELECT cd.id, cd.name, cd.screener_url, cd.symbol
                FROM company_data cd
                WHERE cd.scraped_at IS NULL
                ORDER BY cd.id
                """
            )
        for row in rows:
            await self._queue.put(dict(row))
        self._total_loaded = len(rows)
        logger.info(f"Loaded {self._total_loaded} unscraped companies into queue")

    async def load_by_symbols(self, symbols: List[str]):
        """
        Load companies whose `company_data.symbol` matches the provided list.
        Used for targeted re-scrapes when only a subset of symbols need fixes.
        """
        normalized = [s.strip().upper() for s in (symbols or []) if s and str(s).strip()]
        if not normalized:
            self._total_loaded = 0
            logger.info("No symbols provided for targeted scrape")
            return

        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT cd.id, cd.name, cd.screener_url, cd.symbol
                FROM company_data cd
                WHERE UPPER(cd.symbol) = ANY($1)
                ORDER BY cd.id
                """,
                normalized,
            )

        found = len(rows)
        requested = len(set(normalized))
        self._total_loaded = found

        for row in rows:
            await self._queue.put(dict(row))

        missing = max(0, requested - found)
        logger.info(f"Loaded {found}/{requested} companies into queue (missing={missing})")

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
