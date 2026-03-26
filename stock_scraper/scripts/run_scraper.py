import asyncio
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from stock_scraper.app.pipeline.scheduler import run_full_scrape, run_retry_failed, run_unscraped
from stock_scraper.app.db.database import close_pool, get_pool
from stock_scraper.app.utils.logger import get_logger
from stock_scraper.app.config import settings

logger = get_logger("run_scraper")


async def main():
    parser = argparse.ArgumentParser(description="Stock Scraper - Screener.in Data Pipeline")
    parser.add_argument("--mode", choices=["full", "retry", "unscraped"], default="full",
                        help="Scraping mode: full (all companies), retry (failed only), unscraped (not yet scraped)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Maximum number of companies to scrape")
    parser.add_argument("--offset", type=int, default=0,
                        help="Offset to start from (for pagination)")
    parser.add_argument("--batch-size", type=int, default=settings.batch_size,
                        help="Number of companies per batch")

    args = parser.parse_args()

    logger.info(f"Starting scraper: mode={args.mode}, limit={args.limit}, offset={args.offset}, batch_size={args.batch_size}")

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM companies")
            logger.info(f"Total companies in database: {count}")

        if args.mode == "full":
            result = await run_full_scrape(
                limit=args.limit,
                offset=args.offset,
                batch_size=args.batch_size,
            )
        elif args.mode == "retry":
            result = await run_retry_failed(batch_size=args.batch_size)
        elif args.mode == "unscraped":
            result = await run_unscraped(batch_size=args.batch_size)

        if result:
            logger.info(f"Scraping completed: {result}")
        else:
            logger.info("No companies to process")

    except KeyboardInterrupt:
        logger.info("Scraper interrupted by user")
    except Exception as e:
        logger.error(f"Scraper failed: {e}", exc_info=True)
        raise
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
