import asyncio
import argparse
import csv
import os
from typing import List, Optional

from stock_scraper.app.pipeline.scheduler import (
    run_full_scrape, run_retry_failed, run_unscraped,
    run_daily_prices, run_weekly_financials, run_monthly_full_refresh,
    run_symbols_scrape,
)
from stock_scraper.app.db.database import close_pool, get_pool
from stock_scraper.app.utils.logger import get_logger
from stock_scraper.app.config import settings  # noqa: F401

logger = get_logger("run_scraper")


async def main():
    parser = argparse.ArgumentParser(description="Stock Scraper - Screener.in Data Pipeline")
    parser.add_argument(
        "--mode",
        choices=["full", "retry", "unscraped", "daily_prices", "weekly_financials", "monthly_full", "symbols"],
        default="full",
        help="Scraping mode: full (all), retry (failed), unscraped, daily_prices, weekly_financials, monthly_full",
    )
    parser.add_argument("--limit", type=int, default=None,
                        help="Maximum number of companies to scrape")
    parser.add_argument("--offset", type=int, default=0,
                        help="Offset to start from (for pagination)")
    parser.add_argument("--batch-size", type=int, default=settings.batch_size,
                        help="Number of companies per batch")
    parser.add_argument(
        "--symbols-file",
        type=str,
        default=None,
        help="CSV/text file containing a column/header named `symbol` (or first column) with symbols to scrape",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Comma-separated list of symbols to scrape",
    )

    args = parser.parse_args()

    logger.info(f"Starting scraper: mode={args.mode}, limit={args.limit}, offset={args.offset}, batch_size={args.batch_size}")

    def _load_symbols_file(path: str) -> List[str]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"symbols-file not found: {path}")

        _, ext = os.path.splitext(path.lower())
        if ext == ".csv":
            with open(path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    return []
                if "symbol" in reader.fieldnames:
                    return [row["symbol"] for row in reader if row.get("symbol")]
                # Fall back: first column in the CSV
                first_col = reader.fieldnames[0]
                return [row[first_col] for row in reader if row.get(first_col)]
        else:
            with open(path, "r", encoding="utf-8") as f:
                syms = []
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    syms.extend([p.strip() for p in line.split(",") if p.strip()])
                return syms

    def _parse_symbols_arg(s: Optional[str]) -> List[str]:
        if not s:
            return []
        return [p.strip() for p in s.split(",") if p.strip()]

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM company_data")
            logger.info(f"Total companies in database: {count}")

        mode_handlers = {
            "full": lambda: run_full_scrape(limit=args.limit, offset=args.offset, batch_size=args.batch_size),
            "retry": lambda: run_retry_failed(batch_size=args.batch_size),
            "unscraped": lambda: run_unscraped(batch_size=args.batch_size),
            "daily_prices": lambda: run_daily_prices(batch_size=args.batch_size),
            "weekly_financials": lambda: run_weekly_financials(batch_size=args.batch_size),
            "monthly_full": lambda: run_monthly_full_refresh(batch_size=args.batch_size),
        }

        if args.mode == "symbols":
            if not args.symbols_file and not args.symbols:
                parser.error("For --mode symbols, you must provide --symbols-file and/or --symbols")

            symbols: List[str] = []
            if args.symbols_file:
                symbols.extend(_load_symbols_file(args.symbols_file))
            if args.symbols:
                symbols.extend(_parse_symbols_arg(args.symbols))

            # Dedupe while keeping order
            seen = set()
            symbols = [s for s in symbols if not (s in seen or seen.add(s))]

            if not symbols:
                logger.info("No symbols resolved for targeted scrape")
                return

            mode_handlers["symbols"] = lambda: run_symbols_scrape(symbols=symbols, batch_size=args.batch_size, scrape_mode="full")

        handler = mode_handlers[args.mode]
        result = await handler()

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
