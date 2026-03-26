import asyncio
from datetime import datetime
from typing import Optional, List
from stock_scraper.app.pipeline.queue import CompanyQueue
from stock_scraper.app.pipeline.worker import run_scrape_batch
from stock_scraper.app.config import settings
from stock_scraper.app.utils.logger import get_logger

logger = get_logger("scheduler")


async def _run_queued_scrape(queue: CompanyQueue, mode: str, batch_size: int):
    total_results = {"total": 0, "success": 0, "failure": 0}
    batch_num = 0

    while not queue.empty():
        batch = await queue.get_batch(batch_size)
        if not batch:
            break
        batch_num += 1
        logger.info(f"Processing batch {batch_num} ({len(batch)} companies, mode={mode})")
        result = await run_scrape_batch(batch, mode=mode)
        total_results["total"] += result["total"]
        total_results["success"] += result["success"]
        total_results["failure"] += result["failure"]

    return total_results


async def run_full_scrape(limit: Optional[int] = None, offset: int = 0, batch_size: int = 50):
    queue = CompanyQueue()
    await queue.load_all(limit=limit, offset=offset)

    if queue.empty():
        logger.info("No companies to scrape")
        return None

    logger.info(f"Starting full scrape of {queue.size()} companies")
    result = await _run_queued_scrape(queue, "full", batch_size)
    logger.info(f"Full scrape complete: {result['success']}/{result['total']} succeeded, {result['failure']} failed")
    return result


async def run_daily_prices(batch_size: int = 50):
    queue = CompanyQueue()
    await queue.load_all()

    if queue.empty():
        logger.info("No companies for daily price update")
        return None

    logger.info(f"Starting daily price/ratio update for {queue.size()} companies")
    result = await _run_queued_scrape(queue, "daily_prices", batch_size)
    logger.info(f"Daily price update complete: {result['success']}/{result['total']} succeeded")
    return result


async def run_weekly_financials(batch_size: int = 50):
    queue = CompanyQueue()
    await queue.load_all()

    if queue.empty():
        logger.info("No companies for weekly financial update")
        return None

    logger.info(f"Starting weekly financial update for {queue.size()} companies")
    result = await _run_queued_scrape(queue, "weekly_financials", batch_size)
    logger.info(f"Weekly financial update complete: {result['success']}/{result['total']} succeeded")
    return result


async def run_monthly_full_refresh(batch_size: int = 50):
    queue = CompanyQueue()
    await queue.load_all()

    if queue.empty():
        logger.info("No companies for monthly full refresh")
        return None

    logger.info(f"Starting monthly full refresh for {queue.size()} companies")
    result = await _run_queued_scrape(queue, "full", batch_size)
    logger.info(f"Monthly full refresh complete: {result['success']}/{result['total']} succeeded")
    return result


async def run_retry_failed(batch_size: int = 50):
    queue = CompanyQueue()
    await queue.load_failed()

    if queue.empty():
        logger.info("No failed companies to retry")
        return None

    logger.info(f"Retrying {queue.size()} failed companies")
    result = await _run_queued_scrape(queue, "retry", batch_size)
    return result


async def run_unscraped(batch_size: int = 50):
    queue = CompanyQueue()
    await queue.load_unscraped()

    if queue.empty():
        logger.info("No unscraped companies remaining")
        return None

    logger.info(f"Scraping {queue.size()} unscraped companies")
    result = await _run_queued_scrape(queue, "full", batch_size)
    return result


async def run_symbols_scrape(symbols: List[str], batch_size: int = 50, scrape_mode: str = "full"):
    """
    Targeted scrape for a provided set of Screener symbols.
    """
    queue = CompanyQueue()
    await queue.load_by_symbols(symbols)

    if queue.empty():
        logger.info("No companies to scrape for provided symbols")
        return None

    logger.info(f"Starting targeted scrape of {queue.size()} companies ({scrape_mode})")
    result = await _run_queued_scrape(queue, scrape_mode, batch_size)
    logger.info(
        f"Targeted scrape complete: {result['success']}/{result['total']} succeeded, {result['failure']} failed"
    )
    return result


def start_scheduler():
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger

        scheduler = AsyncIOScheduler()

        scheduler.add_job(
            run_daily_prices,
            CronTrigger(hour=18, minute=0),
            id="daily_prices",
            name="Daily price and ratio update (after market close)",
            kwargs={"batch_size": settings.batch_size},
            replace_existing=True,
        )

        scheduler.add_job(
            run_weekly_financials,
            CronTrigger(day_of_week="sat", hour=2, minute=0),
            id="weekly_financials",
            name="Weekly financial data update",
            kwargs={"batch_size": settings.batch_size},
            replace_existing=True,
        )

        scheduler.add_job(
            run_monthly_full_refresh,
            CronTrigger(day=1, hour=2, minute=0),
            id="monthly_full_refresh",
            name="Monthly full data refresh",
            kwargs={"batch_size": settings.batch_size},
            replace_existing=True,
        )

        scheduler.add_job(
            run_retry_failed,
            CronTrigger(hour=6, minute=0),
            id="retry_failed",
            name="Daily retry of failed scrapes",
            kwargs={"batch_size": settings.batch_size},
            replace_existing=True,
        )

        scheduler.start()
        logger.info(
            "Scheduler started: daily prices at 18:00, weekly financials Sat 02:00, "
            "monthly full refresh 1st at 02:00, retry failed at 06:00"
        )
        return scheduler
    except ImportError:
        logger.warning("APScheduler not available. Scheduler disabled.")
        return None
