import asyncio
from datetime import datetime
from typing import Optional
from stock_scraper.app.pipeline.queue import load_companies, load_unscraped_companies, load_failed_companies, chunk_list
from stock_scraper.app.pipeline.worker import run_scrape_batch
from stock_scraper.app.config import settings
from stock_scraper.app.utils.logger import get_logger

logger = get_logger("scheduler")


async def run_full_scrape(limit: Optional[int] = None, offset: int = 0, batch_size: int = 50):
    companies = await load_companies(limit=limit, offset=offset)
    if not companies:
        logger.info("No companies to scrape")
        return

    logger.info(f"Starting full scrape of {len(companies)} companies")
    batches = chunk_list(companies, batch_size)
    total_results = {"total": 0, "success": 0, "failure": 0}

    for i, batch in enumerate(batches):
        logger.info(f"Processing batch {i+1}/{len(batches)} ({len(batch)} companies)")
        result = await run_scrape_batch(batch, mode="full")
        total_results["total"] += result["total"]
        total_results["success"] += result["success"]
        total_results["failure"] += result["failure"]

    logger.info(
        f"Full scrape complete: {total_results['success']}/{total_results['total']} succeeded, "
        f"{total_results['failure']} failed"
    )
    return total_results


async def run_retry_failed(batch_size: int = 50):
    companies = await load_failed_companies()
    if not companies:
        logger.info("No failed companies to retry")
        return

    logger.info(f"Retrying {len(companies)} failed companies")
    batches = chunk_list(companies, batch_size)
    total_results = {"total": 0, "success": 0, "failure": 0}

    for i, batch in enumerate(batches):
        logger.info(f"Retry batch {i+1}/{len(batches)}")
        result = await run_scrape_batch(batch, mode="retry")
        total_results["total"] += result["total"]
        total_results["success"] += result["success"]
        total_results["failure"] += result["failure"]

    return total_results


async def run_unscraped(batch_size: int = 50):
    companies = await load_unscraped_companies()
    if not companies:
        logger.info("No unscraped companies remaining")
        return

    logger.info(f"Scraping {len(companies)} unscraped companies")
    batches = chunk_list(companies, batch_size)
    total_results = {"total": 0, "success": 0, "failure": 0}

    for i, batch in enumerate(batches):
        logger.info(f"Unscraped batch {i+1}/{len(batches)}")
        result = await run_scrape_batch(batch, mode="full")
        total_results["total"] += result["total"]
        total_results["success"] += result["success"]
        total_results["failure"] += result["failure"]

    return total_results


def start_scheduler():
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger

        scheduler = AsyncIOScheduler()

        scheduler.add_job(
            run_full_scrape,
            CronTrigger(hour=2, minute=0),
            id="daily_scrape",
            name="Daily full scrape",
            kwargs={"batch_size": settings.batch_size},
            replace_existing=True,
        )

        scheduler.add_job(
            run_retry_failed,
            CronTrigger(hour=6, minute=0),
            id="retry_failed",
            name="Retry failed scrapes",
            kwargs={"batch_size": settings.batch_size},
            replace_existing=True,
        )

        scheduler.start()
        logger.info("Scheduler started with daily scrape at 2:00 AM and retry at 6:00 AM")
        return scheduler
    except ImportError:
        logger.warning("APScheduler not available. Scheduler disabled.")
        return None
