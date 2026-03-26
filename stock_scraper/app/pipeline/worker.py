import asyncio
import time
from typing import Dict, Any
from stock_scraper.app.scraper.screener_scraper import resolve_and_fetch
from stock_scraper.app.scraper.parser import parse_company_page
from stock_scraper.app.scraper.cleaner import clean_all
from stock_scraper.app.db.database import (
    get_pool, upsert_company_data, insert_scrape_log,
)
from stock_scraper.app.utils.logger import get_logger

logger = get_logger("worker")


async def process_company(company: Dict[str, Any], mode: str = "full") -> bool:
    company_id = company["id"]
    company_name = company["name"]
    current_url = company.get("screener_url", "")

    start_time = time.monotonic()
    pool = await get_pool()

    try:
        html, resolved_url = await resolve_and_fetch(company_name, current_url)

        if html is None:
            duration_ms = int((time.monotonic() - start_time) * 1000)
            async with pool.acquire() as conn:
                await insert_scrape_log(conn, company_id, "not_found", "Company not found on Screener.in", duration_ms)
            logger.warning(f"Company not found on Screener.in: {company_name}")
            return False

        final_url = resolved_url or current_url

        parsed = parse_company_page(html)

        financials_parsed = parsed.get("financials", {})
        insights_parsed = parsed.get("insights", {})
        has_data = any([
            parsed.get("fundamentals"),
            financials_parsed.get("profit_loss"),
            financials_parsed.get("balance_sheet"),
            insights_parsed.get("about"),
        ])
        if not has_data:
            duration_ms = int((time.monotonic() - start_time) * 1000)
            async with pool.acquire() as conn:
                await insert_scrape_log(conn, company_id, "parse_empty", "Page parsed but no meaningful data extracted", duration_ms)
            logger.warning(f"Empty parse result for {company_name} ({final_url})")
            return False

        cleaned = clean_all(parsed)

        async with pool.acquire() as conn:
            async with conn.transaction():
                await upsert_company_data(conn, company_name, final_url, cleaned)

                duration_ms = int((time.monotonic() - start_time) * 1000)
                await insert_scrape_log(conn, company_id, "success", None, duration_ms)

        logger.info(
            f"Successfully scraped {company_name} (mode={mode})",
            extra={"company_id": company_id, "company_name": company_name, "duration_ms": duration_ms, "status": "success"},
        )
        return True

    except Exception as e:
        duration_ms = int((time.monotonic() - start_time) * 1000)
        error_msg = str(e)[:500]
        try:
            async with pool.acquire() as conn:
                await insert_scrape_log(conn, company_id, "failure", error_msg, duration_ms)
        except Exception:
            pass
        logger.error(
            f"Failed to scrape {company_name}: {error_msg}",
            extra={"company_id": company_id, "company_name": company_name, "duration_ms": duration_ms, "status": "failure"},
        )
        return False


async def run_scrape_batch(companies: list, mode: str = "full", concurrency: int = 5) -> Dict[str, int]:
    total = len(companies)
    success_count = 0
    failure_count = 0

    logger.info(f"Starting scrape batch: {total} companies, mode={mode}, concurrency={concurrency}")

    sem = asyncio.Semaphore(concurrency)

    async def _limited(company):
        async with sem:
            return await process_company(company, mode=mode)

    tasks = [_limited(c) for c in companies]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            failure_count += 1
            logger.error(f"Unexpected error for company {companies[i]['name']}: {result}")
        elif result:
            success_count += 1
        else:
            failure_count += 1

    logger.info(
        f"Batch complete: {success_count}/{total} succeeded, {failure_count} failed"
    )

    return {
        "total": total,
        "success": success_count,
        "failure": failure_count,
    }
