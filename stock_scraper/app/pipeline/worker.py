import asyncio
import time
from typing import Dict, Any, Optional
from stock_scraper.app.scraper.screener_scraper import resolve_and_fetch
from stock_scraper.app.scraper.parser import parse_company_page
from stock_scraper.app.scraper.cleaner import clean_all
from stock_scraper.app.db.database import (
    get_pool, upsert_company_details, upsert_fundamentals,
    upsert_financials, upsert_ratios, upsert_insights,
    upsert_news, insert_scrape_log,
)
from stock_scraper.app.utils.logger import get_logger

logger = get_logger("worker")

DAILY_SECTIONS = {"company_info", "fundamentals", "ratios"}
WEEKLY_SECTIONS = {"company_info", "fundamentals", "financials", "ratios", "shareholding"}
FULL_SECTIONS = {"company_info", "fundamentals", "financials", "ratios", "insights", "news", "shareholding"}


def _sections_for_mode(mode: str) -> set:
    if mode == "daily_prices":
        return DAILY_SECTIONS
    elif mode == "weekly_financials":
        return WEEKLY_SECTIONS
    return FULL_SECTIONS


async def process_company(company: Dict[str, Any], mode: str = "full") -> bool:
    company_id = company["id"]
    company_name = company["name"]
    current_url = company.get("screener_url", "")
    sections = _sections_for_mode(mode)

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

        if resolved_url and resolved_url != current_url:
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE companies SET screener_url = $1, updated_at = NOW() WHERE id = $2",
                    resolved_url, company_id,
                )

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
            logger.warning(f"Empty parse result for {company_name} ({resolved_url})")
            return False

        cleaned = clean_all(parsed)

        async with pool.acquire() as conn:
            async with conn.transaction():
                if "company_info" in sections:
                    company_info = cleaned.get("company_info", {})
                    company_info.update({
                        "market_cap_str": cleaned.get("fundamentals", {}).get("market_cap_str"),
                        "current_price_str": cleaned.get("fundamentals", {}).get("current_price_str"),
                    })
                    await upsert_company_details(conn, company_id, company_info)

                if "fundamentals" in sections:
                    fundamentals = cleaned.get("fundamentals", {})
                    if fundamentals:
                        await upsert_fundamentals(conn, company_id, fundamentals)

                if "financials" in sections or "shareholding" in sections:
                    financials = cleaned.get("financials", {})
                    if "shareholding" in sections:
                        financials["shareholding"] = cleaned.get("shareholding", {})
                    await upsert_financials(conn, company_id, financials)

                if "ratios" in sections:
                    ratios = cleaned.get("ratios", {})
                    await upsert_ratios(conn, company_id, ratios)

                if "insights" in sections:
                    insights = cleaned.get("insights", {})
                    await upsert_insights(conn, company_id, insights)

                if "news" in sections:
                    news_items = cleaned.get("news", [])
                    await upsert_news(conn, company_id, news_items)

                duration_ms = int((time.monotonic() - start_time) * 1000)
                await insert_scrape_log(conn, company_id, "success", None, duration_ms)

        logger.info(
            f"Successfully scraped {company_name} (mode={mode})",
            extra={"company_id": company_id, "company_name": company_name, "duration_ms": duration_ms, "status": "success", "mode": mode},
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


async def run_scrape_batch(companies: list, mode: str = "full") -> Dict[str, int]:
    total = len(companies)
    success_count = 0
    failure_count = 0

    logger.info(f"Starting scrape batch: {total} companies, mode={mode}")

    tasks = []
    for company in companies:
        tasks.append(process_company(company, mode=mode))

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
