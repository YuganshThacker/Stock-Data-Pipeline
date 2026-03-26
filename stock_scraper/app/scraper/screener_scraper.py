import asyncio
import httpx
import random
import time
from typing import Optional, Dict, Any, List
from stock_scraper.app.utils.logger import get_logger
from stock_scraper.app.utils.retry import exponential_backoff
from stock_scraper.app.config import settings

logger = get_logger("scraper")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]

SCREENER_BASE = "https://www.screener.in"


def _normalize_company_name(name: str) -> str:
    n = name.lower().strip()
    for suffix in [" ltd", " limited", " pvt", " private", " inc", " corp", " corporation"]:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    for char in [".", ",", "'", "(", ")", "&"]:
        n = n.replace(char, "")
    n = " ".join(n.split())
    return n

_semaphore: Optional[asyncio.Semaphore] = None
_rate_limiter_lock = asyncio.Lock()
_last_request_time = 0.0


def get_semaphore() -> asyncio.Semaphore:
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(settings.max_concurrent_requests)
    return _semaphore


async def _rate_limit():
    global _last_request_time
    async with _rate_limiter_lock:
        now = time.monotonic()
        min_interval = 1.0 / settings.rate_limit_per_second
        elapsed = now - _last_request_time
        if elapsed < min_interval:
            wait_time = min_interval - elapsed + random.uniform(0.05, 0.2)
            await asyncio.sleep(wait_time)
        _last_request_time = time.monotonic()


def _get_headers(ajax: bool = False) -> dict:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
    if ajax:
        headers["Accept"] = "application/json, text/javascript, */*; q=0.01"
        headers["X-Requested-With"] = "XMLHttpRequest"
    else:
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        headers["Upgrade-Insecure-Requests"] = "1"
        headers["Cache-Control"] = "max-age=0"
    return headers


@exponential_backoff(
    max_retries=settings.max_retries,
    base_delay=settings.retry_base_delay,
    max_delay=settings.retry_max_delay,
    exceptions=(httpx.HTTPError, httpx.TimeoutException, ConnectionError, OSError),
)
async def search_company(company_name: str) -> Optional[Dict[str, Any]]:
    sem = get_semaphore()
    async with sem:
        await _rate_limit()
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(settings.request_timeout),
            follow_redirects=True,
        ) as client:
            response = await client.get(
                f"{SCREENER_BASE}/api/company/search/",
                params={"q": company_name},
                headers=_get_headers(ajax=True),
            )
            if response.status_code == 429:
                raise httpx.HTTPError("Rate limited on search")
            response.raise_for_status()
            results = response.json()

            if not results:
                return None

            if not isinstance(results, list):
                logger.warning(f"Unexpected search response type for: {company_name}")
                return None

            name_lower = company_name.lower().strip()
            for r in results:
                if r.get("name", "").lower().strip() == name_lower:
                    return r

            name_no_ltd = _normalize_company_name(name_lower)

            for r in results:
                r_normalized = _normalize_company_name(r.get("name", "").lower().strip())
                if r_normalized == name_no_ltd:
                    return r

            for r in results:
                r_normalized = _normalize_company_name(r.get("name", "").lower().strip())
                if name_no_ltd in r_normalized or r_normalized in name_no_ltd:
                    return r

            logger.warning(f"No confident match for '{company_name}' among {len(results)} search results")
            return None


@exponential_backoff(
    max_retries=settings.max_retries,
    base_delay=settings.retry_base_delay,
    max_delay=settings.retry_max_delay,
    exceptions=(httpx.HTTPError, httpx.TimeoutException, ConnectionError, OSError),
)
async def fetch_company_page(url: str) -> Optional[str]:
    sem = get_semaphore()
    async with sem:
        await _rate_limit()
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(settings.request_timeout),
            follow_redirects=True,
            verify=True,
        ) as client:
            response = await client.get(url, headers=_get_headers())
            if response.status_code == 429:
                logger.warning(f"Rate limited (429) for {url}. Will retry.")
                raise httpx.HTTPError("Rate limited")
            if response.status_code == 404:
                logger.warning(f"Page not found (404) for {url}")
                return None
            response.raise_for_status()
            return response.text


async def resolve_and_fetch(company_name: str, current_url: Optional[str] = None) -> tuple[Optional[str], Optional[str]]:
    if current_url and not current_url.startswith("https://www.screener.in/company/search/"):
        html = await fetch_company_page(current_url)
        if html:
            return html, current_url
        if "/consolidated/" in current_url:
            standalone_url = current_url.replace("/consolidated/", "/")
            html = await fetch_company_page(standalone_url)
            if html:
                return html, standalone_url

    search_result = await search_company(company_name)
    if not search_result:
        short_name = _extract_short_search_term(company_name)
        if short_name and short_name.lower() != _normalize_company_name(company_name):
            logger.info(f"Retrying search with short term '{short_name}' for: {company_name}")
            search_result = await search_company(short_name)

    if not search_result:
        logger.warning(f"No search results for: {company_name}")
        return None, None

    url_path = search_result.get("url", "")
    if not url_path:
        return None, None

    full_url = f"{SCREENER_BASE}{url_path}"
    html = await fetch_company_page(full_url)
    return html, full_url


def _extract_short_search_term(company_name: str) -> Optional[str]:
    cleaned = _normalize_company_name(company_name)
    words = cleaned.split()
    stop_words = {"india", "the", "and", "of", "in", "for", "a", "an"}
    significant = [w for w in words if w.lower() not in stop_words and len(w) > 1]
    if not significant:
        return None
    if len(significant) == 1:
        return significant[0]
    return " ".join(significant[:2])


def company_name_to_screener_url(name: str) -> str:
    return f"https://www.screener.in/company/search/?q={name.strip()}"
