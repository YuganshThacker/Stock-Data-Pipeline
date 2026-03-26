import asyncpg
from typing import Optional, List, Dict, Any
from stock_scraper.app.config import settings
from stock_scraper.app.utils.logger import get_logger

logger = get_logger("database")

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None or _pool._closed:
        _pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            min_size=5,
            max_size=20,
            command_timeout=60,
        )
        logger.info("Database connection pool created")
    return _pool


async def close_pool():
    global _pool
    if _pool and not _pool._closed:
        await _pool.close()
        logger.info("Database connection pool closed")
        _pool = None


async def upsert_company(conn, name: str, screener_url: str, symbol: Optional[str] = None) -> int:
    row = await conn.fetchrow(
        """
        INSERT INTO companies (name, screener_url, symbol)
        VALUES ($1, $2, $3)
        ON CONFLICT (name) DO UPDATE SET
            screener_url = COALESCE(EXCLUDED.screener_url, companies.screener_url),
            symbol = COALESCE(EXCLUDED.symbol, companies.symbol),
            updated_at = NOW()
        RETURNING id
        """,
        name, screener_url, symbol,
    )
    return row["id"]


async def upsert_company_details(conn, company_id: int, data: Dict[str, Any]):
    symbol = data.get("symbol")
    sector = data.get("sector")
    industry = data.get("industry")
    market_cap = data.get("market_cap_str")
    current_price = data.get("current_price_str")

    await conn.execute(
        """
        UPDATE companies SET
            symbol = COALESCE($2, symbol),
            sector = COALESCE($3, sector),
            industry = COALESCE($4, industry),
            market_cap = COALESCE($5, market_cap),
            current_price = COALESCE($6, current_price),
            updated_at = NOW()
        WHERE id = $1
        """,
        company_id, symbol, sector, industry, market_cap, current_price,
    )


async def upsert_fundamentals(conn, company_id: int, data: Dict[str, Any]):
    import json
    raw_json = json.dumps(data.get("raw_data", {}))

    await conn.execute(
        """
        INSERT INTO fundamentals (company_id, market_cap, current_price, pe, pb, roce, roe,
            debt_to_equity, eps, dividend_yield, sales_growth, profit_growth,
            face_value, book_value, high_low, stock_pe, industry_pe,
            intrinsic_value, pledged_pct, change_in_promoter_holding, raw_data)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
        ON CONFLICT (company_id) DO UPDATE SET
            market_cap = COALESCE(EXCLUDED.market_cap, fundamentals.market_cap),
            current_price = COALESCE(EXCLUDED.current_price, fundamentals.current_price),
            pe = COALESCE(EXCLUDED.pe, fundamentals.pe),
            pb = COALESCE(EXCLUDED.pb, fundamentals.pb),
            roce = COALESCE(EXCLUDED.roce, fundamentals.roce),
            roe = COALESCE(EXCLUDED.roe, fundamentals.roe),
            debt_to_equity = COALESCE(EXCLUDED.debt_to_equity, fundamentals.debt_to_equity),
            eps = COALESCE(EXCLUDED.eps, fundamentals.eps),
            dividend_yield = COALESCE(EXCLUDED.dividend_yield, fundamentals.dividend_yield),
            sales_growth = COALESCE(EXCLUDED.sales_growth, fundamentals.sales_growth),
            profit_growth = COALESCE(EXCLUDED.profit_growth, fundamentals.profit_growth),
            face_value = COALESCE(EXCLUDED.face_value, fundamentals.face_value),
            book_value = COALESCE(EXCLUDED.book_value, fundamentals.book_value),
            high_low = COALESCE(EXCLUDED.high_low, fundamentals.high_low),
            stock_pe = COALESCE(EXCLUDED.stock_pe, fundamentals.stock_pe),
            industry_pe = COALESCE(EXCLUDED.industry_pe, fundamentals.industry_pe),
            intrinsic_value = COALESCE(EXCLUDED.intrinsic_value, fundamentals.intrinsic_value),
            pledged_pct = COALESCE(EXCLUDED.pledged_pct, fundamentals.pledged_pct),
            change_in_promoter_holding = COALESCE(EXCLUDED.change_in_promoter_holding, fundamentals.change_in_promoter_holding),
            raw_data = COALESCE(EXCLUDED.raw_data, fundamentals.raw_data),
            updated_at = NOW()
        """,
        company_id,
        data.get("market_cap"), data.get("current_price"),
        data.get("pe"), data.get("pb"), data.get("roce"), data.get("roe"),
        data.get("debt_to_equity"), data.get("eps"), data.get("dividend_yield"),
        data.get("sales_growth"), data.get("profit_growth"),
        data.get("face_value"), data.get("book_value"), data.get("high_low"),
        data.get("stock_pe"), data.get("industry_pe"),
        data.get("intrinsic_value"), data.get("pledged_pct"),
        data.get("change_in_promoter_holding"),
        raw_json,
    )


async def upsert_financials(conn, company_id: int, data: Dict[str, Any]):
    import json
    await conn.execute(
        """
        INSERT INTO financials (company_id, profit_loss, balance_sheet, cash_flow, quarterly, shareholding)
        VALUES ($1, $2::jsonb, $3::jsonb, $4::jsonb, $5::jsonb, $6::jsonb)
        ON CONFLICT (company_id) DO UPDATE SET
            profit_loss = COALESCE(EXCLUDED.profit_loss, financials.profit_loss),
            balance_sheet = COALESCE(EXCLUDED.balance_sheet, financials.balance_sheet),
            cash_flow = COALESCE(EXCLUDED.cash_flow, financials.cash_flow),
            quarterly = COALESCE(EXCLUDED.quarterly, financials.quarterly),
            shareholding = COALESCE(EXCLUDED.shareholding, financials.shareholding),
            updated_at = NOW()
        """,
        company_id,
        json.dumps(data.get("profit_loss", {})),
        json.dumps(data.get("balance_sheet", {})),
        json.dumps(data.get("cash_flow", {})),
        json.dumps(data.get("quarterly", {})),
        json.dumps(data.get("shareholding", {})),
    )


async def upsert_ratios(conn, company_id: int, data: Dict[str, Any]):
    import json
    await conn.execute(
        """
        INSERT INTO ratios (company_id, ratios_data)
        VALUES ($1, $2::jsonb)
        ON CONFLICT (company_id) DO UPDATE SET
            ratios_data = COALESCE(EXCLUDED.ratios_data, ratios.ratios_data),
            updated_at = NOW()
        """,
        company_id,
        json.dumps(data.get("ratios_data", {})),
    )


async def upsert_insights(conn, company_id: int, data: Dict[str, Any]):
    await conn.execute(
        """
        INSERT INTO insights (company_id, pros, cons, about)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (company_id) DO UPDATE SET
            pros = COALESCE(EXCLUDED.pros, insights.pros),
            cons = COALESCE(EXCLUDED.cons, insights.cons),
            about = COALESCE(EXCLUDED.about, insights.about),
            updated_at = NOW()
        """,
        company_id,
        data.get("pros", []),
        data.get("cons", []),
        data.get("about", ""),
    )


async def upsert_news(conn, company_id: int, news_items: List[Dict[str, Any]]):
    if not news_items:
        return
    records = [
        (company_id, item.get("title", ""), item.get("date"), item.get("source"), item.get("link"))
        for item in news_items
        if item.get("title")
    ]
    if not records:
        return
    await conn.executemany(
        """
        INSERT INTO news (company_id, title, news_date, source, link)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (company_id, title) DO UPDATE SET
            news_date = COALESCE(EXCLUDED.news_date, news.news_date),
            source = COALESCE(EXCLUDED.source, news.source),
            link = COALESCE(EXCLUDED.link, news.link)
        """,
        records,
    )


async def insert_scrape_log(conn, company_id: int, status: str, error_message: Optional[str] = None, duration_ms: Optional[int] = None):
    await conn.execute(
        """
        INSERT INTO scrape_logs (company_id, status, error_message, duration_ms)
        VALUES ($1, $2, $3, $4)
        """,
        company_id, status, error_message, duration_ms,
    )


async def get_all_companies(conn) -> List[Dict[str, Any]]:
    rows = await conn.fetch("SELECT id, name, screener_url, symbol FROM companies ORDER BY id")
    return [dict(r) for r in rows]


async def get_company_count(conn) -> int:
    row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM companies")
    return row["cnt"]
