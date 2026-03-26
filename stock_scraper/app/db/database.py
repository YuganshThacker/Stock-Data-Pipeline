import asyncpg
import json
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


def _build_rag_content(name: str, data: Dict[str, Any]) -> str:
    parts = [f"Company: {name}"]

    fundamentals = data.get("fundamentals", {})
    company_info = data.get("company_info", {})

    if company_info.get("sector"):
        parts.append(f"Sector: {company_info['sector']}")
    if company_info.get("industry"):
        parts.append(f"Industry: {company_info['industry']}")

    fund_labels = {
        "market_cap": "Market Cap (Cr)",
        "current_price": "Current Price (Rs)",
        "pe": "PE Ratio",
        "stock_pe": "Stock PE",
        "industry_pe": "Industry PE",
        "pb": "Price to Book",
        "roce": "ROCE (%)",
        "roe": "ROE (%)",
        "debt_to_equity": "Debt to Equity",
        "eps": "EPS",
        "dividend_yield": "Dividend Yield (%)",
        "sales_growth": "Sales Growth (%)",
        "profit_growth": "Profit Growth (%)",
        "book_value": "Book Value",
        "face_value": "Face Value",
        "opm": "OPM (%)",
        "promoter_holding": "Promoter Holding (%)",
    }
    fund_parts = []
    for key, label in fund_labels.items():
        val = fundamentals.get(key)
        if val is not None:
            fund_parts.append(f"{label}: {val}")
    if fund_parts:
        parts.append("Fundamentals: " + ", ".join(fund_parts))

    insights = data.get("insights", {})
    if insights.get("about"):
        parts.append(f"About: {insights['about']}")
    if insights.get("pros"):
        parts.append("Strengths: " + "; ".join(insights["pros"]))
    if insights.get("cons"):
        parts.append("Weaknesses: " + "; ".join(insights["cons"]))

    financials = data.get("financials", {})
    pl = financials.get("profit_loss", {})
    revenue = pl.get("sales", pl.get("revenue", pl.get("revenue_from_operations", {})))
    net_profit = pl.get("net_profit", pl.get("profit_after_tax", {}))
    if isinstance(revenue, dict):
        recent = {k: v for k, v in revenue.items() if "2025" in k or "2026" in k or "TTM" in k}
        if recent:
            parts.append("Recent Revenue: " + ", ".join(f"{k}: {v}" for k, v in recent.items()))
    if isinstance(net_profit, dict):
        recent = {k: v for k, v in net_profit.items() if "2025" in k or "2026" in k or "TTM" in k}
        if recent:
            parts.append("Recent Net Profit: " + ", ".join(f"{k}: {v}" for k, v in recent.items()))

    ratios = data.get("ratios", {}).get("ratios_data", {})
    for ratio_name in ("roce", "roe", "debtor_days", "inventory_days"):
        ratio_vals = ratios.get(ratio_name, {})
        if isinstance(ratio_vals, dict):
            recent = {k: v for k, v in ratio_vals.items() if "2025" in k or "2026" in k}
            if recent:
                parts.append(f"Recent {ratio_name.replace('_', ' ').title()}: " + ", ".join(f"{k}: {v}" for k, v in recent.items()))

    news = data.get("news", [])
    if news:
        news_titles = [n.get("title", "") for n in news[:5] if n.get("title")]
        if news_titles:
            parts.append("Recent News: " + "; ".join(news_titles))

    return "\n".join(parts)


async def upsert_company_data(conn, company_name: str, screener_url: str, cleaned: Dict[str, Any]):
    fundamentals = cleaned.get("fundamentals", {})
    company_info = cleaned.get("company_info", {})
    insights = cleaned.get("insights", {})
    financials = cleaned.get("financials", {})
    ratios = cleaned.get("ratios", {})
    shareholding = cleaned.get("shareholding", {})
    news_items = cleaned.get("news", [])
    data_quality = cleaned.get("data_quality", {})

    rag_content = _build_rag_content(company_name, cleaned)

    await conn.execute(
        """
        INSERT INTO company_data (
            name, symbol, sector, industry, screener_url,
            market_cap, current_price, pe, stock_pe, industry_pe, pb,
            roce, roe, debt_to_equity, eps, dividend_yield,
            sales_growth, profit_growth, face_value, book_value,
            high_low, intrinsic_value, pledged_pct, promoter_holding, opm,
            about, pros, cons,
            profit_loss, balance_sheet, cash_flow, quarterly_results, shareholding, ratios,
            news, rag_content, data_quality, data_completeness,
            scraped_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9, $10, $11,
            $12, $13, $14, $15, $16,
            $17, $18, $19, $20,
            $21, $22, $23, $24, $25,
            $26, $27, $28,
            $29::jsonb, $30::jsonb, $31::jsonb, $32::jsonb, $33::jsonb, $34::jsonb,
            $35::jsonb, $36, $37, $38,
            NOW(), NOW()
        )
        ON CONFLICT (name) DO UPDATE SET
            symbol = COALESCE(EXCLUDED.symbol, company_data.symbol),
            sector = COALESCE(EXCLUDED.sector, company_data.sector),
            industry = COALESCE(EXCLUDED.industry, company_data.industry),
            screener_url = COALESCE(EXCLUDED.screener_url, company_data.screener_url),
            market_cap = COALESCE(EXCLUDED.market_cap, company_data.market_cap),
            current_price = COALESCE(EXCLUDED.current_price, company_data.current_price),
            pe = COALESCE(EXCLUDED.pe, company_data.pe),
            stock_pe = COALESCE(EXCLUDED.stock_pe, company_data.stock_pe),
            industry_pe = COALESCE(EXCLUDED.industry_pe, company_data.industry_pe),
            pb = COALESCE(EXCLUDED.pb, company_data.pb),
            roce = COALESCE(EXCLUDED.roce, company_data.roce),
            roe = COALESCE(EXCLUDED.roe, company_data.roe),
            debt_to_equity = COALESCE(EXCLUDED.debt_to_equity, company_data.debt_to_equity),
            eps = COALESCE(EXCLUDED.eps, company_data.eps),
            dividend_yield = COALESCE(EXCLUDED.dividend_yield, company_data.dividend_yield),
            sales_growth = COALESCE(EXCLUDED.sales_growth, company_data.sales_growth),
            profit_growth = COALESCE(EXCLUDED.profit_growth, company_data.profit_growth),
            face_value = COALESCE(EXCLUDED.face_value, company_data.face_value),
            book_value = COALESCE(EXCLUDED.book_value, company_data.book_value),
            high_low = COALESCE(EXCLUDED.high_low, company_data.high_low),
            intrinsic_value = COALESCE(EXCLUDED.intrinsic_value, company_data.intrinsic_value),
            pledged_pct = COALESCE(EXCLUDED.pledged_pct, company_data.pledged_pct),
            promoter_holding = COALESCE(EXCLUDED.promoter_holding, company_data.promoter_holding),
            opm = COALESCE(EXCLUDED.opm, company_data.opm),
            about = COALESCE(EXCLUDED.about, company_data.about),
            pros = COALESCE(EXCLUDED.pros, company_data.pros),
            cons = COALESCE(EXCLUDED.cons, company_data.cons),
            profit_loss = COALESCE(EXCLUDED.profit_loss, company_data.profit_loss),
            balance_sheet = COALESCE(EXCLUDED.balance_sheet, company_data.balance_sheet),
            cash_flow = COALESCE(EXCLUDED.cash_flow, company_data.cash_flow),
            quarterly_results = COALESCE(EXCLUDED.quarterly_results, company_data.quarterly_results),
            shareholding = COALESCE(EXCLUDED.shareholding, company_data.shareholding),
            ratios = COALESCE(EXCLUDED.ratios, company_data.ratios),
            news = COALESCE(EXCLUDED.news, company_data.news),
            rag_content = COALESCE(EXCLUDED.rag_content, company_data.rag_content),
            data_quality = COALESCE(EXCLUDED.data_quality, company_data.data_quality),
            data_completeness = COALESCE(EXCLUDED.data_completeness, company_data.data_completeness),
            scraped_at = NOW(),
            updated_at = NOW()
        RETURNING id
        """,
        company_name,
        company_info.get("symbol"),
        company_info.get("sector"),
        company_info.get("industry"),
        screener_url,
        fundamentals.get("market_cap"),
        fundamentals.get("current_price"),
        fundamentals.get("pe") or fundamentals.get("stock_pe"),
        fundamentals.get("stock_pe"),
        fundamentals.get("industry_pe"),
        fundamentals.get("pb"),
        fundamentals.get("roce"),
        fundamentals.get("roe"),
        fundamentals.get("debt_to_equity"),
        fundamentals.get("eps"),
        fundamentals.get("dividend_yield"),
        fundamentals.get("sales_growth"),
        fundamentals.get("profit_growth"),
        fundamentals.get("face_value"),
        fundamentals.get("book_value"),
        fundamentals.get("high_low"),
        fundamentals.get("intrinsic_value"),
        fundamentals.get("pledged_pct"),
        fundamentals.get("promoter_holding"),
        fundamentals.get("opm"),
        insights.get("about", ""),
        insights.get("pros", []),
        insights.get("cons", []),
        json.dumps(financials.get("profit_loss", {})),
        json.dumps(financials.get("balance_sheet", {})),
        json.dumps(financials.get("cash_flow", {})),
        json.dumps(financials.get("quarterly", {})),
        json.dumps(shareholding or {}),
        json.dumps(ratios.get("ratios_data", {})),
        json.dumps(news_items),
        rag_content,
        data_quality.get("grade", "pending"),
        data_quality.get("completeness", 0),
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
    rows = await conn.fetch("SELECT id, name, screener_url, symbol FROM company_data ORDER BY id")
    return [dict(r) for r in rows]


async def get_company_count(conn) -> int:
    row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM company_data")
    return row["cnt"]
