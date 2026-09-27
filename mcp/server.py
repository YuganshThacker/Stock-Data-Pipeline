"""Read-only MCP server for the financial data pipeline."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import asyncpg
from mcp.server.mcpserver import MCPServer

ROOT = Path(__file__).resolve().parent
DEMO_FILE = ROOT / "demo_companies.json"

mcp = MCPServer("financial-data-tools", instructions="Read-only financial-data tools. No arbitrary SQL; queries are bounded and parameterized.")

def _demo_records() -> list[dict[str, Any]]:
    return json.loads(DEMO_FILE.read_text(encoding="utf-8"))

def _using_demo_data() -> bool:
    return not bool(os.getenv("DATABASE_URL", "").strip())

async def _query_db(sql: str, *args: Any) -> list[dict[str, Any]]:
    dsn = os.getenv("DATABASE_URL", "").strip()
    if not dsn:
        return []
    conn = await asyncpg.connect(dsn=dsn, command_timeout=15)
    try:
        rows = await conn.fetch(sql, *args)
        return [dict(row) for row in rows]
    finally:
        await conn.close()

@mcp.tool()
async def search_companies(query: str, limit: int = 10) -> list[dict[str, Any]]:
    """Search companies by name or symbol with a bounded result set."""
    query = query.strip()
    if not query:
        raise ValueError("query must not be empty")
    if not 1 <= limit <= 25:
        raise ValueError("limit must be between 1 and 25")
    if _using_demo_data():
        needle = query.lower()
        return [row for row in _demo_records() if needle in row["name"].lower() or needle in row["symbol"].lower()][:limit]
    return await _query_db(
        "SELECT id, name, symbol, sector, industry FROM company_data WHERE name ILIKE '%' || $1 || '%' OR symbol ILIKE '%' || $1 || '%' ORDER BY name LIMIT $2",
        query, limit,
    )

@mcp.tool()
async def get_company_fundamentals(symbol_or_name: str) -> dict[str, Any]:
    """Return selected fundamentals for one company."""
    key = symbol_or_name.strip()
    if not key:
        raise ValueError("symbol_or_name must not be empty")
    if _using_demo_data():
        needle = key.lower()
        for row in _demo_records():
            if row["symbol"].lower() == needle or row["name"].lower() == needle:
                return row
        raise ValueError(f"company not found: {symbol_or_name}")
    rows = await _query_db(
        "SELECT name, symbol, sector, industry, market_cap, current_price, pe, pb, roce, roe, debt_to_equity, eps, dividend_yield, sales_growth, profit_growth, data_quality, data_completeness, scraped_at, updated_at FROM company_data WHERE symbol ILIKE $1 OR name ILIKE $1 ORDER BY updated_at DESC NULLS LAST LIMIT 1",
        key,
    )
    if not rows:
        raise ValueError(f"company not found: {symbol_or_name}")
    return rows[0]

@mcp.tool()
async def data_health() -> dict[str, Any]:
    """Return high-level pipeline health without exposing DB internals."""
    if _using_demo_data():
        return {"mode": "demo", "company_records": len(_demo_records()), "database_reachable": False}
    rows = await _query_db(
        "SELECT COUNT(*)::int AS company_records, COUNT(*) FILTER (WHERE scraped_at IS NOT NULL)::int AS scraped_records, MAX(scraped_at) AS latest_scrape, COUNT(*) FILTER (WHERE data_quality = 'pending')::int AS pending_quality FROM company_data",
    )
    return {"mode": "postgresql", **rows[0]}

@mcp.resource("stock://schema")
def schema_resource() -> str:
    """Describe the fixed read-only MCP surface."""
    return json.dumps({"read_only": True, "arbitrary_sql": False, "max_search_results": 25})

@mcp.prompt()
def research_company(symbol_or_name: str) -> str:
    """Create a structured research prompt for an AI client."""
    return f"Research {symbol_or_name!r} using the read-only company tools. Resolve the company, retrieve fundamentals, then separate facts from interpretation."

def main() -> None:
    mcp.run(transport="stdio")

if __name__ == "__main__":
    main()
