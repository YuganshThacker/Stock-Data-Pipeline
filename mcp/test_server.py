import pytest
from mcp import Client
from server import mcp

@pytest.mark.anyio
async def test_search_is_bounded() -> None:
    async with Client(mcp) as client:
        result = await client.call_tool("search_companies", {"query": "example", "limit": 1})
        assert len(result.structured_content["result"]) == 1

@pytest.mark.anyio
async def test_fundamentals_are_structured() -> None:
    async with Client(mcp) as client:
        result = await client.call_tool("get_company_fundamentals", {"symbol_or_name": "EXENERGY"})
        data = result.structured_content["result"]
        assert data["symbol"] == "EXENERGY"
        assert "roe" in data

@pytest.mark.anyio
async def test_schema_is_read_only() -> None:
    async with Client(mcp) as client:
        resource = await client.read_resource("stock://schema")
        assert "arbitrary_sql" in resource[0].text
