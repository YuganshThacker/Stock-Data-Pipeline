"""Contract tests for the read-only financial-data MCP server.

These run against the in-memory demo fixture, so they need no database and
prove the MCP surface itself: the tools, their bounds, and the read-only schema.
"""
import json

import pytest
from mcp import Client

from server import mcp


@pytest.mark.anyio
async def test_exposes_only_the_fixed_read_only_tools() -> None:
    async with Client(mcp) as client:
        tools = {tool.name for tool in (await client.list_tools()).tools}
        assert tools == {"search_companies", "get_company_fundamentals", "data_health"}


@pytest.mark.anyio
async def test_search_is_bounded() -> None:
    async with Client(mcp) as client:
        result = await client.call_tool("search_companies", {"query": "example", "limit": 1})
        assert not result.is_error
        assert len(result.structured_content["result"]) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("limit", [0, 26, 1000])
async def test_search_rejects_out_of_bounds_limits(limit: int) -> None:
    async with Client(mcp) as client:
        result = await client.call_tool("search_companies", {"query": "example", "limit": limit})
        assert result.is_error


@pytest.mark.anyio
async def test_search_rejects_an_empty_query() -> None:
    async with Client(mcp) as client:
        result = await client.call_tool("search_companies", {"query": "   "})
        assert result.is_error


@pytest.mark.anyio
async def test_fundamentals_are_structured() -> None:
    async with Client(mcp) as client:
        result = await client.call_tool("get_company_fundamentals", {"symbol_or_name": "EXENERGY"})
        data = result.structured_content
        assert data["symbol"] == "EXENERGY"
        assert "roe" in data


@pytest.mark.anyio
async def test_unknown_company_is_an_error_not_a_guess() -> None:
    async with Client(mcp) as client:
        result = await client.call_tool("get_company_fundamentals", {"symbol_or_name": "NOPE"})
        assert result.is_error


@pytest.mark.anyio
async def test_schema_is_read_only() -> None:
    async with Client(mcp) as client:
        resource = await client.read_resource("stock://schema")
        schema = json.loads(resource.contents[0].text)
        assert schema["read_only"] is True
        assert schema["arbitrary_sql"] is False
