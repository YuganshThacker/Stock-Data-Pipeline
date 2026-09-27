# Financial Data MCP Server

A read-only Model Context Protocol server that exposes financial-data capabilities to an LLM through typed tools.

## Tools

- search_companies — bounded company resolution by name or symbol
- get_company_fundamentals — selected fundamentals for one company
- data_health — high-level pipeline health
- stock://schema — fixed read-only capability description
- research_company — structured research prompt

## Safety boundary

The server never accepts arbitrary SQL. PostgreSQL queries are parameterized, results are bounded, and the surface is read-only.

When DATABASE_URL is absent, the server uses a small demo fixture so the MCP contract can be inspected without private infrastructure.

## Run

Create a virtual environment, install requirements.txt, and run:

python server.py

The default transport is stdio, suitable for local MCP hosts such as Claude Code.

The official MCP Python SDK currently documents MCPServer as the v2 server API and recommends in-memory Client tests for server contracts. https://py.sdk.modelcontextprotocol.io/

## Production mode

Set DATABASE_URL to a PostgreSQL connection string. Only the fixed read-only tools are exposed; the database credential is never passed to the model.
