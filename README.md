# Stock Data Pipeline

Production-oriented financial data infrastructure for collecting, cleaning, structuring, and exposing Indian market data for analytics, machine learning, and AI applications.

## Why This Project Exists

Financial AI systems depend on reliable data: source formats change, historical data is fragmented, and downstream systems need stable schemas. This project focuses on the data layer between external sources and AI or analytics applications.

## Architecture

NSE / BSE / Financial Sources
            ↓
      Collection Layer
            ↓
   Cleaning & Normalization
            ↓
       PostgreSQL
            ↓
 ┌──────────┼──────────┐
 ↓          ↓          ↓
Analytics   ML / AI    APIs

## Current Capabilities

- automated collection of Indian company and financial data
- normalization into structured relational data
- PostgreSQL persistence
- Python/Pandas processing
- modular source-specific collectors
- data shaped for analytics, ML, retrieval, and AI systems
- a read-only MCP server exposing safe financial-data tools to LLM clients

## MCP Integration

The mcp/ directory contains a working Model Context Protocol server built around the financial data model.

LLM Client → MCP → Typed, bounded tools → PostgreSQL / demo fixture → Structured financial facts

The MCP surface includes company search, selected fundamentals, data-health information, a schema resource, and a research prompt. Arbitrary SQL is deliberately not exposed.

See mcp/README.md.

## Data Layer

The Python scraper uses PostgreSQL-compatible storage and environment-based configuration. The schema includes company data, financials, fundamentals, ratios, insights, news, and scrape logs.

## Tech Stack

- Python
- PostgreSQL
- Pandas
- Requests / BeautifulSoup
- Playwright
- asyncpg
- MCP Python SDK

## Run the Python Scraper

git clone https://github.com/YuganshThacker/Stock-Data-Pipeline.git
cd Stock-Data-Pipeline
python -m venv .venv
source .venv/bin/activate
pip install -r stock_scraper/requirements.txt
cp stock_scraper/.env.example stock_scraper/.env
# configure DATABASE_URL and other local settings
python -m stock_scraper

## Run the MCP Server

cd mcp
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# Demo mode: no private database required
python server.py

For real data, set DATABASE_URL before starting the server.

## Engineering Priorities

collect → normalize → persist → expose → build intelligence

The repository intentionally separates source ingestion from downstream consumers. The MCP layer is a thin, read-only capability boundary rather than a second copy of the data pipeline.
