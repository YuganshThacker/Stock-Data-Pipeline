# Indian Stock Market Data Pipeline

Production-ready data pipeline that scrapes all NSE & BSE company data from Screener.in, cleans/normalizes it, and stores it in PostgreSQL.

## Prerequisites

- Python 3.12+
- PostgreSQL database (DATABASE_URL environment variable)
- pip packages (see requirements.txt)

## Setup

```bash
# Install dependencies
pip install -r stock_scraper/requirements.txt

# Copy env template and configure
cp stock_scraper/.env.example .env
# Edit .env with your DATABASE_URL
```

## Database Schema

The database has 7 tables:
- **companies** — Company name, symbol, sector, industry, screener URL
- **fundamentals** — PE, PB, ROCE, ROE, EPS, market cap, etc.
- **financials** — P&L, balance sheet, cash flow, quarterly (JSONB)
- **ratios** — All available ratios (JSONB)
- **insights** — Pros, cons, about section
- **news** — Headlines, dates, sources
- **scrape_logs** — Scrape status, errors, duration

Tables are created automatically. The SQL schema is also available at `stock_scraper/migrations/001_create_tables.sql`.

## Usage

### 1. Load Companies from CSV

Load all 4,492 companies from the provided CSV into the database:

```bash
python stock_scraper/scripts/load_companies.py
```

### 2. Run the Scraper

Full scrape (all companies):
```bash
python stock_scraper/scripts/run_scraper.py --mode full --batch-size 50
```

Scrape with limit (for testing):
```bash
python stock_scraper/scripts/run_scraper.py --mode full --limit 10
```

Retry failed companies:
```bash
python stock_scraper/scripts/run_scraper.py --mode retry
```

Scrape only unscraped companies:
```bash
python stock_scraper/scripts/run_scraper.py --mode unscraped
```

Or use the module entry point:
```bash
python -m stock_scraper --mode full --limit 10
```

### 3. Run the API Server

```bash
python -m stock_scraper.app.api.main
```

API will be available at `http://localhost:8001/stock-api/`

### API Endpoints

- `GET /stock-api/health` — Health check
- `GET /stock-api/companies` — Paginated company list (query params: page, per_page, search, sector)
- `GET /stock-api/companies/{id}` — Full company detail with all data
- `GET /stock-api/companies/{id}/news` — Company news
- `GET /stock-api/scrape-status` — Scrape pipeline status summary
- `GET /stock-api/sectors` — List all sectors with company counts

## Configuration

All settings can be configured via environment variables or `.env` file:

| Variable | Default | Description |
|----------|---------|-------------|
| DATABASE_URL | (required) | PostgreSQL connection string |
| MAX_CONCURRENT_REQUESTS | 5 | Max parallel HTTP requests |
| RATE_LIMIT_PER_SECOND | 3.0 | Max requests per second |
| REQUEST_TIMEOUT | 30 | HTTP request timeout (seconds) |
| MAX_RETRIES | 3 | Max retry attempts per company |
| BATCH_SIZE | 50 | Companies per processing batch |
| API_PORT | 8001 | FastAPI server port |

## Scheduler

The pipeline includes an APScheduler-based scheduler for automated updates:
- **Daily at 2:00 AM** — Full scrape of all companies
- **Daily at 6:00 AM** — Retry failed scrapes

## Architecture

```
stock_scraper/
├── app/
│   ├── scraper/
│   │   ├── screener_scraper.py  # Async HTTP fetcher with rate limiting
│   │   ├── parser.py            # HTML → structured JSON parser
│   │   └── cleaner.py           # Data cleaning & normalization
│   ├── db/
│   │   └── database.py          # asyncpg connection pool & upsert functions
│   ├── pipeline/
│   │   ├── worker.py            # Async scrape workers
│   │   ├── queue.py             # Company queue management
│   │   └── scheduler.py         # APScheduler job definitions
│   ├── api/
│   │   └── main.py              # FastAPI query API
│   ├── utils/
│   │   ├── logger.py            # Structured JSON logging
│   │   └── retry.py             # Exponential backoff decorator
│   └── config.py                # Pydantic settings
├── scripts/
│   ├── load_companies.py        # CSV → database loader
│   └── run_scraper.py           # Scraper entry point
├── migrations/
│   └── 001_create_tables.sql    # Database schema
├── requirements.txt
└── .env.example
```

## Production Features

- **Rate limiting** — Semaphore-based concurrency control + per-request rate limiting
- **Retry logic** — Exponential backoff with jitter for failed requests
- **User-Agent rotation** — 8 different browser user agents
- **Idempotent writes** — All database writes use UPSERT (ON CONFLICT DO UPDATE)
- **Connection pooling** — asyncpg pool with min 5 / max 20 connections
- **Structured logging** — JSON-formatted logs with company_id, duration, status
- **Graceful error handling** — Individual company failures don't stop the pipeline
- **JSONB storage** — Flexible schema for financial data that varies by company
