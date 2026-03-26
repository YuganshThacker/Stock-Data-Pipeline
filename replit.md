# Workspace

## Overview

pnpm workspace monorepo using TypeScript. Each package manages its own dependencies.

## Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5
- **Database**: PostgreSQL + Drizzle ORM
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Build**: esbuild (CJS bundle)

## Structure

```text
artifacts-monorepo/
├── artifacts/              # Deployable applications
│   └── api-server/         # Express API server
├── lib/                    # Shared libraries
│   ├── api-spec/           # OpenAPI spec + Orval codegen config
│   ├── api-client-react/   # Generated React Query hooks
│   ├── api-zod/            # Generated Zod schemas from OpenAPI
│   └── db/                 # Drizzle ORM schema + DB connection
├── scripts/                # Utility scripts (single workspace package)
│   └── src/                # Individual .ts scripts, run via `pnpm --filter @workspace/scripts run <script>`
├── pnpm-workspace.yaml     # pnpm workspace (artifacts/*, lib/*, lib/integrations/*, scripts)
├── tsconfig.base.json      # Shared TS options (composite, bundler resolution, es2022)
├── tsconfig.json           # Root TS project references
└── package.json            # Root package with hoisted devDeps
```

## TypeScript & Composite Projects

Every package extends `tsconfig.base.json` which sets `composite: true`. The root `tsconfig.json` lists all packages as project references. This means:

- **Always typecheck from the root** — run `pnpm run typecheck` (which runs `tsc --build --emitDeclarationOnly`). This builds the full dependency graph so that cross-package imports resolve correctly. Running `tsc` inside a single package will fail if its dependencies haven't been built yet.
- **`emitDeclarationOnly`** — we only emit `.d.ts` files during typecheck; actual JS bundling is handled by esbuild/tsx/vite...etc, not `tsc`.
- **Project references** — when package A depends on package B, A's `tsconfig.json` must list B in its `references` array. `tsc --build` uses this to determine build order and skip up-to-date packages.

## Root Scripts

- `pnpm run build` — runs `typecheck` first, then recursively runs `build` in all packages that define it
- `pnpm run typecheck` — runs `tsc --build --emitDeclarationOnly` using project references

## Packages

### `artifacts/api-server` (`@workspace/api-server`)

Express 5 API server. Routes live in `src/routes/` and use `@workspace/api-zod` for request and response validation and `@workspace/db` for persistence.

- Entry: `src/index.ts` — reads `PORT`, starts Express
- App setup: `src/app.ts` — mounts CORS, JSON/urlencoded parsing, routes at `/api`
- Routes: `src/routes/index.ts` mounts sub-routers; `src/routes/health.ts` exposes `GET /health` (full path: `/api/health`)
- Depends on: `@workspace/db`, `@workspace/api-zod`
- `pnpm --filter @workspace/api-server run dev` — run the dev server
- `pnpm --filter @workspace/api-server run build` — production esbuild bundle (`dist/index.cjs`)
- Build bundles an allowlist of deps (express, cors, pg, drizzle-orm, zod, etc.) and externalizes the rest

### `lib/db` (`@workspace/db`)

Database layer using Drizzle ORM with PostgreSQL. Exports a Drizzle client instance and schema models.

- `src/index.ts` — creates a `Pool` + Drizzle instance, exports schema
- `src/schema/index.ts` — barrel re-export of all models
- `src/schema/<modelname>.ts` — table definitions with `drizzle-zod` insert schemas (no models definitions exist right now)
- `drizzle.config.ts` — Drizzle Kit config (requires `DATABASE_URL`, automatically provided by Replit)
- Exports: `.` (pool, db, schema), `./schema` (schema only)

Production migrations are handled by Replit when publishing. In development, we just use `pnpm --filter @workspace/db run push`, and we fallback to `pnpm --filter @workspace/db run push-force`.

### `lib/api-spec` (`@workspace/api-spec`)

Owns the OpenAPI 3.1 spec (`openapi.yaml`) and the Orval config (`orval.config.ts`). Running codegen produces output into two sibling packages:

1. `lib/api-client-react/src/generated/` — React Query hooks + fetch client
2. `lib/api-zod/src/generated/` — Zod schemas

Run codegen: `pnpm --filter @workspace/api-spec run codegen`

### `lib/api-zod` (`@workspace/api-zod`)

Generated Zod schemas from the OpenAPI spec (e.g. `HealthCheckResponse`). Used by `api-server` for response validation.

### `lib/api-client-react` (`@workspace/api-client-react`)

Generated React Query hooks and fetch client from the OpenAPI spec (e.g. `useHealthCheck`, `healthCheck`).

### `scripts` (`@workspace/scripts`)

Utility scripts package. Each script is a `.ts` file in `src/` with a corresponding npm script in `package.json`. Run scripts via `pnpm --filter @workspace/scripts run <script>`. Scripts can import any workspace package (e.g., `@workspace/db`) by adding it as a dependency in `scripts/package.json`.

## Stock Scraper (Python)

### Overview

Indian stock market data pipeline that scrapes company data from Screener.in for 4,492 NSE & BSE companies, cleans/normalizes it, and stores it in PostgreSQL. Includes a FastAPI query API and scheduler for daily/weekly/monthly updates.

### Structure

```text
stock_scraper/
├── app/
│   ├── api/main.py          # FastAPI query API (port 8001, root_path=/stock-api)
│   ├── config.py             # Settings (pydantic-settings, reads .env + env vars)
│   ├── db/database.py        # asyncpg connection pool, UPSERT operations for all 7 tables
│   ├── pipeline/
│   │   ├── worker.py          # Per-company scrape logic (resolve URL → fetch → parse → clean → store)
│   │   ├── scheduler.py       # Batch orchestration (full/retry/unscraped modes)
│   │   └── queue.py           # Company queue from DB with offset/limit
│   ├── scraper/
│   │   ├── screener_scraper.py  # HTTP client with rate limiting, search API URL resolver
│   │   ├── parser.py            # BeautifulSoup HTML parser for Screener.in pages
│   │   └── cleaner.py           # Data normalization (numbers, percentages, dates)
│   └── utils/
│       ├── logger.py           # JSON structured logging
│       └── retry.py            # Exponential backoff decorator
├── scripts/
│   ├── run_scraper.py          # CLI entry point (--mode full/retry/unscraped --limit N --offset N)
│   └── load_companies.py       # CSV bulk loader for companies table
└── __main__.py                 # python -m stock_scraper entry point
```

### Database Tables (PostgreSQL)

- `companies` — 4,492 NSE/BSE companies (name, symbol, sector, industry, market_cap, screener_url)
- `fundamentals` — PE, PB, ROCE, ROE, market cap, dividend yield, etc.
- `financials` — P&L, balance sheet, cash flow, shareholding (JSONB)
- `ratios` — Historical ratio data (JSONB)
- `insights` — Pros, cons, company about text
- `news` — Company news articles
- `scrape_logs` — Scrape attempt tracking (status, duration, errors)

### Running the Scraper

```bash
# Full scrape (all companies)
python3 stock_scraper/scripts/run_scraper.py --mode full --limit 100

# Retry failed companies
python3 stock_scraper/scripts/run_scraper.py --mode retry

# Scrape only unscraped companies
python3 stock_scraper/scripts/run_scraper.py --mode unscraped

# Module entry point
python3 -m stock_scraper --mode full --limit 50
```

### Running the API

```bash
python3 -m stock_scraper.app.api.main
# API available at http://localhost:8001/stock-api/
```

### API Endpoints

- `GET /stock-api/health` — Health check
- `GET /stock-api/companies` — List companies (search, sector filter, pagination)
- `GET /stock-api/companies/{id}` — Full company detail with fundamentals, financials, ratios, insights, news
- `GET /stock-api/companies/{id}/news` — Company news (paginated)
- `GET /stock-api/scrape-status` — Scraping progress summary
- `GET /stock-api/sectors` — List sectors with company counts

### Key Design Decisions

- URL resolution uses Screener.in search API (`/api/company/search/?q=`) to map company names → correct URL slugs (ticker symbols like TCS, INFY, RELIANCE)
- Rate limiting: 3 req/sec max with semaphore-based concurrency (5 concurrent), 8 rotating User-Agent strings
- All DB writes use UPSERT (ON CONFLICT DO UPDATE) for idempotent re-scraping
- asyncpg connection pool: min=5, max=20
- Exponential backoff retry with configurable max_retries (default 3)
- Resolved URLs are persisted back to companies.screener_url so subsequent scrapes skip the search step
