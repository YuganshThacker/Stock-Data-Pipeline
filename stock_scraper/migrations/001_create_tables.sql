CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    symbol TEXT,
    sector TEXT,
    industry TEXT,
    screener_url TEXT,
    market_cap TEXT,
    current_price TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(name)
);

CREATE TABLE IF NOT EXISTS fundamentals (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    market_cap NUMERIC,
    current_price NUMERIC,
    pe NUMERIC,
    pb NUMERIC,
    roce NUMERIC,
    roe NUMERIC,
    debt_to_equity NUMERIC,
    eps NUMERIC,
    dividend_yield NUMERIC,
    sales_growth NUMERIC,
    profit_growth NUMERIC,
    face_value NUMERIC,
    book_value NUMERIC,
    high_low TEXT,
    stock_pe NUMERIC,
    industry_pe NUMERIC,
    intrinsic_value NUMERIC,
    pledged_pct NUMERIC,
    change_in_promoter_holding NUMERIC,
    raw_data JSONB DEFAULT '{}',
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(company_id)
);

CREATE TABLE IF NOT EXISTS financials (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    profit_loss JSONB DEFAULT '{}',
    balance_sheet JSONB DEFAULT '{}',
    cash_flow JSONB DEFAULT '{}',
    quarterly JSONB DEFAULT '{}',
    shareholding JSONB DEFAULT '{}',
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(company_id)
);

CREATE TABLE IF NOT EXISTS ratios (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    ratios_data JSONB DEFAULT '{}',
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(company_id)
);

CREATE TABLE IF NOT EXISTS insights (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    pros TEXT[] DEFAULT '{}',
    cons TEXT[] DEFAULT '{}',
    about TEXT DEFAULT '',
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(company_id)
);

CREATE TABLE IF NOT EXISTS news (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    news_date TEXT,
    source TEXT,
    link TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(company_id, title)
);

CREATE TABLE IF NOT EXISTS scrape_logs (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    error_message TEXT,
    duration_ms INTEGER,
    scraped_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fundamentals_company_id ON fundamentals(company_id);
CREATE INDEX IF NOT EXISTS idx_financials_company_id ON financials(company_id);
CREATE INDEX IF NOT EXISTS idx_ratios_company_id ON ratios(company_id);
CREATE INDEX IF NOT EXISTS idx_insights_company_id ON insights(company_id);
CREATE INDEX IF NOT EXISTS idx_news_company_id ON news(company_id);
CREATE INDEX IF NOT EXISTS idx_scrape_logs_company_id ON scrape_logs(company_id);
CREATE INDEX IF NOT EXISTS idx_scrape_logs_scraped_at ON scrape_logs(scraped_at);
CREATE INDEX IF NOT EXISTS idx_companies_symbol ON companies(symbol);
CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(name);
