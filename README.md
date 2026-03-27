# Stock-Data-Pipeline

Production-grade pipeline to collect, clean, and structure Indian stock market data (NSE & BSE) for analytics, research, and AI applications.

## Overview

Stock Data Pipeline is a scalable data infrastructure designed to:

- Scrape financial data from multiple sources (Screener, NSE, BSE)
- Clean and normalize raw datasets
- Store structured data in PostgreSQL
- Power downstream use cases: quant research, IPO analysis, AI/ML models, financial dashboards

## Features

- Automated data collection (NSE + BSE companies)
- Fundamental data engine (P&L, Balance Sheet, Ratios)
- Structured PostgreSQL storage
- Scalable pipeline architecture
- Modular & extensible design

## Architecture
```
Data Sources (Screener, NSE, BSE)
        ↓
Scraping Engine
        ↓
Data Cleaning & Processing
        ↓
PostgreSQL Database
        ↓
APIs / ML / Analytics
```

## Quickstart

**1. Clone the repo**
```bash
git clone https://github.com/YuganshThacker/Stock-Data-Pipeline.git
cd Stock-Data-Pipeline
```

**2. Setup environment**
```bash
python -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows

pip install -r requirements.txt
```

**3. Configure environment variables**

Create a `.env` file:
```env
DATABASE_URL=postgresql://user:password@localhost:5432/stockdb
```

**4. Run the pipeline**
```bash
python main.py
```

## Data Coverage

**Available now:**
- NSE & BSE listed companies
- Financial statements: Balance Sheet, Profit & Loss, Cash Flow
- Key ratios: PE, ROE, ROCE, Debt/Equity

**Coming soon:**
- News data
- Shareholding patterns
- Insider trading data

## Database Schema

| Table | Description |
|---|---|
| `companies` | Company master data |
| `financials` | P&L, Balance Sheet, Cash Flow |
| `ratios` | Key financial ratios |
| `prices` | Historical price data |
| `news` | News data *(planned)* |

## Use Cases

- Quantitative trading strategies
- Training financial ML models
- IPO analysis platforms
- Financial dashboards
- Institutional-grade data infrastructure

## Tech Stack

- **Language:** Python
- **Database:** PostgreSQL
- **Scraping:** BeautifulSoup, Requests, Playwright
- **Data Processing:** Pandas

## Roadmap

- [ ] Real-time data ingestion
- [ ] API layer (FastAPI)
- [ ] Data versioning
- [ ] ML-ready feature store
- [ ] Distributed scraping

## Contributing

Contributions are welcome!

`fork → clone → branch → commit → PR`

## License

MIT License © 2026 Yugansh Thacker
