# Stock Data Pipeline

Production-oriented financial data infrastructure for collecting, cleaning, and structuring Indian market data for analytics, machine learning, and AI applications.

## Problem

Financial applications often fail before the model is involved: source data is inconsistent, schemas change, historical data is fragmented, and downstream systems need the same normalized entities repeatedly.

This project focuses on building a reusable data layer between external market sources and downstream analytics or AI systems.

## Architecture

```
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
```

## Current Capabilities

- automated collection of Indian company and financial data
- normalization into structured relational tables
- PostgreSQL persistence
- processing with Python and Pandas
- modular collectors that can be extended with additional sources
- data shaped for downstream ML, analytics, and AI applications

## Financial Data

The current schema is centered around:

- company master data
- financial statements
- financial ratios
- historical prices
- extensible data-source integrations

## Tech Stack

- **Python**
- **PostgreSQL**
- **Pandas**
- **Requests / BeautifulSoup**
- **Playwright**

## Quickstart

```bash
git clone https://github.com/YuganshThacker/Stock-Data-Pipeline.git
cd Stock-Data-Pipeline

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# configure your own PostgreSQL connection
export DATABASE_URL="postgresql://user:password@localhost:5432/stockdb"

python main.py
```

## Design Priorities

### Modular collectors

Source-specific collection logic is separated from downstream cleaning and persistence so new vendors or exchanges can be added without redesigning the whole pipeline.

### Structured storage

PostgreSQL provides a stable relational layer for analytics, feature generation, and API consumers.

### AI-ready downstream use

A normalized financial dataset can support:

- ML feature engineering
- financial research systems
- retrieval pipelines
- market analytics
- dashboarding
- model evaluation

## Roadmap

- FastAPI access layer
- stronger data versioning
- ML feature-store integration
- distributed collection
- additional research/news sources

## Engineering Takeaway

This repository represents the data-engineering side of AI systems:

**collect → normalize → persist → expose → build intelligence on top**

For AI products, the quality and reliability of this layer often matter as much as the model itself.

## License

MIT
