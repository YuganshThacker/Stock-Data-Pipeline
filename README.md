# 🚀 Stock Data Pipeline

![GitHub stars](https://img.shields.io/github/stars/YuganshThacker/Stock-Data-Pipeline?style=social)
![GitHub forks](https://img.shields.io/github/forks/YuganshThacker/Stock-Data-Pipeline?style=social)
![GitHub issues](https://img.shields.io/github/issues/YuganshThacker/Stock-Data-Pipeline)
![License](https://img.shields.io/github/license/YuganshThacker/Stock-Data-Pipeline)
![Python](https://img.shields.io/badge/python-3.10+-blue)
![Status](https://img.shields.io/badge/status-active-success)

> Production-grade pipeline to collect, clean, and structure Indian stock market data (NSE & BSE) for analytics, research, and AI applications.

---

## 🔥 Overview

**Stock Data Pipeline** is a scalable data infrastructure designed to:

- 📊 Scrape financial data from multiple sources (Screener, NSE, BSE)
- 🧹 Clean and normalize raw datasets
- 🗄️ Store structured data in PostgreSQL
- ⚡ Power downstream use cases like:
  - Quant research
  - IPO analysis
  - AI/ML models
  - Financial dashboards

---

## ✨ Features

- ⚡ Automated data collection (NSE + BSE companies)
- 🧠 Fundamental data engine (P&L, Balance Sheet, Ratios)
- 🗃️ Structured PostgreSQL storage
- 🔄 Scalable pipeline architecture
- 🧩 Modular & extensible design

---

## 🏗️ Architecture


Data Sources (Screener, NSE, BSE)
│
▼
Scraping Engine
│
▼
Data Cleaning & Processing
│
▼
PostgreSQL Database
│
▼
APIs / ML / Analytics


---

## ⚡ Quickstart

### 1. Clone the repo
```bash
git clone https://github.com/YuganshThacker/Stock-Data-Pipeline.git
cd Stock-Data-Pipeline
2. Setup environment
python -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows

pip install -r requirements.txt
3. Configure environment variables

Create a .env file:

DATABASE_URL=postgresql://user:password@localhost:5432/stockdb
4. Run the pipeline
python main.py
📊 Data Coverage
✅ NSE listed companies
✅ BSE listed companies
✅ Financial statements:
Balance Sheet
Profit & Loss
Cash Flow
✅ Key ratios:
PE, ROE, ROCE, Debt/Equity

🚧 Upcoming:

News data
Shareholding patterns
Insider trading data
🗄️ Database Schema (Simplified)
companies
financials
ratios
prices
news (planned)
🧠 Use Cases
📈 Quantitative trading strategies
🤖 Training financial ML models
🧾 IPO analysis platforms
📊 Financial dashboards
🏦 Institutional-grade data infrastructure
🛠️ Tech Stack
Python
PostgreSQL
BeautifulSoup / Requests / Playwright
Pandas
📌 Roadmap
 Real-time data ingestion
 API layer (FastAPI)
 Data versioning
 ML-ready feature store
 Distributed scraping
🤝 Contributing

Contributions are welcome!

fork → clone → branch → commit → PR
📜 License

MIT License © 2026 Yugansh Thacker
