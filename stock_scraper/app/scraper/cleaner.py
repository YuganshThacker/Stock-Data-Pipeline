import re
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from stock_scraper.app.utils.logger import get_logger

logger = get_logger("cleaner")

FUNDAMENTAL_KEY_MAP = {
    "Market Cap": "market_cap",
    "Market Cap ₹": "market_cap",
    "Current Price": "current_price",
    "Current Price ₹": "current_price",
    "Stock P/E": "stock_pe",
    "P/E": "pe",
    "PE": "pe",
    "Book Value": "book_value",
    "Price to book value": "pb",
    "P/B": "pb",
    "PB": "pb",
    "ROCE": "roce",
    "ROCE %": "roce",
    "ROE": "roe",
    "ROE %": "roe",
    "Debt to equity": "debt_to_equity",
    "Debt to Equity": "debt_to_equity",
    "EPS": "eps",
    "Dividend Yield": "dividend_yield",
    "Div Yield %": "dividend_yield",
    "Dividend yield %": "dividend_yield",
    "Face Value": "face_value",
    "High / Low": "high_low",
    "Industry PE": "industry_pe",
    "Intrinsic Value": "intrinsic_value",
    "Pledged percentage": "pledged_pct",
    "Change in Prom Hold": "change_in_promoter_holding",
    "Promoter holding": "promoter_holding",
    "Sales Growth": "sales_growth",
    "Sales growth": "sales_growth",
    "Profit Growth": "profit_growth",
    "Profit growth": "profit_growth",
    "OPM": "opm",
    "Net Profit": "net_profit",
}


def clean_numeric(value: str) -> Optional[float]:
    if not value or value.strip() in ("", "-", "N/A", "NA", "nan", "None"):
        return None
    cleaned = value.strip()
    cleaned = cleaned.replace("₹", "").replace("Rs.", "").replace("Rs", "")
    cleaned = cleaned.replace(",", "")
    cleaned = cleaned.replace("%", "")
    cleaned = cleaned.replace("Cr.", "").replace("Cr", "")
    cleaned = cleaned.replace("Lakh", "").replace("Lac", "")
    cleaned = cleaned.strip()
    if "/" in cleaned:
        return None
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def clean_text(value: str) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value.strip())


def to_snake_case(key: str) -> str:
    key = re.sub(r"[^\w\s]", "", key)
    key = re.sub(r"\s+", "_", key.strip())
    key = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", key)
    key = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", key)
    return key.lower()


def clean_fundamentals(raw: Dict[str, str]) -> Dict[str, Any]:
    cleaned: Dict[str, Any] = {"raw_data": {}}

    for raw_key, raw_value in raw.items():
        mapped_key = FUNDAMENTAL_KEY_MAP.get(raw_key)
        cleaned["raw_data"][raw_key] = raw_value

        if mapped_key:
            if mapped_key == "high_low":
                cleaned[mapped_key] = raw_value
                cleaned[f"{mapped_key}_str"] = raw_value
            elif mapped_key in ("market_cap", "current_price"):
                numeric_val = clean_numeric(raw_value)
                cleaned[mapped_key] = numeric_val
                cleaned[f"{mapped_key}_str"] = raw_value.strip()
            else:
                cleaned[mapped_key] = clean_numeric(raw_value)
        else:
            snake_key = to_snake_case(raw_key)
            cleaned["raw_data"][snake_key] = raw_value

    return cleaned


def clean_company_info(raw: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = {}
    for key, value in raw.items():
        if isinstance(value, str):
            cleaned[key] = clean_text(value)
        else:
            cleaned[key] = value
    return cleaned


def clean_financials(raw: Dict[str, Any]) -> Dict[str, Any]:
    return raw


def clean_ratios(raw: Dict[str, Any]) -> Dict[str, Any]:
    return raw


def clean_insights(raw: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = {
        "pros": [clean_text(p) for p in raw.get("pros", []) if clean_text(p)],
        "cons": [clean_text(c) for c in raw.get("cons", []) if clean_text(c)],
        "about": clean_text(raw.get("about", "")),
    }
    return cleaned


def clean_news(raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned_items = []
    seen_titles = set()
    for item in raw:
        title = clean_text(item.get("title", ""))
        if title and title not in seen_titles:
            seen_titles.add(title)
            cleaned_items.append({
                "title": title,
                "date": clean_text(item.get("date", "")),
                "source": clean_text(item.get("source", "")),
                "link": item.get("link", ""),
            })
    return cleaned_items


def clean_all(parsed_data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "company_info": clean_company_info(parsed_data.get("company_info", {})),
        "fundamentals": clean_fundamentals(parsed_data.get("fundamentals", {})),
        "financials": clean_financials(parsed_data.get("financials", {})),
        "ratios": clean_ratios(parsed_data.get("ratios", {})),
        "insights": clean_insights(parsed_data.get("insights", {})),
        "news": clean_news(parsed_data.get("news", [])),
        "shareholding": parsed_data.get("shareholding", {}),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }
