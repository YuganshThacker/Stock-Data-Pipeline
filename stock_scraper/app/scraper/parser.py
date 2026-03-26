from bs4 import BeautifulSoup, Tag
from typing import Dict, List, Any, Optional
from stock_scraper.app.utils.logger import get_logger

logger = get_logger("parser")


def parse_company_page(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    result = {
        "company_info": _parse_company_info(soup),
        "fundamentals": _parse_fundamentals(soup),
        "financials": _parse_financials(soup),
        "ratios": _parse_ratios(soup),
        "insights": _parse_insights(soup),
        "news": _parse_news(soup),
        "shareholding": _parse_shareholding(soup),
    }
    return result


def _parse_company_info(soup: BeautifulSoup) -> Dict[str, Any]:
    info: Dict[str, Any] = {}
    name_tag = soup.find("h1")
    if name_tag:
        info["name"] = name_tag.get_text(strip=True)

    company_info_div = soup.find("div", class_="company-info")
    if company_info_div:
        links = company_info_div.find_all("a")
        for link in links:
            href = link.get("href", "")
            text = link.get_text(strip=True)
            if "/sector/" in href or "/industry/" in href:
                if "sector" not in info:
                    info["sector"] = text
                else:
                    info["industry"] = text

    bse_nse = soup.find("div", class_="company-links")
    if bse_nse:
        text = bse_nse.get_text(strip=True)
        if "BSE:" in text:
            parts = text.split("BSE:")
            if len(parts) > 1:
                bse_code = parts[1].split()[0] if parts[1].split() else ""
                info["bse_code"] = bse_code.strip()
        if "NSE:" in text:
            parts = text.split("NSE:")
            if len(parts) > 1:
                nse_code = parts[1].split()[0] if parts[1].split() else ""
                info["nse_code"] = nse_code.strip()

    symbol_meta = soup.find("meta", attrs={"name": "twitter:title"})
    if symbol_meta:
        content = symbol_meta.get("content", "")
        if content:
            symbol_part = content.split("-")[0].strip() if "-" in content else content.strip()
            info["symbol"] = symbol_part

    return info


def _parse_fundamentals(soup: BeautifulSoup) -> Dict[str, Any]:
    fundamentals: Dict[str, Any] = {}

    top_ratios = soup.find("div", id="top-ratios")
    if not top_ratios:
        top_ratios = soup.find("ul", id="top-ratios")
    if not top_ratios:
        top_ratios = soup.find("div", class_="company-ratios")

    if top_ratios:
        li_items = top_ratios.find_all("li")
        for li in li_items:
            name_span = li.find("span", class_="name")
            value_span = li.find("span", class_="number") or li.find("span", class_="value")
            if name_span and value_span:
                key = name_span.get_text(strip=True)
                value = value_span.get_text(strip=True)
                fundamentals[key] = value

        spans = top_ratios.find_all("span")
        i = 0
        while i < len(spans) - 1:
            span = spans[i]
            cls = span.get("class", [])
            if "name" in cls:
                key = span.get_text(strip=True)
                next_span = spans[i + 1]
                next_cls = next_span.get("class", [])
                if "number" in next_cls or "value" in next_cls or "nowrap" in next_cls:
                    value = next_span.get_text(strip=True)
                    fundamentals[key] = value
                    i += 2
                    continue
            i += 1

    ratios_list = soup.find_all("li", class_="flex")
    for li in ratios_list:
        name_el = li.find("span", class_="name")
        number_el = li.find("span", class_="number") or li.find("span", class_="nowrap")
        if name_el and number_el:
            key = name_el.get_text(strip=True)
            val = number_el.get_text(strip=True)
            if key and val and key not in fundamentals:
                fundamentals[key] = val

    return fundamentals


def _parse_table(section) -> Dict[str, Any]:
    if not section:
        return {}

    table = section.find("table") if not isinstance(section, Tag) or section.name != "table" else section
    if not table:
        return {}

    result = {}
    thead = table.find("thead")
    headers = []
    if thead:
        ths = thead.find_all("th")
        headers = [th.get_text(strip=True) for th in ths]

    tbody = table.find("tbody")
    if tbody:
        rows = tbody.find_all("tr")
    else:
        rows = table.find_all("tr")
        if rows and thead:
            rows = rows[1:]

    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        row_name = cells[0].get_text(strip=True)
        if not row_name:
            continue
        values = {}
        for idx, cell in enumerate(cells[1:], 1):
            header_key = headers[idx] if idx < len(headers) else f"col_{idx}"
            values[header_key] = cell.get_text(strip=True)
        result[row_name] = values

    return result


def _parse_financials(soup: BeautifulSoup) -> Dict[str, Any]:
    financials: Dict[str, Any] = {}

    section_ids = {
        "profit_loss": "profit-loss",
        "balance_sheet": "balance-sheet",
        "cash_flow": "cash-flow",
        "quarterly": "quarters",
    }

    for key, section_id in section_ids.items():
        section = soup.find("section", id=section_id)
        if not section:
            section = soup.find("div", id=section_id)
        if section:
            financials[key] = _parse_table(section)
        else:
            financials[key] = {}

    return financials


def _parse_ratios(soup: BeautifulSoup) -> Dict[str, Any]:
    section = soup.find("section", id="ratios")
    if not section:
        section = soup.find("div", id="ratios")
    if section:
        return {"ratios_data": _parse_table(section)}
    return {"ratios_data": {}}


def _parse_insights(soup: BeautifulSoup) -> Dict[str, Any]:
    insights: Dict[str, Any] = {"pros": [], "cons": [], "about": ""}

    pros_section = soup.find("div", class_="pros")
    if not pros_section:
        for s in soup.find_all("section"):
            header = s.find(["h2", "h3"])
            if header and "pros" in header.get_text(strip=True).lower():
                pros_section = s
                break

    if pros_section:
        items = pros_section.find_all("li")
        insights["pros"] = [li.get_text(strip=True) for li in items if li.get_text(strip=True)]

    cons_section = soup.find("div", class_="cons")
    if not cons_section:
        for s in soup.find_all("section"):
            header = s.find(["h2", "h3"])
            if header and "cons" in header.get_text(strip=True).lower():
                cons_section = s
                break

    if cons_section:
        items = cons_section.find_all("li")
        insights["cons"] = [li.get_text(strip=True) for li in items if li.get_text(strip=True)]

    about_section = soup.find("div", class_="about")
    if not about_section:
        about_section = soup.find("div", id="company-profile")
    if about_section:
        paragraphs = about_section.find_all("p")
        if paragraphs:
            insights["about"] = " ".join(p.get_text(strip=True) for p in paragraphs)
        else:
            insights["about"] = about_section.get_text(strip=True)

    return insights


def _parse_news(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    news_items = []

    news_section = soup.find("section", id="news")
    if not news_section:
        news_section = soup.find("div", class_="news")

    if news_section:
        items = news_section.find_all("li")
        for item in items:
            link_tag = item.find("a")
            date_tag = item.find("span", class_="date") or item.find("time")

            news_entry: Dict[str, Any] = {}
            if link_tag:
                news_entry["title"] = link_tag.get_text(strip=True)
                news_entry["link"] = link_tag.get("href", "")
            else:
                text = item.get_text(strip=True)
                if text:
                    news_entry["title"] = text

            if date_tag:
                news_entry["date"] = date_tag.get_text(strip=True)

            source_tag = item.find("span", class_="source")
            if source_tag:
                news_entry["source"] = source_tag.get_text(strip=True)

            if news_entry.get("title"):
                news_items.append(news_entry)

    return news_items


def _parse_shareholding(soup: BeautifulSoup) -> Dict[str, Any]:
    section = soup.find("section", id="shareholding")
    if not section:
        section = soup.find("div", id="shareholding")
    if section:
        return _parse_table(section)
    return {}
