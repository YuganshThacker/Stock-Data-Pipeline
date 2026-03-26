import asyncio
import csv
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from stock_scraper.app.db.database import get_pool, close_pool
from stock_scraper.app.scraper.screener_scraper import company_name_to_screener_url
from stock_scraper.app.utils.logger import get_logger

logger = get_logger("load_companies")

CSV_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "attached_assets",
    "List-Of-All-Companies_1774497861468.csv"
)


async def load_companies_from_csv(csv_path: str = CSV_PATH):
    if not os.path.exists(csv_path):
        logger.error(f"CSV file not found: {csv_path}")
        return

    companies = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if len(row) >= 2:
                name = row[1].strip()
                if name and name != "0":
                    companies.append(name)

    logger.info(f"Read {len(companies)} companies from CSV")

    pool = await get_pool()
    inserted = 0
    skipped = 0

    async with pool.acquire() as conn:
        for i in range(0, len(companies), 100):
            batch = companies[i:i + 100]
            for name in batch:
                screener_url = company_name_to_screener_url(name)
                try:
                    await conn.execute(
                        """
                        INSERT INTO companies (name, screener_url)
                        VALUES ($1, $2)
                        ON CONFLICT (name) DO UPDATE SET
                            screener_url = COALESCE(EXCLUDED.screener_url, companies.screener_url),
                            updated_at = NOW()
                        """,
                        name, screener_url,
                    )
                    inserted += 1
                except Exception as e:
                    skipped += 1
                    logger.warning(f"Failed to insert {name}: {e}")

            logger.info(f"Progress: {min(i + 100, len(companies))}/{len(companies)} processed")

    logger.info(f"Load complete: {inserted} inserted/updated, {skipped} skipped")
    await close_pool()


if __name__ == "__main__":
    asyncio.run(load_companies_from_csv())
