import asyncio
import csv
import os

from stock_scraper.app.db.database import get_pool, close_pool
from stock_scraper.app.utils.logger import get_logger

logger = get_logger("load_companies")

CSV_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "attached_assets",
    "List-Of-All-Companies_1774497861468.csv"
)


def derive_screener_slug(name: str) -> str:
    n = name.strip()
    for suffix in [" Ltd", " Limited", " Pvt", " Private"]:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    slug = n.replace("&", "and").replace("'", "").replace(".", "").replace(",", "")
    slug = slug.replace("(", "").replace(")", "").replace("/", "-")
    parts = slug.split()
    slug = "-".join(p for p in parts if p)
    slug = slug.replace("--", "-").strip("-").lower()
    return slug


def derive_screener_url(name: str) -> str:
    slug = derive_screener_slug(name)
    return f"https://www.screener.in/company/{slug}/consolidated/"


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
    batch_size = 500

    async with pool.acquire() as conn:
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i + batch_size]

            records = [
                (name, derive_screener_url(name))
                for name in batch
            ]

            try:
                await conn.executemany(
                    """
                    INSERT INTO companies (name, screener_url)
                    VALUES ($1, $2)
                    ON CONFLICT (name) DO UPDATE SET
                        screener_url = COALESCE(companies.screener_url, EXCLUDED.screener_url),
                        updated_at = NOW()
                    """,
                    records,
                )
                inserted += len(batch)
            except Exception as e:
                logger.warning(f"Batch insert failed at offset {i}, falling back to row-by-row: {e}")
                for name in batch:
                    try:
                        await conn.execute(
                            """
                            INSERT INTO companies (name, screener_url)
                            VALUES ($1, $2)
                            ON CONFLICT (name) DO UPDATE SET
                                screener_url = COALESCE(companies.screener_url, EXCLUDED.screener_url),
                                updated_at = NOW()
                            """,
                            name, derive_screener_url(name),
                        )
                        inserted += 1
                    except Exception as e2:
                        skipped += 1
                        logger.warning(f"Failed to insert {name}: {e2}")

            logger.info(f"Progress: {min(i + batch_size, len(companies))}/{len(companies)} processed")

    logger.info(f"Load complete: {inserted} inserted/updated, {skipped} skipped")
    await close_pool()


if __name__ == "__main__":
    asyncio.run(load_companies_from_csv())
