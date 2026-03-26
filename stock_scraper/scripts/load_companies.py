import asyncio
import csv
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from stock_scraper.app.db.database import get_pool, close_pool
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
    batch_size = 500

    async with pool.acquire() as conn:
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i + batch_size]

            records = [(name,) for name in batch]

            try:
                result = await conn.executemany(
                    """
                    INSERT INTO companies (name)
                    VALUES ($1)
                    ON CONFLICT (name) DO UPDATE SET
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
                            INSERT INTO companies (name)
                            VALUES ($1)
                            ON CONFLICT (name) DO UPDATE SET
                                updated_at = NOW()
                            """,
                            name,
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
