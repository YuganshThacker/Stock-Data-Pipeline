import asyncio
import os

from stock_scraper.app.db.database import get_pool, close_pool
from stock_scraper.app.utils.logger import get_logger

logger = get_logger("migrations")

MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), "..", "migrations")


async def run_migrations():
    pool = await get_pool()

    migration_files = sorted(
        f for f in os.listdir(MIGRATIONS_DIR)
        if f.endswith(".sql")
    )

    if not migration_files:
        logger.info("No migration files found")
        await close_pool()
        return

    async with pool.acquire() as conn:
        for migration_file in migration_files:
            file_path = os.path.join(MIGRATIONS_DIR, migration_file)
            logger.info(f"Running migration: {migration_file}")

            with open(file_path, "r") as f:
                sql = f.read()

            await conn.execute(sql)
            logger.info(f"Migration complete: {migration_file}")

    logger.info(f"All {len(migration_files)} migrations executed successfully")
    await close_pool()


if __name__ == "__main__":
    asyncio.run(run_migrations())
