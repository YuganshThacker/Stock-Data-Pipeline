import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from stock_scraper.scripts.run_scraper import main
import asyncio

if __name__ == "__main__":
    asyncio.run(main())
