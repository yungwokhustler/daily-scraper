import asyncio
import os
import asyncpg
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from src.logger import get_logger
from src.notification import send_error_to_telegram
from src.types import ScrapeStats

load_dotenv()
logger = get_logger(__name__)

class SupabaseDB:
    def __init__(self, database_url: Optional[str] = None, min_size: int = 1, max_size: int = 5):
        self.database_url = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable must be set.")
        self.min_size = min_size
        self.max_size = max_size
        self._pool: Optional[asyncpg.Pool] = None

    async def initialize(self) -> None:
        """Initialize the connection pool."""
        if self._pool is None:
            try:
                self._pool = await asyncpg.create_pool(
                    dsn=self.database_url,
                    statement_cache_size=0,
                    min_size=self.min_size,
                    max_size=self.max_size,
                    timeout=60,
                    command_timeout=30
                )
                logger.info("✅ Database connection pool initialized.")
            except Exception as e:
                logger.error(f"❌ Failed to initialize database pool: {e}")
                await send_error_to_telegram(f"❌ DB Pool Init Failed: {e}")
                raise

    async def close(self) -> None:
        """Gracefully close the connection pool with timeout."""
        if self._pool:
            try:

                await asyncio.wait_for(self._pool.close(), timeout=10.0)
                self._pool = None
                logger.info("🔌 Database connection pool closed.")
            except asyncio.TimeoutError:
                logger.warning("⚠️ Timeout while closing database pool. Forcing cleanup.")
                self._pool.terminate()
                self._pool = None


    async def get_all_sources(self) -> List[Dict[str, Any]]:
        """Fetch all records from the 'sources' table."""
        if self._pool is None:
            raise RuntimeError("Database pool not initialized. Call .initialize() first.")

        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch("SELECT id, channel_id, platform, channel_name FROM sources")
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Failed to fetch sources: {e}")
            await send_error_to_telegram(f"❌ DB: Failed to fetch sources: {e}")
            raise

    async def insert_log_runs_batch(self, logs: List[ScrapeStats]) -> None:
        """Insert multiple log entries into 'logs_runs' table."""
        if not logs:
            return
        if self._pool is None:
            raise RuntimeError("Database pool not initialized. Call .initialize() first.")

        try:
            async with self._pool.acquire() as conn:
                records = [
                    (
                        log.get("channel_id"),
                        log["pulled"],
                        log["kept"],
                        log.get("platform")
                    )
                    for log in logs
                ]
                await conn.executemany(
                    """
                    INSERT INTO logs_runs (channel_id, pulled, kept, platform)
                    VALUES ($1, $2, $3, $4)
                    """,
                    records
                )
            logger.info(f"✅ Successfully saved {len(logs)} log entries to logs_runs.")
        except Exception as e:
            logger.error(f"❌ Failed to insert logs: {e}")
            await send_error_to_telegram(f"❌ DB: Failed to insert logs: {e}")
            raise