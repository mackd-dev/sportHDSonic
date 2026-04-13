"""
Channel Scheduler Module
Manages the 5-hour refresh cycle for channel stream URLs
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from .channel_scraper import ChannelScraper

logger = logging.getLogger("channel-scheduler")


class ChannelScheduler:
    """
    Manages automatic channel scraping on a 5-hour schedule.
    Uses asyncio.Task for non-blocking background execution.
    """

    def __init__(self, db, interval_hours: int = 1):
        """
        Initialize the scheduler.

        Args:
            db: Motor AsyncIOMotorDatabase instance
            interval_hours: Interval between scrapes (default 5 hours)
        """
        self.db = db
        self.interval_hours = interval_hours
        self.interval_seconds = interval_hours * 3600
        self.scraper = ChannelScraper(db)
        self.task: Optional[asyncio.Task] = None
        self.is_running = False

    async def start(self) -> None:
        """Start the scheduler."""
        if self.is_running:
            logger.warning("⚠️ Scheduler already running")
            return

        self.is_running = True
        logger.info(f"🚀 Channel scheduler started (interval: {self.interval_hours} hours)")

        # Run initial scrape immediately
        logger.info("📡 Running initial scrape...")
        await self.scraper.scrape_channels()

        # Schedule recurring scrapes
        self.task = asyncio.create_task(self._run_scheduler())
        logger.info("✅ Channel scheduler initialized and running")

    async def stop(self) -> None:
        """Stop the scheduler."""
        if not self.is_running:
            logger.warning("⚠️ Scheduler not running")
            return

        self.is_running = False

        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

        logger.info("🛑 Channel scheduler stopped")

    async def _run_scheduler(self) -> None:
        """Main scheduler loop."""
        cycle = 0
        while self.is_running:
            try:
                next_run = datetime.utcnow() + timedelta(seconds=self.interval_seconds)
                logger.info(f"⏳ Scheduler cycle {cycle}: next scrape at {next_run.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                await asyncio.sleep(self.interval_seconds)

                if not self.is_running:
                    break

                cycle += 1
                logger.info(f"📡 Scheduler cycle {cycle}: running scheduled channel scrape...")
                result = await self.scraper.scrape_channels()

                if result["success"]:
                    logger.info(
                        f"✅ Scheduler cycle {cycle} complete: "
                        f"{result['channels_found']} channels found, "
                        f"{result['channels_updated']} updated, "
                        f"{result['channels_failed']} failed"
                    )
                else:
                    logger.error(
                        f"❌ Scheduler cycle {cycle} FAILED: {result.get('error', 'Unknown error')} "
                        f"(found={result['channels_found']}, updated={result['channels_updated']}, failed={result['channels_failed']})"
                    )

            except asyncio.CancelledError:
                logger.info("🛑 Scheduler cancelled")
                break
            except Exception as e:
                logger.error(f"💥 Scheduler unexpected error: {str(e)}")
                # Continue running despite errors
                await asyncio.sleep(60)  # Wait 1 minute before retrying

    async def run_now(self) -> dict:
        """
        Trigger an immediate scrape (useful for testing/admin).

        Returns:
            Scrape result dictionary
        """
        logger.info("🔄 Manual scrape triggered")
        return await self.scraper.scrape_channels()

    async def get_status(self) -> dict:
        """Get current scheduler status."""
        return {
            "is_running": self.is_running,
            "interval_hours": self.interval_hours,
            "next_run_in_seconds": self.interval_seconds if self.is_running else None,
            "stats": await self.scraper.get_scraper_stats()
        }
