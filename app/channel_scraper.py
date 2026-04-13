"""
Channel Scraper Module
Fetches stream URLs from nur.mpingotv.com and stores them in MongoDB
"""

import asyncio
import logging
import re
import httpx
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

logger = logging.getLogger("channel-scraper")


class ChannelScraper:
    """Scrapes channel stream URLs from nur.mpingotv.com"""

    # API Configuration
    BASE_URL = "https://nur.mpingotv.com/v1/player.php?channel="
    DEFAULT_HEADERS = {
        "X-Requested-With": "com.nurhd.tv",
        "Authorization": "Bearer ee6fed1becd8ed3db7aacfa48685600766d01d09a7d2478422b93c8a02b3",
        "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    }

    # How long we consider a scraped URL "fresh" in the DB.
    # Set conservatively below the real CDN token lifetime (~8h) so we
    # always refresh well before the token actually dies.
    TOKEN_EXPIRY_HOURS = 5

    def __init__(self, db, custom_headers: Optional[Dict[str, str]] = None):
        """
        Initialize the scraper with MongoDB database connection.

        Args:
            db: Motor AsyncIOMotorDatabase instance
            custom_headers: Optional custom headers to override defaults
        """
        self.db = db
        self.channels_col = db.channels_streams
        self.logs_col = db.scraper_logs
        self.headers = {**self.DEFAULT_HEADERS, **(custom_headers or {})}

    async def scrape_channels(self, max_channels: int = 100) -> Dict[str, Any]:
        """
        Scrape all available channels from nur.mpingotv.com.

        Args:
            max_channels: Maximum number of channels to scan (default 100)

        Returns:
            Dictionary with scrape results:
            {
                "success": bool,
                "channels_found": int,
                "channels_updated": int,
                "channels_failed": int,
                "error": Optional[str],
                "channels": List[Dict]
            }
        """
        logger.info(f"🔄 Starting channel scrape (scanning channels 1-{max_channels})...")

        run_started_at = datetime.utcnow()
        channels_found = []
        channels_updated = 0
        channels_failed = 0
        errors = []

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                for channel_id in range(1, max_channels + 1):
                    try:
                        url = f"{self.BASE_URL}{channel_id}"
                        response = await client.get(url, headers=self.headers)

                        if response.status_code != 200:
                            logger.warning(f"⚠️ Channel {channel_id}: HTTP {response.status_code}")
                            channels_failed += 1
                            continue

                        html = response.text

                        # Extract stream name from H1 tag
                        name_match = re.search(
                            r'<h1 id="streamName"[^>]*>(.*?)</h1>',
                            html
                        )

                        # Extract stream URL from script
                        url_match = re.search(
                            r'var streamUrl = "(.*?)";',
                            html
                        )

                        if name_match and url_match:
                            name = name_match.group(1).strip()
                            stream_url = url_match.group(1).strip()

                            if stream_url and ("mpd" in stream_url.lower() or "m3u8" in stream_url.lower()):
                                logger.info(f"✅ Found Channel {channel_id}: {name}")

                                # Calculate expiration time
                                url_expires_at = datetime.utcnow() + timedelta(hours=self.TOKEN_EXPIRY_HOURS)

                                channel_data = {
                                    "channelId": channel_id,
                                    "name": name,
                                    "streamUrl": stream_url,
                                    "urlExpiresAt": url_expires_at,
                                    "lastScrapedAt": datetime.utcnow(),
                                    "status": "active",
                                    "updatedAt": datetime.utcnow()
                                }

                                # Upsert channel into database
                                result = await self.channels_col.update_one(
                                    {"channelId": channel_id},
                                    {"$set": channel_data},
                                    upsert=True
                                )

                                if result.upserted_id or result.modified_count > 0:
                                    channels_updated += 1

                                channels_found.append(channel_data)
                            else:
                                logger.debug(f"⚠️ Channel {channel_id}: No valid stream URL")
                                channels_failed += 1
                        else:
                            logger.debug(f"⚠️ Channel {channel_id}: Could not extract name or URL")
                            channels_failed += 1

                    except asyncio.TimeoutError:
                        logger.error(f"❌ Channel {channel_id}: Request timeout")
                        channels_failed += 1
                    except Exception as e:
                        logger.error(f"❌ Channel {channel_id}: {str(e)}")
                        channels_failed += 1
                        errors.append(f"Channel {channel_id}: {str(e)}")

                    # Small delay to be polite to the server
                    await asyncio.sleep(0.1)

        except Exception as e:
            logger.error(f"💥 Scraper crashed: {str(e)}")
            error_msg = f"Scraper error: {str(e)}"
            errors.append(error_msg)

            # Log the failure
            await self._log_scraper_run(
                run_started_at,
                0,
                0,
                0,
                "failed",
                error_msg
            )

            return {
                "success": False,
                "channels_found": 0,
                "channels_updated": 0,
                "channels_failed": 0,
                "error": error_msg,
                "channels": []
            }

        # Determine overall status
        status = "success" if channels_failed == 0 else ("partial" if channels_found > 0 else "failed")

        # Log the scraper run
        await self._log_scraper_run(
            run_started_at,
            len(channels_found),
            channels_updated,
            channels_failed,
            status,
            "; ".join(errors) if errors else None
        )

        logger.info(
            f"✅ Scrape complete: {len(channels_found)} found, "
            f"{channels_updated} updated, {channels_failed} failed"
        )

        return {
            "success": status == "success",
            "channels_found": len(channels_found),
            "channels_updated": channels_updated,
            "channels_failed": channels_failed,
            "error": "; ".join(errors) if errors else None,
            "channels": channels_found
        }

    async def scrape_single_channel(self, channel_id: int) -> Optional[Dict[str, Any]]:
        """
        Scrape a single channel by its numeric ID and update the DB immediately.
        Used for on-demand refresh when a URL is found to be expired at playback time.

        Returns the updated channel_data dict, or None if scraping failed.
        """
        logger.info(f"🔄 On-demand scrape for channel {channel_id}...")
        url = f"{self.BASE_URL}{channel_id}"

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.get(url, headers=self.headers)

            if response.status_code != 200:
                logger.warning(f"⚠️ On-demand scrape channel {channel_id}: HTTP {response.status_code}")
                return None

            html = response.text

            name_match = re.search(r'<h1 id="streamName"[^>]*>(.*?)</h1>', html)
            url_match = re.search(r'var streamUrl = "(.*?)";', html)

            if not (name_match and url_match):
                logger.warning(f"⚠️ On-demand scrape channel {channel_id}: could not extract data from page")
                return None

            name = name_match.group(1).strip()
            stream_url = url_match.group(1).strip()

            if not stream_url or ("mpd" not in stream_url.lower() and "m3u8" not in stream_url.lower()):
                logger.warning(f"⚠️ On-demand scrape channel {channel_id}: no valid stream URL found")
                return None

            url_expires_at = datetime.utcnow() + timedelta(hours=self.TOKEN_EXPIRY_HOURS)
            channel_data = {
                "channelId": channel_id,
                "name": name,
                "streamUrl": stream_url,
                "urlExpiresAt": url_expires_at,
                "lastScrapedAt": datetime.utcnow(),
                "status": "active",
                "updatedAt": datetime.utcnow()
            }

            await self.channels_col.update_one(
                {"channelId": channel_id},
                {"$set": channel_data},
                upsert=True
            )

            logger.info(f"✅ On-demand scrape channel {channel_id} ({name}): fresh URL saved, expires at {url_expires_at.isoformat()}")
            return channel_data

        except Exception as e:
            logger.error(f"❌ On-demand scrape channel {channel_id} failed: {e}")
            return None

    async def get_channel_by_id(self, channel_id: int) -> Optional[Dict[str, Any]]:
        """Get a channel by its ID."""
        return await self.channels_col.find_one({"channelId": channel_id})

    async def get_all_channels(self) -> List[Dict[str, Any]]:
        """Get all channels."""
        cursor = self.channels_col.find({}).sort("channelId", 1)
        return await cursor.to_list(None)

    async def get_active_channels(self) -> List[Dict[str, Any]]:
        """Get only active channels with valid URLs."""
        cursor = self.channels_col.find({
            "status": "active",
            "urlExpiresAt": {"$gt": datetime.utcnow()}
        }).sort("channelId", 1)
        return await cursor.to_list(None)

    async def mark_channel_inactive(self, channel_id: int) -> None:
        """Mark a channel as inactive."""
        await self.channels_col.update_one(
            {"channelId": channel_id},
            {"$set": {"status": "inactive", "updatedAt": datetime.utcnow()}}
        )

    async def _log_scraper_run(
        self,
        run_started_at: datetime,
        channels_scraped: int,
        channels_updated: int,
        channels_failed: int,
        status: str,
        error_message: Optional[str] = None
    ) -> None:
        """Log a scraper run to the database."""
        log_entry = {
            "runStartedAt": run_started_at,
            "runCompletedAt": datetime.utcnow(),
            "channelsScraped": channels_scraped,
            "channelsUpdated": channels_updated,
            "channelsFailed": channels_failed,
            "status": status,
            "errorMessage": error_message,
            "createdAt": datetime.utcnow()
        }

        try:
            await self.logs_col.insert_one(log_entry)
            logger.info(f"📝 Scraper run logged: {status}")
        except Exception as e:
            logger.error(f"❌ Failed to log scraper run: {str(e)}")

    async def get_recent_logs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent scraper logs."""
        cursor = self.logs_col.find({}).sort("createdAt", -1).limit(limit)
        return await cursor.to_list(limit)

    async def get_scraper_stats(self) -> Dict[str, Any]:
        """Get scraper statistics."""
        total_channels = await self.channels_col.count_documents({})
        active_channels = await self.channels_col.count_documents({"status": "active"})

        last_log = await self.logs_col.find_one({}, sort=[("createdAt", -1)])

        # Calculate success rate from last 10 runs
        recent_logs = await self.get_recent_logs(10)
        success_count = sum(1 for log in recent_logs if log.get("status") == "success")
        success_rate = success_count / len(recent_logs) if recent_logs else 0

        # Calculate average scrape duration
        durations = []
        for log in recent_logs:
            if log.get("runCompletedAt") and log.get("runStartedAt"):
                duration = (log["runCompletedAt"] - log["runStartedAt"]).total_seconds()
                durations.append(duration)

        avg_duration = sum(durations) / len(durations) if durations else 0

        return {
            "totalChannels": total_channels,
            "activeChannels": active_channels,
            "lastScraperRun": last_log.get("createdAt").isoformat() if last_log else None,
            "lastScraperStatus": last_log.get("status") if last_log else None,
            "successRate": round(success_rate, 2),
            "averageScrapeDuration": round(avg_duration, 2),
            "nextScheduledRun": (
                (last_log.get("createdAt") + timedelta(hours=self.TOKEN_EXPIRY_HOURS)).isoformat()
                if last_log else None
            )
        }
