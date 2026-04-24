"""
Channel Scraper Module - UPDATED FOR NEW API FORMAT
Fetches stream URLs from nur.mpingotv.com and stores them in MongoDB
Updated: March 2026 - API now returns HTML with embedded JavaScript variables
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
        
        NEW FORMAT (March 2026): API returns HTML with embedded JavaScript variables
        - var streamUrl = "..."
        - var streamType = "..."
        - var clearKey = "..."

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

                        # NEW FORMAT: Extract stream URL from JavaScript variable
                        url_match = re.search(
                            r'var streamUrl = "([^"]+)"',
                            html
                        )

                        # NEW FORMAT: Extract stream type (mpd or hls)
                        type_match = re.search(
                            r'var streamType = "([^"]+)"',
                            html
                        )

                        # NEW FORMAT: Extract clearkey if available
                        clearkey_match = re.search(
                            r'var clearKey = "([^"]+)"',
                            html
                        )

                        # Extract channel name from page title
                        title_match = re.search(
                            r'<title>([^<]+)</title>',
                            html
                        )

                        if url_match:
                            stream_url = url_match.group(1).strip()
                            stream_type = type_match.group(1).strip() if type_match else "mpd"
                            clearkey = clearkey_match.group(1).strip() if clearkey_match else None
                            channel_name = title_match.group(1).strip() if title_match else f"Channel {channel_id}"

                            # Validate stream URL
                            if stream_url and ("mpd" in stream_url.lower() or "m3u8" in stream_url.lower()):
                                logger.info(f"✅ Found Channel {channel_id}: {channel_name} ({stream_type})")

                                # Calculate expiration time
                                url_expires_at = datetime.utcnow() + timedelta(hours=self.TOKEN_EXPIRY_HOURS)

                                channel_data = {
                                    "channelId": channel_id,
                                    "name": channel_name,
                                    "streamUrl": stream_url,
                                    "streamType": stream_type,
                                    "clearKey": clearkey,
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
                            logger.debug(f"⚠️ Channel {channel_id}: Could not extract stream URL")
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

        # Log the scrape run
        run_ended_at = datetime.utcnow()
        duration_seconds = (run_ended_at - run_started_at).total_seconds()

        log_entry = {
            "timestamp": run_started_at,
            "endedAt": run_ended_at,
            "durationSeconds": duration_seconds,
            "channelsScanned": max_channels,
            "channelsFound": len(channels_found),
            "channelsUpdated": channels_updated,
            "channelsFailed": channels_failed,
            "status": "success" if channels_found else "failed",
            "errors": errors if errors else None
        }

        try:
            await self.logs_col.insert_one(log_entry)
            logger.info(f"📝 Scrape log saved")
        except Exception as e:
            logger.error(f"Failed to save scrape log: {str(e)}")

        success = len(channels_found) > 0
        logger.info(f"✅ Scrape complete: {len(channels_found)} channels found, {channels_updated} updated")

        return {
            "success": success,
            "channels_found": len(channels_found),
            "channels_updated": channels_updated,
            "channels_failed": channels_failed,
            "error": errors[0] if errors else None,
            "channels": channels_found
        }

    async def get_fresh_channels(self) -> List[Dict]:
        """
        Get all channels that haven't expired yet.

        Returns:
            List of fresh channel documents
        """
        try:
            channels = await self.channels_col.find(
                {
                    "status": "active",
                    "urlExpiresAt": {"$gt": datetime.utcnow()}
                }
            ).to_list(None)

            logger.info(f"📺 Retrieved {len(channels)} fresh channels")
            return channels
        except Exception as e:
            logger.error(f"Error retrieving fresh channels: {str(e)}")
            return []

    async def get_channel_by_id(self, channel_id: int) -> Optional[Dict]:
        """
        Get a specific channel by ID.

        Args:
            channel_id: Channel ID to retrieve

        Returns:
            Channel document or None if not found
        """
        try:
            channel = await self.channels_col.find_one(
                {
                    "channelId": channel_id,
                    "status": "active"
                }
            )
            return channel
        except Exception as e:
            logger.error(f"Error retrieving channel {channel_id}: {str(e)}")
            return None
