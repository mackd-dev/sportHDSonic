"""
Channel Routes Module
FastAPI endpoints for channel alias management and stream retrieval
"""

import logging
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends, Header
from pydantic import BaseModel, Field

from .channel_scraper import ChannelScraper

logger = logging.getLogger("channel-routes")

import base64

# ==========================================
# HARD-CODED CLEARKEY MAPPING (OPTIONAL)
# ==========================================
# Primary mapping: numeric scraper channelId -> "kid:key" (HEX)
# Fill this if you know the numeric channelIds for your ClearKey channels.
CLEARKEY_BY_CHANNEL_ID = {
    1: "c31df1600afc33799ecac543331803f2:dd2101530e222f545997d4c553787f85",  # Azam Sport1 HD
    2: "739e7499125b31cc9948da8057b84cf9:1b7d44d798c351acc02f33ddfbb7682a",  # Azam Sport2 HD
    3: "2f12d7b889de381a9fb5326ca3aa166d:51c2d733a54306fdf89acd4c9d4f6005",  # Azam Sport3 HD
    4: "1606cddebd3c36308ec5072350fb790a:04ece212a9201531afdd91c6f468e0b3",  # Azam Sport4 HD
    5: "b5cbe1bb5acf3c7f9995be428245cfcd:89f1188a11e5e000d4443eb27ca378e1",  # Azam One
    6: "3b92b644635f3bad9f7d09ded676ec47:d012a9d5834f69be1313d4864d150a5f",  # Azam Two
    7: "d628ae37a8f0336b970f250d9699461e:1194c3d60bb494aabe9114ca46c2738e",  # SinemaZetu
    8: "8714fe102679348e9c76cfd315dacaa0:a8b86ceda831061c13c7c4c67bd77f8e",  # Wasafi TV
    9: "d861e2b92c744fbba861fb8b1906cf74:977897864cf6d102c85816edb8e403a8",  # Crown Tv
    10: "4dce7643f03c3327832b657d74056b6b:8b8675b9d2ff24dd7c7619d86a698231",  # ChekaPlus TV
    21: "a7e155b282f33335ae8d553f169f443c:c3fdcfd5d509f1ed8550d76a525e34e5",  # Kix Movies
    23: "2d60429f7d043a638beb7349ae25f008:f9b38900f31ce549425df1de2ea28f9d",  # ZBC 2
    25: "31b8fc6289fe3ca698588a59d845160c:f8c4e73f419cb80db3bdf4a974e31894",  # UTV
}

# Fallback mapping: channel name (case-insensitive) -> "kid:key" (HEX)
# Auto-populated from your sports_hd.channels.json export for convenience.
CLEARKEY_BY_CHANNEL_NAME = {
    "azam sports hd1": "c31df1600afc33799ecac543331803f2:dd2101530e222f545997d4c553787f85",
    "azam sports hd2": "739e7499125b31cc9948da8057b84cf9:1b7d44d798c351acc02f33ddfbb7682a",
    "azam two": "3b92b644635f3bad9f7d09ded676ec47:d012a9d5834f69be1313d4864d150a5f",
    "azamsport 1": "c31df1600afc33799ecac543331803f2:dd2101530e222f545997d4c553787f85",
    "fubu": "3dcfbec0e7146928baa55210bf2cb62f:bc85f74f815d9be5ae1dd6defaa05135",
    "sinema zetu": "d628ae37a8f0336b970f250d9699461e:1194c3d60bb494aabe9114ca46c2738e",
}

def _b64url_nopad(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")

def build_clearkey_json(kid_key_hex: str) -> dict:
    """Convert 'kid:key' (HEX) into ClearKey JSON (base64url, no padding) for players."""
    kid_hex, key_hex = kid_key_hex.split(":", 1)
    kid = bytes.fromhex(kid_hex.strip())
    key = bytes.fromhex(key_hex.strip())
    return {"keys": [{"kty": "oct", "kid": _b64url_nopad(kid), "k": _b64url_nopad(key)}]}



# ==========================================
# Pydantic Models
# ==========================================

class CreateAliasRequest(BaseModel):
    """Request to create a new channel alias"""
    channelId: int
    animalName: str
    description: Optional[str] = None


class UpdateAliasRequest(BaseModel):
    """Request to update an existing alias"""
    description: Optional[str] = None
    isActive: Optional[bool] = None


class StreamResponse(BaseModel):
    """Response for stream URL request"""
    alias: str
    channelName: str
    streamUrl: str
    expiresAt: str
    status: str
    lastUpdated: str


class ChannelResponse(BaseModel):
    """Response for channel info"""
    channelId: int
    name: str
    streamUrl: Optional[str]
    status: str
    lastScrapedAt: Optional[str]
    urlExpiresAt: Optional[str]


class AliasResponse(BaseModel):
    """Response for alias info"""
    id: str
    alias: str
    channelId: int
    channelName: str
    animalName: str
    description: Optional[str]
    isActive: bool
    createdAt: str


class ScraperLogResponse(BaseModel):
    """Response for scraper log"""
    id: str
    runStartedAt: str
    runCompletedAt: Optional[str]
    channelsScraped: int
    channelsUpdated: int
    channelsFailed: int
    status: str
    errorMessage: Optional[str]
    createdAt: str


# ==========================================
# Helper Functions
# ==========================================

def _serialize_channel(doc: dict) -> dict:
    """Serialize channel document for API response"""
    if not doc:
        return None

    return {
        "channelId": doc.get("channelId"),
        "name": doc.get("name"),
        "streamUrl": doc.get("streamUrl"),
        "status": doc.get("status"),
        "lastScrapedAt": doc.get("lastScrapedAt").isoformat() if doc.get("lastScrapedAt") else None,
        "urlExpiresAt": doc.get("urlExpiresAt").isoformat() if doc.get("urlExpiresAt") else None,
    }


def _serialize_alias(doc: dict, channel_name: str = None) -> dict:
    """Serialize alias document for API response"""
    if not doc:
        return None

    return {
        "id": str(doc.get("_id")),
        "alias": doc.get("alias"),
        "channelId": doc.get("channelId"),
        "channelName": channel_name,
        "animalName": doc.get("animalName"),
        "description": doc.get("description"),
        "isActive": bool(doc.get("isActive", 1)),
        "createdAt": doc.get("createdAt").isoformat() if doc.get("createdAt") else None,
    }


def _serialize_log(doc: dict) -> dict:
    """Serialize scraper log for API response"""
    if not doc:
        return None

    return {
        "id": str(doc.get("_id")),
        "runStartedAt": doc.get("runStartedAt").isoformat() if doc.get("runStartedAt") else None,
        "runCompletedAt": doc.get("runCompletedAt").isoformat() if doc.get("runCompletedAt") else None,
        "channelsScraped": doc.get("channelsScraped", 0),
        "channelsUpdated": doc.get("channelsUpdated", 0),
        "channelsFailed": doc.get("channelsFailed", 0),
        "status": doc.get("status"),
        "errorMessage": doc.get("errorMessage"),
        "createdAt": doc.get("createdAt").isoformat() if doc.get("createdAt") else None,
    }


# ==========================================
# Route Setup Function
# ==========================================

def setup_channel_routes(app, db, get_current_admin=None):
    """
    Setup channel routes on the FastAPI app.

    Args:
        app: FastAPI application instance
        db: Motor AsyncIOMotorDatabase instance
        get_current_admin: Optional admin dependency function
    """

    router = APIRouter(prefix="/api", tags=["channels"])
    scraper = ChannelScraper(db)

    # Collections
    channels_col = db.channels_streams
    aliases_col = db.channel_aliases
    logs_col = db.scraper_logs

    # ==========================================
    # PUBLIC ENDPOINTS
    # ==========================================

    @router.get("/stream/{alias}")
    async def get_stream_by_alias(alias: str):
        """
        Get stream URL by alias (e.g., paka.nyama).

        Returns the latest valid stream URL for the given alias.
        """
        logger.info(f"📡 Stream request for alias: {alias}")

        # Find alias
        alias_doc = await aliases_col.find_one({"alias": alias})
        if not alias_doc:
            logger.warning(f"⚠️ Alias not found: {alias}")
            raise HTTPException(404, {"error": "Alias not found", "alias": alias})

        # Check if alias is active
        if not alias_doc.get("isActive", 1):
            logger.warning(f"⚠️ Alias inactive: {alias}")
            raise HTTPException(503, {
                "error": "Channel currently unavailable",
                "alias": alias,
                "status": "inactive"
            })

        # Get channel
        channel_doc = await channels_col.find_one({"channelId": alias_doc.get("channelId")})
        if not channel_doc:
            logger.warning(f"⚠️ Channel not found for alias: {alias}")
            raise HTTPException(404, {"error": "Channel not found", "alias": alias})

        # Check if URL is expired
        url_expires_at = channel_doc.get("urlExpiresAt")
        if url_expires_at and url_expires_at <= datetime.utcnow():
            logger.warning(f"⚠️ Stream URL expired for alias: {alias}")
            raise HTTPException(410, {
                "error": "Stream URL expired, scraper will refresh soon",
                "alias": alias,
                "expiresAt": url_expires_at.isoformat()
            })

        # Check channel status
        if channel_doc.get("status") != "active":
            logger.warning(f"⚠️ Channel inactive: {alias}")
            raise HTTPException(503, {
                "error": "Channel currently unavailable",
                "alias": alias,
                "status": channel_doc.get("status")
            })

        logger.info(f"✅ Serving stream for alias: {alias}")

        stream_url = channel_doc.get("streamUrl")
        channel_id = alias_doc.get("channelId")

        # If this stream is DASH (.mpd) and the channel requires ClearKey, attach DRM info.
        # Priority: channelId mapping -> name mapping fallback.
        clearkey_hex = None
        if isinstance(channel_id, int):
            clearkey_hex = CLEARKEY_BY_CHANNEL_ID.get(channel_id)

        if not clearkey_hex:
            ch_name = (channel_doc.get("name") or "").strip().lower()
            if ch_name:
                clearkey_hex = CLEARKEY_BY_CHANNEL_NAME.get(ch_name)

        is_dash = isinstance(stream_url, str) and ".mpd" in stream_url.lower()

        drm_type = None
        license_url = None
        drm_data = None
        if clearkey_hex and is_dash:
            drm_type = "CLEARKEY"
            license_url = clearkey_hex
            try:
                drm_data = build_clearkey_json(clearkey_hex)
            except Exception as e:
                logger.warning(f"⚠️ Invalid ClearKey format for alias={alias}: {e}")
                drm_type = None
                license_url = None
                drm_data = None

        return {
            "alias": alias,
            "channelId": channel_id,
            "channelName": channel_doc.get("name"),
            "streamUrl": stream_url,
            "expiresAt": url_expires_at.isoformat() if url_expires_at else None,
            "status": channel_doc.get("status"),
            "lastUpdated": channel_doc.get("lastScrapedAt").isoformat() if channel_doc.get("lastScrapedAt") else None,
            "drm_type": drm_type,
            "license_url": license_url,
            "drm_data": drm_data
        }

    # ==========================================
    # ADMIN ENDPOINTS
    # ==========================================

    @router.get("/admin/channels")
    async def list_channels(authorization: str = Header(None)):
        """List all channels with their current status."""
        # Note: In production, use proper get_current_admin dependency
        if not authorization:
            raise HTTPException(401, "Authorization required")

        channels = await scraper.get_all_channels()
        return {
            "channels": [_serialize_channel(ch) for ch in channels],
            "total": len(channels)
        }

    @router.get("/admin/aliases")
    async def list_aliases(authorization: str = Header(None)):
        """List all channel aliases."""
        if not authorization:
            raise HTTPException(401, "Authorization required")

        cursor = aliases_col.find({}).sort("alias", 1)
        aliases = await cursor.to_list(None)

        result = []
        for alias_doc in aliases:
            channel = await channels_col.find_one({"channelId": alias_doc.get("channelId")})
            result.append(_serialize_alias(alias_doc, channel.get("name") if channel else None))

        return {
            "aliases": result,
            "total": len(result)
        }

    @router.post("/admin/aliases")
    async def create_alias(request: CreateAliasRequest, authorization: str = Header(None)):
        """Create a new channel alias."""
        if not authorization:
            raise HTTPException(401, "Authorization required")

        # Validate animal name
        animal_name = request.animalName.lower().strip()
        if not animal_name or not animal_name.isalpha():
            raise HTTPException(400, "Invalid animal name")

        # Create alias
        alias_name = f"{animal_name}.nyama"

        # Check if alias already exists
        existing = await aliases_col.find_one({"alias": alias_name})
        if existing:
            raise HTTPException(409, f"Alias '{alias_name}' already exists")

        # Create alias document
        alias_doc = {
            "alias": alias_name,
            "channelId": request.channelId,
            "animalName": animal_name,
            "description": request.description,
            "isActive": 1,
            "createdAt": datetime.utcnow()
        }

        result = await aliases_col.insert_one(alias_doc)
        alias_doc["_id"] = result.inserted_id

        channel = await channels_col.find_one({"channelId": request.channelId})
        logger.info(f"✅ Created alias: {alias_name} -> {channel.get('name') if channel else request.channelId}")

        return _serialize_alias(alias_doc, channel.get("name") if channel else None)

    @router.patch("/admin/aliases/{alias_id}")
    async def update_alias(alias_id: str, request: UpdateAliasRequest, authorization: str = Header(None)):
        """Update an existing alias."""
        if not authorization:
            raise HTTPException(401, "Authorization required")

        from bson import ObjectId

        try:
            alias_oid = ObjectId(alias_id)
        except:
            raise HTTPException(400, "Invalid alias ID")

        # Prepare update
        update_data = {}
        if request.description is not None:
            update_data["description"] = request.description
        if request.isActive is not None:
            update_data["isActive"] = 1 if request.isActive else 0

        if not update_data:
            raise HTTPException(400, "No update data provided")

        # Update
        result = await aliases_col.update_one({"_id": alias_oid}, {"$set": update_data})
        if result.matched_count == 0:
            raise HTTPException(404, "Alias not found")

        # Fetch updated document
        updated = await aliases_col.find_one({"_id": alias_oid})
        channel = await channels_col.find_one({"channelId": updated.get("channelId")})

        logger.info(f"✅ Updated alias: {updated.get('alias')}")

        return _serialize_alias(updated, channel.get("name") if channel else None)

    @router.delete("/admin/aliases/{alias_id}")
    async def delete_alias(alias_id: str, authorization: str = Header(None)):
        """Delete an alias."""
        if not authorization:
            raise HTTPException(401, "Authorization required")

        from bson import ObjectId

        try:
            alias_oid = ObjectId(alias_id)
        except:
            raise HTTPException(400, "Invalid alias ID")

        # Find and delete
        alias_doc = await aliases_col.find_one({"_id": alias_oid})
        if not alias_doc:
            raise HTTPException(404, "Alias not found")

        await aliases_col.delete_one({"_id": alias_oid})

        logger.info(f"✅ Deleted alias: {alias_doc.get('alias')}")

        return {"message": f"Alias '{alias_doc.get('alias')}' deleted"}

    @router.get("/admin/scraper-logs")
    async def get_scraper_logs(limit: int = 20, authorization: str = Header(None)):
        """Get recent scraper logs."""
        if not authorization:
            raise HTTPException(401, "Authorization required")

        logs = await scraper.get_recent_logs(limit)
        return {
            "logs": [_serialize_log(log) for log in logs],
            "total": len(logs)
        }

    @router.get("/admin/scraper-stats")
    async def get_scraper_stats(authorization: str = Header(None)):
        """Get scraper statistics."""
        if not authorization:
            raise HTTPException(401, "Authorization required")

        stats = await scraper.get_scraper_stats()
        return stats

    @router.post("/admin/scraper-run-now")
    async def run_scraper_now(authorization: str = Header(None)):
        """Trigger an immediate scraper run (admin only)."""
        if not authorization:
            raise HTTPException(401, "Authorization required")

        logger.info("🔄 Manual scraper run triggered")
        result = await scraper.scrape_channels()

        return {
            "success": result["success"],
            "channels_found": result["channels_found"],
            "channels_updated": result["channels_updated"],
            "channels_failed": result["channels_failed"],
            "error": result.get("error")
        }

    # Include router in app
    app.include_router(router)
    logger.info("✅ Channel routes registered")
