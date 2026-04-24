"""
Channel Routes Module (Patched)
FastAPI endpoints for channel alias management and stream retrieval
"""

import logging
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends, Header
from pydantic import BaseModel, Field
import base64

from .channel_scraper import ChannelScraper

logger = logging.getLogger("channel-routes")

# ==========================================
# HARD-CODED CLEARKEY MAPPING (UPDATED)
# ==========================================
# Primary mapping: numeric scraper channelId -> "kid:key" (HEX)
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
CLEARKEY_BY_CHANNEL_NAME = {
    "azam sports hd1": "c31df1600afc33799ecac543331803f2:dd2101530e222f545997d4c553787f85",
    "azam sports hd2": "739e7499125b31cc9948da8057b84cf9:1b7d44d798c351acc02f33ddfbb7682a",
    "azam two": "3b92b644635f3bad9f7d09ded676ec47:d012a9d5834f69be1313d4864d150a5f",
    "azamsport 1": "c31df1600afc33799ecac543331803f2:dd2101530e222f545997d4c553787f85",
    "fubu": "3dcfbec0e7146928baa55210bf2cb62f:bc85f74f815d9be5ae1dd6defaa05135",
    "sinema zetu": "d628ae37a8f0336b970f250d9699461e:1194c3d60bb494aabe9114ca46c2738e",
    # ADDED FROM TESTER
    "bein sports 1": "d48b6088253c443eb94d27cb7828f707:e9776141f9e949273a072b0e035070ab",
    "bein1": "d48b6088253c443eb94d27cb7828f707:e9776141f9e949273a072b0e035070ab",
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
    channelId: int
    animalName: str
    description: Optional[str] = None

class UpdateAliasRequest(BaseModel):
    description: Optional[str] = None
    isActive: Optional[bool] = None

# ==========================================
# Route Setup Function
# ==========================================

def setup_channel_routes(app, db, get_current_admin=None):
    router = APIRouter(prefix="/api", tags=["channels"])
    scraper = ChannelScraper(db)

    channels_col = db.channels_streams
    aliases_col = db.channel_aliases

    @router.get("/stream/{alias}")
    async def get_stream_by_alias(alias: str):
        logger.info(f"📡 Stream request for alias: {alias}")

        alias_doc = await aliases_col.find_one({"alias": alias})
        if not alias_doc:
            raise HTTPException(404, {"error": "Alias not found", "alias": alias})

        if not alias_doc.get("isActive", 1):
            raise HTTPException(503, {"error": "Channel currently unavailable", "alias": alias, "status": "inactive"})

        channel_doc = await channels_col.find_one({"channelId": alias_doc.get("channelId")})
        if not channel_doc:
            raise HTTPException(404, {"error": "Channel not found", "alias": alias})

        url_expires_at = channel_doc.get("urlExpiresAt")
        if url_expires_at and url_expires_at <= datetime.utcnow():
            # Try to refresh on demand
            channel_doc = await scraper.scrape_single_channel(alias_doc.get("channelId"))
            if not channel_doc:
                raise HTTPException(410, {"error": "Stream URL expired and refresh failed", "alias": alias})

        stream_url = channel_doc.get("streamUrl")
        channel_id = alias_doc.get("channelId")
        ch_name = (channel_doc.get("name") or "").strip().lower()

        # DRM Logic
        clearkey_hex = None
        if isinstance(channel_id, int):
            clearkey_hex = CLEARKEY_BY_CHANNEL_ID.get(channel_id)

        if not clearkey_hex and ch_name:
            clearkey_hex = CLEARKEY_BY_CHANNEL_NAME.get(ch_name)

        is_dash = isinstance(stream_url, str) and ".mpd" in stream_url.lower()
        
        drm_info = None
        if clearkey_hex and is_dash:
            kid_hex, key_hex = clearkey_hex.split(":", 1)
            drm_info = {
                "type": "CLEARKEY",
                "kid_hex": kid_hex.strip(),
                "key_hex": key_hex.strip(),
                "clearkey_json": build_clearkey_json(clearkey_hex)
            }

        return {
            "alias": alias,
            "channelId": channel_id,
            "channelName": channel_doc.get("name"),
            "streamUrl": stream_url,
            "expiresAt": channel_doc.get("urlExpiresAt").isoformat() if channel_doc.get("urlExpiresAt") else None,
            "status": channel_doc.get("status"),
            "drm": drm_info
        }

    app.include_router(router)
    logger.info("✅ Channel routes registered")
