# PATCHED FILE: Universal "channelId" vs "channel_id" Fix
import os
import asyncio
import json
import time
import uuid
import hmac
import hashlib
import logging
import urllib.parse
import secrets
import re
import httpx
import base64
import binascii
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin, quote, unquote, urlparse

DEFAULT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"

HET140_PRESET = {
    "user_agent": DEFAULT_UA,
    "referer": "https://het140c.ycn-redirect.com/",
    "origin":  "https://het140c.ycn-redirect.com",
}

def _host(u: str) -> str:
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""

def invalid_origin(o: str | None) -> bool:
    if not o:
        return True
    lo = o.lower()
    return lo.startswith("application/") or "mpegurl" in lo or lo.startswith("video/") or lo.startswith("audio/")

def is_ycn_provider(stream_url: str) -> bool:
    if not stream_url:
        return False
    h = _host(stream_url)
    # Strict exclusion for lipopo.live to avoid breaking existing logic
    if "lipopo.live" in h or "lipopotv.live" in h:
        return False
    # Only apply to the ycn-redirect.com family
    return h.endswith("ycn-redirect.com") or ".ycn-redirect.com" in h

def _normalize_alias(a: str) -> str:
    a = (a or "").strip().lower()
    return a

def _looks_like_alias(value: str) -> bool:
    """True when value looks like 'paka.nyama' (not a URL)."""
    if not value:
        return False
    v = value.strip()
    # If it's a URL, it's not an alias
    if urlparse(v).scheme in ("http", "https"):
        return False
    # If it's a normal stream-looking string, ignore
    if v.endswith(".mpd") or v.endswith(".m3u8") or v.endswith(".php") or ".php?" in v:
        return False
    return "." in v  # paka.nyama

async def resolve_alias_to_stream(db, alias: str):
    """
    alias -> channel_aliases -> channelId -> channels_streams -> streamUrl

    If the stored URL is expired or missing, triggers an on-demand scrape
    for that specific channel before returning, so the player always gets
    a fresh URL regardless of whether the background scheduler ran.
    """
    alias = _normalize_alias(alias)
    if not alias:
        return None

    # must match what's stored in DB (e.g. paka.nyama)
    alias_doc = await db.channel_aliases.find_one({"alias": alias})
    if not alias_doc:
        return None
    if alias_doc.get("isActive") in (0, False):
        return None

    channel_id = alias_doc.get("channelId")
    if channel_id is None:
        return None

    stream_doc = await db.channels_streams.find_one({"channelId": int(channel_id)})

    # ✅ Check if URL is expired or missing — trigger on-demand scrape if so
    needs_refresh = False
    if not stream_doc:
        logger.warning(f"⚠️ No stream doc found for channelId={channel_id} (alias={alias}), triggering on-demand scrape...")
        needs_refresh = True
    else:
        url_expires_at = stream_doc.get("urlExpiresAt")
        stream_url = stream_doc.get("streamUrl")
        if not stream_url:
            logger.warning(f"⚠️ Empty streamUrl for channelId={channel_id} (alias={alias}), triggering on-demand scrape...")
            needs_refresh = True
        elif url_expires_at and url_expires_at <= datetime.utcnow():
            logger.warning(
                f"⚠️ Stream URL expired for channelId={channel_id} (alias={alias}) "
                f"at {url_expires_at.isoformat()} — triggering on-demand scrape..."
            )
            needs_refresh = True

    if needs_refresh:
        try:
            from channel_scraper import ChannelScraper
        except ImportError:
            from .channel_scraper import ChannelScraper

        scraper = ChannelScraper(db)

        # Step 1: Scrape ONLY the requested channel immediately so this user
        # gets a fresh URL without waiting for all 100 channels to complete.
        fresh = await scraper.scrape_single_channel(int(channel_id))
        if fresh:
            stream_doc = fresh
            logger.info(f"✅ On-demand scrape succeeded for channelId={channel_id} (alias={alias})")
        else:
            logger.error(f"❌ On-demand scrape failed for channelId={channel_id} (alias={alias}) — serving stale data if available")
            # If we have stale data, use it as last resort rather than failing
            if not stream_doc:
                return None

        # Step 2: Fire-and-forget full scrape of ALL channels in the background.
        # This means all other expired channels get refreshed silently without
        # making this user wait. Any user who clicks another channel after this
        # completes (~1-8 mins) will get a fresh URL too.
        async def _background_full_scrape():
            logger.info(f"\U0001f504 Background full scrape triggered by expired alias \'{alias}\' (channelId={channel_id})")
            try:
                result = await scraper.scrape_channels()
                logger.info(
                    f"\u2705 Background full scrape complete: "
                    f"{result['channels_found']} found, "
                    f"{result['channels_updated']} updated, "
                    f"{result['channels_failed']} failed"
                )
            except Exception as e:
                logger.error(f"\u274c Background full scrape crashed: {e}")

        asyncio.create_task(_background_full_scrape())

    stream_url = stream_doc.get("streamUrl")
    if not stream_url:
        return None

    # ✅ Fetch DRM data from the scraper doc so the app can decrypt it
    return {
        "channelId": int(channel_id),
        "channelName": stream_doc.get("name"),
        "streamUrl": stream_url,
        "urlExpiresAt": stream_doc.get("urlExpiresAt"),
        "status": stream_doc.get("status"),
        "licenseUrl": stream_doc.get("licenseUrl") or stream_doc.get("license_url"),
        "drmType": stream_doc.get("drmType") or stream_doc.get("drm_type"),
        "headers": stream_doc.get("headers")
    }

from passlib.context import CryptContext
from jose import jwt, JWTError

from fastapi import FastAPI, Header, HTTPException, Body, Request, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse, JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError
from pydantic import BaseModel, Field
from bs4 import BeautifulSoup
from enum import Enum

# -------------------------------------------------
# Channel Link Manager (scraper + aliases)
# -------------------------------------------------
try:
    # If running as a script with PYTHONPATH including this folder
    from channel_scheduler import ChannelScheduler
    from channel_routes import setup_channel_routes
except Exception:  # pragma: no cover
    # If running as a package: uvicorn app.main:app
    from .channel_scheduler import ChannelScheduler
    from .channel_routes import setup_channel_routes


# -------------------------------------------------
# Logging
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("stream-debug")

# Suppress noisy httpx logs
logging.getLogger("httpx").setLevel(logging.WARNING)

# -------------------------------------------------
# Configuration & Constants
# -------------------------------------------------
MONGODB_URL = os.getenv("MONGODB_URL")
if not MONGODB_URL:
    raise RuntimeError("❌ MONGODB_URL environment variable is not set")

DATABASE_NAME = "sports_hd"

# 🔐 JWT / Auth config (FIXED)
SECRET_KEY = os.getenv("SECRET_KEY")
if not SECRET_KEY:
    raise RuntimeError("❌ SECRET_KEY environment variable is not set")

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 5

# 🔐 Password hashing
pwd_context = CryptContext(
    schemes=["bcrypt"],
    deprecated="auto"
)

# -------------------------------------------------
# Security
# -------------------------------------------------
security = HTTPBearer()

# ✅ SONICPESA CONFIGURATION
SONICPESA_API_KEY = os.getenv("SONICPESA_API_KEY")
SONICPESA_SECRET_KEY = os.getenv("SONICPESA_SECRET_KEY")
SONICPESA_WEBHOOK_URL = os.getenv("SONICPESA_WEBHOOK_URL")
SONICPESA_BASE_URL = (os.getenv("SONICPESA_BASE_URL") or "https://api.sonicpesa.com/api/v1").rstrip("/")

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")

# ✅ ASPORTSHD BYPASS CODE
ASPORTSHD_BYPASS_CODE = "491YWB317"
EXO_OR_BROWSER_UA = "ReactNativeVideo/3.0 (Linux;Android 11) ExoPlayerLib/2.10.4"

# -------------------------------------------------
# 🔑 Hardcoded ClearKeys for PHP Channels
# -------------------------------------------------
PHP_CHANNELS_CLEARKEYS = {
    "AzamSport1.mpd": "c31df1600afc33799ecac543331803f2:dd2101530e222f545997d4c553787f85",
    "AzamTwo.mpd": "3b92b644635f3bad9f7d09ded676ec47:d012a9d5834f69be1313d4864d150a5f",
    "SinemaZetu.mpd": "d628ae37a8f0336b970f250d9699461e:1194c3d60bb494aabe9114ca46c2738e",
    "AzamSport2.mpd": "739e7499125b31cc9948da8057b84cf9:1b7d44d798c351acc02f33ddfbb7682a",
    "AzamOne.mpd": "b5cbe1bb5acf3c7f9995be428245cfcd:89f1188a11e5e000d4443eb27ca378e1",
    "KIXMovies.mpd": "a7e155b282f33335ae8d553f169f443c:c3fdcfd5d509f1ed8550d76a525e34e5",
    "ChekaPlusTV.mpd": "4dce7643f03c3327832b657d74056b6b:8b8675b9d2ff24dd7c7619d86a698231",
    "AzamSport3.mpd": "2f12d7b889de381a9fb5326ca3aa166d:51c2d733a54306fdf89acd4c9d4f6005",
    "ZamaradiTV.mpd": "c2f5309e756638ef95238636a8ae2593:e1251dfc8cdf06f5a2fc2e05ee693120",
    "AzamSport4.mpd": "1606cddebd3c36308ec5072350fb790a:04ece212a9201531afdd91c6f468e0b3",
    "ZBC2.mpd": "2d60429f7d043a638beb7349ae25f008:f9b38900f31ce549425df1de2ea28f9d",
    "CrownTv.mpd": "d861e2b92c744fbba861fb8b1906cf74:977897864cf6d102c85816edb8e403a8",
    "WasafiTV.mpd": "8714fe102679348e9c76cfd315dacaa0:a8b86ceda831061c13c7c4c67bd77f8e",
    "UTV.mpd": "31b8fc6289fe3ca698588a59d845160c:f8c4e73f419cb80db3bdf4a974e31894",
}

# 🔑 Mapping of channelId to ClearKey license URLs
CLEARKEY_BY_CHANNEL_ID = {
    1: "c31df1600afc33799ecac543331803f2:dd2101530e222f545997d4c553787f85",  # Azam Sport1 HD
    2: "739e7499125b31cc9948da8057b84cf9:1b7d44d798c351acc02f33ddfbb7682a",  # Azam Sport2 HD
    3: "2f12d7b889de381a9fb5326ca3aa166d:51c2d733a54306fdf89acd4c9d4f6005",  # Azam Sport3 HD
    4: "1606cddebd3c36308ec5072350fb790a:04ece212a9201531afdd91c6f468e0b3",  # Azam Sport4 HD
    5: "b5cbe1bb5acf3c7f9995be428245cfcd:89f1188a11e5e000d4443eb27ca378e1",  # Azam One
    6: "3b92b644635f3bad9f7d09ded676ec47:d012a9d5834f69be1313d4864d150a5f",  # Azam Two
    7: "d628ae37a8f0336b970f250d9699461e:1194c3d60bb494aabe9114ca46c2738e",  # SinemaZetu
    8: "8714fe102679348e9c76cfd315dacaa0:a8b86ceda831061c13c7c4c67bd77f8e",  # Wasafi TV
    9: "e3eb6e0656ec3c22aa308aaa3f82c565:a7634d6defd2c255135095c45bc442fb",  # Crown Tv
    10: "4dce7643f03c3327832b657d74056b6b:8b8675b9d2ff24dd7c7619d86a698231",  # ChekaPlus TV
    21: "69646b755f3130303030303030303030:e4a2359b05563399f1d9adfce641724a",  # Kix Movies 
    23: "2d60429f7d043a638beb7349ae25f008:f9b38900f31ce549425df1de2ea28f9d",  # ZBC 2
    25: "31b8fc6289fe3ca698588a59d845160c:f8c4e73f419cb80db3bdf4a974e31894",  # UTV
    26: "e91fec140bc5316f919b4dc9c16287d7:79884fad0dbfcdc43d3e33c82a1f1cfa",  # ZBC
    14: "7995c724a13748ed970840a8ab5bb9b3:67bdaf1e2175b9ff682fcdf0e2354b1e",  # beinsport 3
    15: "e3ce77324a3d4fa2a913b26cc1976052:17774f82a3b9e33ea7a149596acbb20f",  # mbc 2
    11: "c2f5309e756638ef95238636a8ae2593:e1251dfc8cdf06f5a2fc2e05ee693120", #zamaradi

}

PREMIUM_PACKAGES = {
    "DAILY": timedelta(days=1),
    "WEEKLY": timedelta(days=7),
    "MONTHLY": timedelta(days=30)
}

PACKAGE_PRICES = {
    "DAILY": 1000,
    "WEEKLY": 5000,
    "MONTHLY": 15000
}

# --- Database Connection ---
def get_safe_mongodb_url(url: str) -> str:
    if not url: return ""
    if "://" in url and "@" in url:
        try:
            protocol, rest = url.split("://", 1)
            userinfo, hostinfo = rest.rsplit("@", 1)
            if ":" in userinfo:
                username, password = userinfo.split(":", 1)
                safe_username = urllib.parse.quote_plus(username)
                safe_password = urllib.parse.quote_plus(password)
                return f"{protocol}://{safe_username}:{safe_password}@{hostinfo}"
        except Exception:
            pass
    return url

client = AsyncIOMotorClient(get_safe_mongodb_url(MONGODB_URL))
db = client[DATABASE_NAME]

# Collections
devices_col = db.devices
sessions_col = db.sessions
payments_col = db.payments
config_col = db.config
channels_col = db.channels
categories_col = db.categories
schedules_col = db.schedules
vipindi_col = db.vipindi
reminders_col = db.reminders
banners_col = db.banners
ip_guard_col = db.ip_guard  # tracks how many devices registered per IP

# Maximum number of distinct devices allowed to register from the same IP.
# Premium users are always exempt from this limit.
MAX_DEVICES_PER_IP = 3

# Channel Link Manager scheduler handle
channel_scheduler = None  # initialized on startup
# --- Models ---

class AdminUser(BaseModel):
    username: str
    password_hash: str
    role: str = "admin"

class Channel(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    category_id: Optional[str] = Field(None, alias="categoryId")
    alias: Optional[str] = None
    order: int = 0

    logo_url: Optional[str] = ""
    mpd_url: Optional[str] = None
    license_url: Optional[str] = None
    drm_type: str = "WIDEVINE"
    is_premium: bool = False
    active: bool = True
    
    # Relay / Session flags
    is_proxied: bool = False
    session_based_drm: bool = False
    drm_robustness: Optional[str] = None # SW_SECURE_DECODE, HW_SECURE_DECODE, HW_SECURE_ALL

    # Nagra/Azam specific fields
    nv_tenant_id: Optional[str] = None
    nv_authorizations: Optional[str] = None
    user_agent: Optional[str] = Field(None, alias="userAgent")
    referer: Optional[str] = Field(None, alias="referer")
    origin: Optional[str] = Field(None, alias="origin")
    
    # Headers object (preferred way to store all headers)
    headers: Optional[Dict[str, str]] = None

    token: Optional[str] = None

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True
        extra = "ignore"  # 🔥 critical for admin flexibility

class Category(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    icon_url: str  # Logo URL to be displayed in the home screen category header
    logo_url: Optional[str] = None  # Alternative field name for logo (alias support)
    order: int = 0

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True
        extra = "ignore"

class Banner(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    title: str
    image_url: str
    action_url: str
    type: str = "INTERNAL" # INTERNAL, EXTERNAL
    active: bool = True

class Schedule(BaseModel):
    id: Optional[str] = None
    startTime: str  # ISO Format string from Flutter/Android
    homeTeam: str
    awayTeam: str
    homeTeamImage: Optional[str] = None  # Team logo/image URL
    awayTeamImage: Optional[str] = None  # Team logo/image URL
    league: str
    channel: str
    # 🔧 FIX: Allow channelId OR channel_id via alias
    channelId: str = Field(..., alias="channel_id") 

    class Config:
        allow_population_by_field_name = True

class Kipindi(BaseModel):
    id: Optional[str] = None
    name: str
    thumbnailUrl: str
    channelId: str
    active: bool = True
    createdAt: Optional[str] = None

class PaymentUpdate(BaseModel):
    status: str

class StreamSession(BaseModel):
    stream_url: str                 # mpd OR m3u8
    stream_type: str                # DASH | HLS
    drm_type: str = "NONE"           # WIDEVINE | CLEARKEY | NONE
    license_url: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    token: Optional[str] = None

# --- Helpers ---

async def maybe_auto_expire_premium(device: dict) -> dict:
    """
    Checks if a device's premium subscription has expired and downgrades it if so.
    Ensures trialRemaining is reset upon downgrade.
    """
    if not device:
        return device

    is_premium = device.get("isPremium", False)
    premium_until = device.get("premiumUntil")
    now = datetime.utcnow()

    if is_premium and premium_until and premium_until <= now:
        logger.info(f"🚫 Premium expired for {device.get('uuid')} - Auto-downgrading")
        
        TRIAL_AFTER_DOWNGRADE = 5

        update = {
            "isPremium": False,
            "premiumUntil": None,
            "currentPackage": None,
            "trialRemaining": TRIAL_AFTER_DOWNGRADE,
            "trialUsed": False,
            "trialPausedAt": None,
            "lastChannelId": None,
            "downgradedAt": now
        }

        await devices_col.update_one(
            {"uuid": device.get("uuid")},
            {"$set": update}
        )

        # Update the local device object to reflect changes
        device.update(update)
    
    return device

def _parse_duration_seconds_from_doc(doc: dict) -> int:
    """
    Returns duration in SECONDS from either:
      - durationUnit + durationValue
      - durationDays / durationMinutes / durationSeconds
      - duration_days / duration_minutes / duration_seconds
    """
    if not doc:
        return 0

    # New style: durationUnit + durationValue
    unit = (doc.get("durationUnit") or doc.get("duration_unit") or "").strip().lower()
    value = doc.get("durationValue") or doc.get("duration_value")

    if unit and value is not None:
        try:
            v = int(value)
            if unit in ("day", "days"):
                return v * 86400
            if unit in ("minute", "minutes", "min", "mins"):
                return v * 60
            if unit in ("second", "seconds", "sec", "secs"):
                return v
        except Exception:
            pass  # fall back below

    # Legacy style: explicit fields
    try:
        days = int(doc.get("durationDays") or doc.get("duration_days") or 0)
    except Exception:
        days = 0
    try:
        minutes = int(doc.get("durationMinutes") or doc.get("duration_minutes") or 0)
    except Exception:
        minutes = 0
    try:
        seconds = int(doc.get("durationSeconds") or doc.get("duration_seconds") or 0)
    except Exception:
        seconds = 0

    total = days * 86400 + minutes * 60 + seconds
    return int(total)


def _duration_td_from_package_or_payment(package_doc: dict | None, payment_doc: dict | None) -> timedelta:
    """
    Source of truth:
      1) package_doc (from config packages)
      2) payment_doc (stored fields)
    Never silently default to 30 days unless EVERYTHING is missing.
    """
    sec = _parse_duration_seconds_from_doc(package_doc or {})
    if sec <= 0:
        sec = _parse_duration_seconds_from_doc(payment_doc or {})

    # absolute last-resort fallback (keeps server running, but avoids random 30d unless needed)
    if sec <= 0:
        sec = 30 * 86400

    return timedelta(seconds=sec)

# 🔧 PATCH 6 – HEX TO BASE64URL HELPER
def hex_to_base64url(hex_str: str) -> str:
    """Converts a Hex string (e.g. from Admin Panel) to Base64Url (for ClearKey DRM)."""
    try:
        # Strip whitespace
        hex_str = hex_str.strip()
        # Convert hex to binary
        binary = bytes.fromhex(hex_str)
        # Convert binary to base64
        b64 = base64.urlsafe_b64encode(binary).decode('utf-8')
        # Remove padding '=' characters per ClearKey spec
        return b64.rstrip('=')
    except Exception:
        # If conversion fails (e.g., already base64 or invalid), return original
        return hex_str

# 🔧 PATCH 8 – CLEARKEY JSON CONVERTER (WITH BASE64URL ENCODING)
def build_clearkey_json(license_url: str) -> Dict[str, Any]:
    """
    Converts ClearKey format (kid:key hex) to proper JSON structure for ExoPlayer.
    
    CRITICAL: kid and k must be Base64URL encoded (not raw hex) for Android DRM to accept them.
    
    Input: "3dcfbec0e7146928baa55210bf2cb62f:bc85f74f815d9be5ae1dd6defaa05135"
    Output: {"keys": [{"kty": "oct", "kid": "PNv7DnFGkouFILfCvGLw", "k": "vIX3T4FZvroO3dbe+gU1Nw"}]}
    """
    if not license_url or ":" not in license_url:
        logger.warning(f"Invalid ClearKey format: {license_url}")
        return {"keys": []}
    
    try:
        parts = license_url.split(":")
        if len(parts) != 2:
            logger.warning(f"ClearKey must have exactly 2 parts (kid:key), got {len(parts)}")
            return {"keys": []}
        
        kid_hex, key_hex = parts
        kid_hex = kid_hex.strip()
        key_hex = key_hex.strip()
        
        # Convert hex strings to bytes
        kid_bytes = bytes.fromhex(kid_hex)
        key_bytes = bytes.fromhex(key_hex)
        
        # Encode to Base64URL (without padding)
        kid_b64url = base64.urlsafe_b64encode(kid_bytes).decode('utf-8').rstrip('=')
        key_b64url = base64.urlsafe_b64encode(key_bytes).decode('utf-8').rstrip('=')
        
        logger.info(f"ClearKey encoding: kid_hex={kid_hex[:16]}... -> kid_b64url={kid_b64url}")
        logger.info(f"ClearKey encoding: key_hex={key_hex[:16]}... -> key_b64url={key_b64url}")
        
        return {
            "keys": [
                {
                    "kty": "oct",  # Octet sequence (symmetric key)
                    "kid": kid_b64url,  # Base64URL encoded
                    "k": key_b64url     # Base64URL encoded
                }
            ]
        }
    except Exception as e:
        logger.error(f"Failed to build ClearKey JSON: {e}")
        return {"keys": []}



CLEARKEY_SYSTEM_ID = "urn:uuid:e2719d58-a985-b3c9-781a-b030af78d30e"
WIDEVINE_SYSTEM_ID = "urn:uuid:edef8ba9-79d6-4ace-a3c8-27dcd51d21ed"
PLAYREADY_SYSTEM_ID = "urn:uuid:9a04f079-9840-4286-ab92-e65be0885f95"


def should_use_clearkey_mpd_proxy(mpd_url: Optional[str], drm_type: Optional[str]) -> bool:
    if not mpd_url or (drm_type or "").upper() != "CLEARKEY":
        return False
    try:
        parsed = urlparse(mpd_url)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
    except Exception:
        return False
    # ✅ Case-insensitive check for Bein channels on SecureSwiftContent
    is_secureswift = host.endswith("secureswiftcontent.com")
    is_bein_path = "/content/dash/live/channel(bein" in path.lower()
    is_mpd = path.endswith("/master.mpd")
    
    return is_secureswift and is_bein_path and is_mpd


def rewrite_mpd_for_clearkey_android(mpd_xml: str, manifest_base_url: Optional[str] = None, kid_hex: Optional[str] = None) -> str:
    """Rewrite a multi-DRM DASH MPD so Android/ExoPlayer uses ClearKey reliably.

    This targets manifests like SecureSwiftContent Bein that advertise PlayReady + Widevine
    but omit cenc:default_KID. ExoPlayer may show tracks (quality/language) yet render black
    because the DRM session can't map samples to your ClearKey KID.

    Backend-only fix strategy:
      - Remove WV/PR hints (laurl/pssh/pro + WV/PR ContentProtection)
      - Ensure ClearKey ContentProtection exists *per AdaptationSet*
      - Ensure cenc:default_KID exists (we inject it onto BOTH:
          a) the mp4protection ContentProtection element (most reliable)
          b) the ClearKey ContentProtection element
      - Optionally overwrite BaseURL so all segments flow through our proxy
    """
    try:
        import xml.etree.ElementTree as ET

        out = mpd_xml

        # Strip tags that can mislead the player toward other DRMs
        out = re.sub(r'<ms:laurl[^>]*>.*?</ms:laurl>', '', out, flags=re.DOTALL | re.IGNORECASE)
        out = re.sub(r'<(?:cenc:)?pssh[^>]*>.*?</(?:cenc:)?pssh>', '', out, flags=re.DOTALL | re.IGNORECASE)
        out = re.sub(r'<mspr:pro[^>]*>.*?</mspr:pro>', '', out, flags=re.DOTALL | re.IGNORECASE)
        out = re.sub(r'\s+dashif:laurl="[^"]*"', '', out, flags=re.IGNORECASE)
        out = re.sub(r'\s+licenseServerUrl="[^"]*"', '', out, flags=re.IGNORECASE)

        ns = {
            'mpd': 'urn:mpeg:dash:schema:mpd:2011',
            'cenc': 'urn:mpeg:cenc:2013',
        }

        # Preserve familiar DASH namespace formatting (avoid ns0 prefixes)
        ET.register_namespace('', ns['mpd'])
        ET.register_namespace('cenc', ns['cenc'])


        root = ET.fromstring(out)

        # Normalize KID to UUID-with-dashes
        kid_uuid = None
        if kid_hex:
            k = (kid_hex or '').strip().replace('-', '').lower()
            if len(k) == 32 and all(c in '0123456789abcdef' for c in k):
                kid_uuid = f"{k[:8]}-{k[8:12]}-{k[12:16]}-{k[16:20]}-{k[20:]}"

        # Overwrite BaseURL for proxy routing
        if manifest_base_url:
            safe_base = manifest_base_url.rstrip('/') + '/'
            for b in list(root.findall('mpd:BaseURL', ns)):
                root.remove(b)
            base_el = ET.Element(f"{{{ns['mpd']}}}BaseURL")
            base_el.text = safe_base
            root.insert(0, base_el)
            # logger.info(f"Injected MPD BaseURL for proxying: {safe_base}")

        def _scheme(el):
            return (el.attrib.get('schemeIdUri') or '').strip().lower()

        for aset in root.findall('.//mpd:AdaptationSet', ns):
            cps = list(aset.findall('mpd:ContentProtection', ns))

            # Remove PlayReady/Widevine ContentProtection
            for cp in list(cps):
                scheme = _scheme(cp)
                if scheme in (WIDEVINE_SYSTEM_ID.lower(), PLAYREADY_SYSTEM_ID.lower()):
                    aset.remove(cp)

            cps = list(aset.findall('mpd:ContentProtection', ns))
            mp4prot = None
            for cp in cps:
                if _scheme(cp) == 'urn:mpeg:dash:mp4protection:2011':
                    mp4prot = cp
                    break

            has_ck = any(_scheme(cp) == CLEARKEY_SYSTEM_ID.lower() for cp in cps)

            # Inject default_KID on mp4protection (best compatibility)
            if mp4prot is not None and kid_uuid:
                mp4prot.set(f"{{{ns['cenc']}}}default_KID", kid_uuid)

            # Ensure ClearKey ContentProtection exists
            if mp4prot is not None and not has_ck:
                ck = ET.Element(f"{{{ns['mpd']}}}ContentProtection")
                ck.set('schemeIdUri', CLEARKEY_SYSTEM_ID)
                ck.set('value', 'ClearKey 1.0')
                if kid_uuid:
                    ck.set(f"{{{ns['cenc']}}}default_KID", kid_uuid)

                # Insert right after mp4protection
                idx = list(aset).index(mp4prot) + 1
                aset.insert(idx, ck)

        rewritten = ET.tostring(root, encoding='utf-8', xml_declaration=True).decode('utf-8')
        # logger.info("Rewrote MPD ContentProtection for Android ClearKey compatibility (per-AdaptationSet + default_KID)")
        return rewritten

    except Exception as e:
        logger.error(f"Failed to rewrite MPD for ClearKey Android: {e}")
        return mpd_xml
def serialize_doc(doc, for_admin: bool = False):
    """
    Serializes MongoDB document for API responses.
    
    Args:
        doc: MongoDB document
        for_admin: If True, keeps all security fields (nv_authorizations, etc.)
                   If False, moves them to headers and removes originals (for app)
    """
    if not doc:
        return None

    if "_id" in doc:
        doc["_id"] = str(doc["_id"])

    if "id" not in doc:
        doc["id"] = doc.get("_id")

    # Category-specific logic
    if "icon_url" in doc or "iconUrl" in doc:
        # Normalize category fields (handle both camelCase and snake_case from DB)
        category_mappings = {
            "iconUrl": "icon_url",
            "logoUrl": "logo_url",
        }
        
        for src, dst in category_mappings.items():
            if src in doc:
                if dst not in doc or doc[dst] is None:
                    doc[dst] = doc[src]
                if for_admin and (src not in doc or doc[src] is None):
                    doc[src] = doc[dst]
        
        # Ensure icon_url is returned as the primary field
        if "logo_url" in doc and doc["logo_url"] and ("icon_url" not in doc or not doc["icon_url"]):
            doc["icon_url"] = doc["logo_url"]
    
    # 🔥 LIFESAVER PATCH: Fake the mpd_url for the Android app so it doesn't crash
    if not doc.get("mpd_url") and not doc.get("mpdUrl"):
        doc["mpd_url"] = ""
    
    # Channel-specific logic
    if "mpd_url" in doc or "mpdUrl" in doc:

        # Normalize fields (handle both camelCase and snake_case from DB)
        mappings = {
            "categoryId": "category_id",
            "isPremium": "is_premium",
            "logoUrl": "logo_url",
            "mpdUrl": "mpd_url",
            "licenseUrl": "license_url",
            "drmType": "drm_type",
            "isProxied": "is_proxied",
            "sessionBasedDrm": "session_based_drm",
            "drmRobustness": "drm_robustness",
            "nvTenantId": "nv_tenant_id",
            "nvAuthorizations": "nv_authorizations",
            "userAgent": "user_agent",
            "token": "token", # Token is same in both
        }

        for src, dst in mappings.items():
            if src in doc:
                # Always ensure snake_case exists for backend logic
                if dst not in doc or doc[dst] is None:
                    doc[dst] = doc[src]
                # For admin, also ensure camelCase exists for UI compatibility
                if for_admin and (src not in doc or doc[src] is None):
                    doc[src] = doc[dst]

        # Auto-build headers ONLY for app, NEVER admin
        if not for_admin and doc.get("headers") is None:
            doc["headers"] = {
                "nv-authorizations": doc.get("nv_authorizations"),
                "nv-tenant-id": doc.get("nv_tenant_id"),
                "User-Agent": doc.get("user_agent"),
                "Referer": doc.get("referer"),
                "Origin": doc.get("origin"),
            }

        # Clean sensitive fields ONLY for app
        if not for_admin:
            for key in [
                "user_agent",
                "referer",
                "origin",
                "nv_tenant_id",
                "nv_authorizations",
            ]:
                doc.pop(key, None)

        # Remove camelCase duplicates ONLY for app, keep for admin UI
        if not for_admin:
            for key in mappings.keys():
                doc.pop(key, None)

            if "category_id" in doc:
                doc["categoryId"] = doc["category_id"]

    return doc


# 🔧 PATCH 4 – ENFORCE CLEARKEY FORMAT
def is_valid_clearkey(value: str) -> bool:
    """Validates ClearKey format: kid:key"""
    return bool(value and ":" in value and len(value.split(":")) == 2)


# 🔧 PATCH 7 – PHONE NUMBER NORMALIZATION
def normalize_phone(phone: str) -> str:
    """
    Normalizes Tanzanian phone numbers to 255XXXXXXXXX format.
    """
    # Remove any non-digit characters
    clean_phone = "".join(filter(str.isdigit, phone))
    
    # If it starts with 0, replace with 255
    if clean_phone.startswith("0") and len(clean_phone) == 10:
        return "255" + clean_phone[1:]
    
    # If it's already 255...
    if clean_phone.startswith("255") and len(clean_phone) == 12:
        return clean_phone
        
    # Return as is if it doesn't match expected patterns (e.g., short numbers, international numbers)
    return clean_phone



def detect_stream_type(url: str) -> str:
    # ✅ Add this safety check
    if not url:
        return "UNKNOWN"
             
    url = url.lower()
    if ".mpd" in url:
        return "DASH"
    if ".m3u8" in url:
        return "HLS"
    return "UNKNOWN"

    # PHP players → unknown until resolved
    if url.endswith(".php"):
        return StreamType.DASH  # default assumption, may change after resolve

    return StreamType.DASH


def generate_secure_token():
    return secrets.token_urlsafe(32)

# ✅ Step 1 — Add stream capability model (NON-BREAKING)
def normalize_stream_caps(session: dict) -> dict:
    """Ensures session has all required stream capability fields."""
    session.setdefault("stream_type", "dash")   # dash | hls | mp4
    session["drm_type"] = (session.get("drm_type") or "").lower()
    session.setdefault("license_url", None)
    session.setdefault("drm_headers", {})
    session.setdefault("requires_proxy", False)
    
    # Ensure headers are present
    if "headers" not in session or session["headers"] is None:
        session["headers"] = {}
        
    return session

async def get_admin_flags():
    flags = await config_col.find_one({"playerMode": {"$exists": True}})

    if not flags:
        return {
            "maintenance": False,
            "forceLogout": False,
            "playerMode": "EXO",
            "trialSeconds": 300,
            "packages": [],
            "currency": "TZS",
            "support": {}
        }

    # Defensive: Ensure _id is a string even if serialize_doc is modified
    if "_id" in flags:
        flags["_id"] = str(flags["_id"])

    return serialize_doc(flags, for_admin=True)

async def get_current_admin(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("role") != "admin":
            logger.warning(f"❌ Admin Auth Failed: Role mismatch for user {payload.get('sub')}")
            raise HTTPException(403, "Not authorized as admin")
        return payload
    except JWTError as e:
        logger.error(f"❌ Admin Auth Failed: {str(e)}")
        raise HTTPException(401, "Invalid or expired admin token")

# ✅ SONICPESA WEBHOOK VERIFICATION + PAYMENT HELPERS
def verify_sonicpesa_signature(payload_raw: bytes, signature: str) -> bool:
    """Verify SonicPesa webhook signature using HMAC SHA256."""
    if not SONICPESA_SECRET_KEY:
        logger.error("❌ SONICPESA_SECRET_KEY is not configured")
        return False

    if not signature:
        logger.error("❌ Missing X-SonicPesa-Signature header")
        return False

    expected = hmac.new(
        SONICPESA_SECRET_KEY.encode("utf-8"),
        payload_raw,
        hashlib.sha256
    ).hexdigest()

    is_valid = hmac.compare_digest(expected, signature.strip())
    if not is_valid:
        logger.error("❌ Invalid SonicPesa webhook signature")
    return is_valid


def map_sonicpesa_status(raw_status: Optional[str]) -> str:
    status = (raw_status or "").strip().upper()
    if status == "SUCCESS":
        return "COMPLETED"
    if status in {"PENDING", "INPROGRESS"}:
        return "PENDING"
    if status in {"CANCELLED", "USERCANCELLED", "REJECTED"}:
        return "FAILED"
    return "PENDING"


def extract_sonicpesa_gateway_status(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ""

    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    transaction = payload.get("transaction") if isinstance(payload.get("transaction"), dict) else {}

    candidates = [
        payload.get("payment_status"),
        payload.get("status"),
        data.get("payment_status"),
        data.get("status"),
        transaction.get("status"),
    ]

    for candidate in candidates:
        if candidate:
            return str(candidate).upper()
    return ""


async def find_payment_by_order_id(order_id: str):
    cursor = payments_col.find({
        "$or": [{"orderId": order_id}, {"order_id": order_id}]
    }).sort("createdAt", -1)
    payments = await cursor.to_list(length=1)
    return payments[0] if payments else None


async def get_package_doc_for_payment(payment: dict):
    config = await config_col.find_one({"name": "global"}) or {}
    pkg_id = payment.get("package")
    return next(
        (p for p in config.get("packages", []) if str(p.get("id")) == str(pkg_id)),
        None
    )


async def complete_payment_and_upgrade_user(payment: dict, gateway_payload: Optional[dict] = None, source: str = "sonic"):
    """Apply the existing upgrade business logic without changing the subscription flow."""
    if payment.get("status") == "COMPLETED":
        return payment

    now = datetime.utcnow()
    package_doc = await get_package_doc_for_payment(payment)
    duration_td = _duration_td_from_package_or_payment(package_doc, payment)
    duration_seconds = int(duration_td.total_seconds())
    duration_days = max(1, int((duration_seconds + 86399) // 86400))

    uuid_ = payment["uuid"]
    device = await devices_col.find_one({"uuid": uuid_})

    if device and device.get("isPremium") and device.get("premiumUntil"):
        current_expiry = device["premiumUntil"]
        base_date = max(current_expiry, now)
        premium_until = base_date + duration_td
    else:
        premium_until = now + duration_td

    await devices_col.update_one(
        {"uuid": uuid_},
        {"$set": {
            "isPremium": True,
            "premiumUntil": premium_until,
            "currentPackage": payment.get("package"),
            "trialRemaining": 0,
            "trialUsed": True,
            "upgradedAt": now
        }}
    )

    payment_update = {
        "status": "COMPLETED",
        "duration_seconds": duration_seconds,
        "durationSeconds": duration_seconds,
        "duration_days": duration_days,
        "durationDays": duration_days,
        "updatedAt": now,
        "completion_source": source,
        "processing": False,
    }

    payload = gateway_payload or {}
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    if payload:
        payment_update["webhook_payload"] = payload
    if payload.get("reference") or data.get("reference"):
        payment_update["reference"] = payload.get("reference") or data.get("reference")
    if payload.get("transid") or data.get("transid"):
        payment_update["transid"] = payload.get("transid") or data.get("transid")
    if payload.get("channel") or data.get("channel"):
        payment_update["channel"] = payload.get("channel") or data.get("channel")
    if extract_sonicpesa_gateway_status(payload):
        payment_update["gateway_status"] = extract_sonicpesa_gateway_status(payload)

    await payments_col.update_one(
        {"_id": payment["_id"]},
        {"$set": payment_update}
    )

    payment.update(payment_update)
    payment["premiumUntil"] = premium_until
    return payment

def extract_channel_id_from_url(url: str) -> Optional[str]:
    """Extract channel ID from player.php?c=X URL"""
    match = re.search(r'[?&]c=([^&]+)', url)
    return match.group(1) if match else None

async def resolve_php_player(player_url: str) -> Optional[Dict[str, Any]]:
    """
    Updated PHP stream resolver based on test_resolver.py.
    Mimics pixtvmax app headers to bypass 403 errors and extracts MPD/License data.
    """
    logger.info(f"🧩 Resolving PHP player: {player_url}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.91 Mobile Safari/537.36",
        "Referer": "https://lipopotv.live/",
        "Origin": "https://lipopotv.live",
        "X-Package-Name": "com.pixtvmax.app",
        "X-Requested-With": "com.pixtvmax.app",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True, headers=headers) as client:
            resp = await client.get(player_url)
            if resp.status_code != 200:
                logger.error(f"PHP resolver HTTP {resp.status_code}")
                return None

            html = resp.text

            # 1️⃣ Extract DASH (.mpd)
            mpd_match = re.search(r'["\'](https?://[^"\']+\.mpd[^"\']*)["\']', html)
            if mpd_match:
                mpd_url = mpd_match.group(1).replace('\\/', '/')
                
                # Also try to extract a Widevine license if present (as a fallback)
                lic_match = re.search(r'["\'](https?://[^"\']+/widevine[^"\']*)["\']', html)
                license_url = lic_match.group(1).replace('\\/', '/') if lic_match else None
                
                logger.info(f"✅ PHP resolved DASH: {mpd_url}")
                return {
                    "stream_type": "DASH",
                    "stream_url": mpd_url,
                    "license_url": license_url
                }

            # 2️⃣ Extract HLS (.m3u8) fallback
            m3u8_match = re.search(r'["\'](https?://[^"\']+\.m3u8[^"\']*)["\']', html)
            if m3u8_match:
                hls_url = m3u8_match.group(1).replace('\\/', '/')
                logger.info(f"✅ PHP resolved HLS: {hls_url}")
                return {
                    "stream_type": "HLS",
                    "stream_url": hls_url,
                }

            logger.error("❌ PHP resolver: No stream found in HTML")
            return None

    except Exception:
        logger.exception("💥 PHP resolver crashed")
        return None

async def resolve_asportshd_stream(player_url: str) -> Optional[Dict[str, Any]]:
    """Improved scraper for ASportHD to handle Azam/Nagra streams with Paywall Bypass"""
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://asportshd.com/',
                'Origin': 'https://asportshd.com'
            }
            
            # Step 1: Submit the access code to establish a session
            index_url = "https://asportshd.com/index.php"
            logger.info(f"Bypassing paywall with code: {ASPORTSHD_BYPASS_CODE}")
            await client.post(index_url, data={"code": ASPORTSHD_BYPASS_CODE}, headers=headers)
            
            # Step 2: Access the player page
            resp = await client.get(player_url, headers=headers)
            if resp.status_code != 200: 
                logger.error(f"Failed to access player page. Status: {resp.status_code}")
                return None
            
            html = resp.text
            
            # 1. Extract MPD URL (Handles escaped slashes and various filenames)
            mpd_match = re.search(r'playlistUrl\s*=\s*["\']([^"\']+)["\']', html)
            mpd_url = mpd_match.group(1).replace('\\/', '/') if mpd_match else None
            
            # 2. Extract Nagra Authorization Token
            auth_match = re.search(r'nvAuth\s*=\s*["\']([^"\']+)["\']', html)
            nv_auth = auth_match.group(1) if auth_match else None
            
            # 3. Extract Tenant ID
            tenant_match = re.search(r"nv-tenant-id'\]\s*=\s*['\"]([^'\"]+)['\"]", html)
            nv_tenant = tenant_match.group(1) if tenant_match else "AZY4SJ9B"
            
            # 4. Extract License Server URL
            lic_match = re.search(r"'com\\.widevine\\.alpha':\s*['\"]([^'\"]+)['\"]", html)
            license_url = lic_match.group(1) if lic_match else None
    
            if not mpd_url:
                logger.error("Could not find playlistUrl in HTML")
                return None
            
            return {
                'mpd_url': mpd_url,
                'license_url': license_url or 'https://azy4sj9b.anycast.nagra.com/AZY4SJ9B/wvls/contentlicenseservice/v1/licenses',
                'headers': {
                    'nv-tenant-id': nv_tenant,
                    'nv-authorizations': nv_auth,
                    'User-Agent': 'ReactNativeVideo/3.0 (Linux;Android 11) ExoPlayerLib/2.10.4',
                    'Referer': 'app.azamtvmax.com',
                    'Origin': 'app.azamtvmax.com'
                }
            }
    except Exception as e:
        logger.error(f"Scraper error: {e}")
        return None
async def resolve_lipopotv_stream(player_url: str) -> Optional[Dict[str, Any]]:
    """
    Resolver for LipoPoTV stream URLs.
    Extracts MPD/HLS URLs and license information from the PHP player page.
    """
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            # Package name verification is required by lipopotv.live
            headers = {
                "User-Agent": "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.91 Mobile Safari/537.36",
                "Referer": "https://lipopotv.live/",
                "Origin": "https://lipopotv.live",
                "X-Package-Name": "com.pixtvmax.app"
            }

            logger.info(f"🚀 LipoPoTV resolver fetching: {player_url}")
            resp = await client.get(player_url, headers=headers)
            
            if resp.status_code != 200:
                logger.error(f"❌ LipoTV page fetch failed. Status: {resp.status_code}")
                return None

            html = resp.text
            logger.info(f"📄 LipoPoTV HTML fetched, length: {len(html)}")

            # Extract MPD URL
            mpd_match = re.search(r'["\'](https?://[^"\']+\.mpd[^"\']*)["\']', html)
            # Extract License URL
            lic_match = re.search(r'["\'](https?://[^"\']+/widevine[^"\']*)["\']', html)
            # Extract HLS URL as fallback
            hls_match = re.search(r'["\'](https?://[^"\']+\.m3u8[^"\']*)["\']', html)

            if mpd_match:
                mpd_url = mpd_match.group(1).replace('\\/', '/')
                license_url = lic_match.group(1).replace('\\/', '/') if lic_match else None
                logger.info(f"✅ LipoPoTV MPD found: {mpd_url}")
                if license_url:
                    logger.info(f"✅ LipoPoTV License found: {license_url}")
                
                return {
                    "mpd_url": mpd_url,
                    "license_url": license_url,
                    "drm_type": "WIDEVINE" if license_url else "NONE",
                    "headers": {
                        "User-Agent": "ExoPlayer",
                        "Referer": "https://lipopotv.live/",
                        "Origin": "https://lipopotv.live",
                        "X-Package-Name": "com.pixtvmax.app"
                    }
                }
            
            if hls_match:
                hls_url = hls_match.group(1).replace('\\/', '/')
                logger.info(f"✅ LipoPoTV HLS found: {hls_url}")
                return {
                    "stream_url": hls_url,
                    "drm_type": "NONE",
                    "headers": {
                        "User-Agent": "Mozilla/5.0",
                        "Referer": "https://lipopotv.live/",
                        "Origin": "https://lipopotv.live",
                        "X-Package-Name": "com.pixtvmax.app"
                    }
                }

            logger.error("❌ LipoTV: No stream URL found in HTML")
            # Log a snippet of HTML for debugging if no match found
            logger.debug(f"HTML Snippet: {html[:1000]}")
            return None

    except Exception as e:
        logger.error(f"❌ LipoTV resolver error: {str(e)}")
        return None


# --- App Setup ---
# Note: The rest of the file (FastAPI routes, etc.) should follow here.
# Since I only have the first 500 lines, I'll append the rest from the original file.
app = FastAPI(title="PixTvMax Production API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_headers=["*"],
    allow_methods=["*"]
)

# 🆕 Channel Link Manager routes
setup_channel_routes(app, db, get_current_admin)


# ✅ Background Task Function
async def subscription_cleanup_task():
    """Periodically checks for expired subscriptions and downgrades them."""
    while True:
        try:
            logger.info("⏰ Running hourly subscription cleanup...")
            now = datetime.utcnow()

            # ✅ pull trialSeconds from config (defaults to 5)
            flags = await get_admin_flags()
            trial_seconds = int(flags.get("trialSeconds", 5) or 5)

            result = await devices_col.update_many(
                {
                    "isPremium": True, 
                    "premiumUntil": {"$lte": now}
                },
                {"$set": {
                    "isPremium": False, 
                    "premiumUntil": None,
                    "currentPackage": None,
                    "trialRemaining": trial_seconds,
                    "trialPausedAt": None,
                    "lastChannelId": None,
                    "downgradedAt": now
                }}
            )
            if result.modified_count > 0:
                logger.info(f"📉 Background Cleanup: Downgraded {result.modified_count} users.")
        except Exception as e:
            logger.error(f"❌ Error in subscription cleanup task: {e}")
        
        # Wait for 1 hour (3600 seconds) before running again
        await asyncio.sleep(3600)

@app.on_event("startup")
async def startup():
    global channel_scheduler
    # 🟡 Existing indexes
    await payments_col.create_index("order_id")
    await payments_col.create_index("orderId")
    await payments_col.create_index("internal_payment_id")
    await payments_col.create_index("status")
    await devices_col.create_index("uuid", unique=True)
    await ip_guard_col.create_index([("ip", 1), ("uuid", 1)], unique=True)
    await ip_guard_col.create_index("ip")
    
    # ✅ SONICPESA STARTUP VALIDATION
    if not SONICPESA_API_KEY:
        logger.error("❌ SONICPESA_API_KEY not set!")
    if not SONICPESA_SECRET_KEY:
        logger.error("❌ SONICPESA_SECRET_KEY not set!")
    if not SONICPESA_WEBHOOK_URL:
        logger.warning("⚠️ SONICPESA_WEBHOOK_URL not set - remember to configure it in SonicPesa dashboard")
    else:
        logger.info(f"✅ SonicPesa webhook URL configured: {SONICPESA_WEBHOOK_URL}")
    logger.info(f"✅ SonicPesa base URL: {SONICPESA_BASE_URL}")

    # ✅ START THE BACKGROUND TASK
    asyncio.create_task(subscription_cleanup_task())

    # 🆕 Channel Link Manager startup (scrape + refresh loop)
    # Interval is 4 hours — scraper marks URLs fresh for 5 hours, so this
    # ensures we always refresh before the DB record expires.
    try:
        channel_scheduler = ChannelScheduler(db, interval_hours=4)
        await channel_scheduler.start()
        logger.info("✅ Channel Link Manager scheduler started (4-hour interval)")
    except Exception as e:
        logger.error(f"❌ Failed to start Channel Link Manager: {e}")



@app.on_event("shutdown")
async def shutdown():
    global channel_scheduler

    if channel_scheduler:
        try:
            await channel_scheduler.stop()
            logger.info("✅ Channel Link Manager scheduler stopped")
        except Exception as e:
            logger.error(f"❌ Failed to stop Channel Link Manager: {e}")

@app.get("/")
async def root():
    return {"status": "OK", "service": "PixTb Max"}

@app.get("/config")
async def get_config():
    flags = await get_admin_flags()
    banners = await banners_col.find({"active": True}).to_list(100)
    
    # Get support links from config
    support = flags.get("support", {})
    whatsapp = support.get("whatsapp", "https://wa.me/255000000000")
    telegram = support.get("telegram", "https://t.me/azammax")

    return {
        "maintenance": flags.get("maintenance", False),
        "forceLogout": flags.get("forceLogout", False),
        "playerMode": flags.get("playerMode", "EXO",),
        "trialSeconds": flags.get("trialSeconds", 300),
        "packages": flags.get("packages", []),
        "currency": flags.get("currency", "TZS"),
        "banners": [serialize_doc(b) for b in banners],
        "support_links": {
            "whatsapp": whatsapp,
            "telegram": telegram
        }
    }


# ✅ MUST BE TOP-LEVEL (NO SPACES BEFORE @)
@app.get("/packages")
async def get_packages():
    config = await config_col.find_one({"name": "global"})

    if not config or "packages" not in config:
        return []

    packages = []
    for p in config.get("packages", []):
        if p.get("active") is True:
            packages.append({
                "id": p.get("id"),
                "name": p.get("name"),
                "durationDays": int(p.get("duration_days", 0)),
                "priceTZS": int(p.get("price", 0)),
                "isRecommended": bool(p.get("recommended", False))
            })

    return packages





@app.get("/content/discovery")
async def get_discovery():
    categories = await categories_col.find().sort("order", 1).to_list(None)
    channels = await channels_col.find({"active": True}).sort("order", 1).to_list(None)
    
    return {
        "categories": [serialize_doc(c) for c in categories],
        "channels": [serialize_doc(c) for c in channels]
    }

# --- GET: Works for both Android App and Admin Panel ---
@app.get("/api/schedules")
@app.get("/schedules")
async def get_schedules():
    # We fetch all; the Android app handles date filtering locally as per your code
    cursor = schedules_col.find({})
    schedules = await cursor.to_list(length=100)
    return [serialize_doc(doc) for doc in schedules]

# --- POST: Required for Admin Panel "Save" button ---
@app.post("/api/schedules")
async def create_or_update_schedule(
    schedule: Schedule, 
    admin: dict = Depends(get_current_admin)
):
    schedule_dict = schedule.dict()
    # If ID exists, update; otherwise create new
    sched_id = schedule_dict.get("id") or str(uuid.uuid4())
    schedule_dict["id"] = sched_id
    
    await schedules_col.update_one(
        {"id": sched_id},
        {"$set": schedule_dict},
        upsert=True
    )
    return {"status": "success", "id": sched_id}

# --- DELETE: Required for Admin Panel delete action ---
@app.delete("/api/schedules/{schedule_id}")
async def delete_schedule(
    schedule_id: str, 
    admin: dict = Depends(get_current_admin)
):
    result = await schedules_col.delete_one({"id": schedule_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"status": "success"}

# ─────────────────────────────────────────────────────────────────────────────
# VIPINDI (TV Programs / Shows) ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/vipindi")
@app.get("/vipindi")
async def get_vipindi():
    """Get all active vipindi for home screen ticker"""
    cursor = vipindi_col.find({"active": True})
    docs = await cursor.to_list(length=200)
    return [serialize_doc(doc) for doc in docs]

@app.get("/admin/vipindi")
async def admin_get_vipindi(admin: dict = Depends(get_current_admin)):
    """Admin: Get all vipindi (including inactive)"""
    cursor = vipindi_col.find({})
    docs = await cursor.to_list(length=200)
    return [serialize_doc(doc) for doc in docs]

@app.post("/api/vipindi")
@app.post("/admin/vipindi")
async def create_or_update_kipindi(
    kipindi: Kipindi,
    admin: dict = Depends(get_current_admin)
):
    kipindi_dict = kipindi.dict()
    kipindi_id = kipindi_dict.get("id") or str(uuid.uuid4())
    kipindi_dict["id"] = kipindi_id
    kipindi_dict["createdAt"] = datetime.utcnow().isoformat()
    await vipindi_col.update_one(
        {"id": kipindi_id},
        {"$set": kipindi_dict},
        upsert=True
    )
    return {"status": "success", "id": kipindi_id}

@app.delete("/api/vipindi/{kipindi_id}")
@app.delete("/admin/vipindi/{kipindi_id}")
async def delete_kipindi(
    kipindi_id: str,
    admin: dict = Depends(get_current_admin)
):
    result = await vipindi_col.delete_one({"id": kipindi_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Kipindi not found")
    return {"status": "success"}

@app.post("/schedules/remind")
async def add_reminder(payload: dict = Body(...)):
    """Handle bell notification clicks by registering device interest"""
    schedule_id = payload.get("scheduleId")
    device_uuid = payload.get("uuid")

    if not schedule_id or not device_uuid:
        raise HTTPException(400, "Missing scheduleId or uuid")

    try:
        # Upsert: If the user clicks the bell again, it just updates the timestamp
        await reminders_col.update_one(
            {"scheduleId": schedule_id, "uuid": device_uuid},
            {"$set": {
                "createdAt": datetime.utcnow(),
                "notified": False
            }},
            upsert=True
        )
        return {"status": "SUCCESS", "message": "Reminder set successfully"}
    except Exception as e:
        logger.error(f"Reminder Error: {e}")
        raise HTTPException(500, "Could not set reminder")

@app.post("/device/register")
async def register_device(payload: dict, request: Request):
    uuid_ = payload.get("uuid")
    if not uuid_:
        raise HTTPException(400, "UUID required")

    now = datetime.utcnow()
    device = await devices_col.find_one({"uuid": uuid_})

    if not device:
        # ── NEW DEVICE: apply IP guard before creating the record ──────────
        client_ip = request.client.host if request.client else "unknown"

        # Count how many *distinct non-premium* devices are already registered from this IP.
        # Premium devices are excluded so that a premium user's home/office IP does not
        # block other family members from registering free devices.
        non_premium_uuids_on_ip = await ip_guard_col.distinct("uuid", {"ip": client_ip})
        premium_on_ip = await devices_col.count_documents({
            "uuid": {"$in": non_premium_uuids_on_ip},
            "isPremium": True
        })
        existing_count = max(0, len(non_premium_uuids_on_ip) - premium_on_ip)

        if existing_count >= MAX_DEVICES_PER_IP:
            logger.warning(
                f"🚫 IP guard blocked new device {uuid_} from {client_ip} "
                f"({existing_count} devices already registered)"
            )
            # Update the device record with blocked status before raising HTTPException
            block_reason_msg = (
                f"IP Guard auto-block: IP {client_ip} exceeded device limit "
                f"({existing_count} devices already registered, max {MAX_DEVICES_PER_IP})."
            )
            await devices_col.update_one(
                {"uuid": uuid_},
                {
                    "$set": {
                        "uuid": uuid_,
                        "createdAt": now,
                        "lastSeen": now,
                        "lastIp": client_ip,
                        "appVersion": payload.get("appVersion"),
                        "osVersion": payload.get("osVersion"),
                        "deviceModel": payload.get("deviceModel"),
                        "brand": payload.get("brand"),
                        "manufacturer": payload.get("manufacturer"),
                        "isPremium": False,
                        "trialRemaining": 12,
                        "trialUsed": False,
                        "isBlocked": True,  # Set isBlocked to True
                        "blockReason": block_reason_msg, # Set blockReason
                        "blockedAt": now, # Set blockedAt
                    }
                },
                upsert=True
            )
            raise HTTPException(
                status_code=429,
                detail=(
                    f"Too many devices registered from your network "
                    f"(max {MAX_DEVICES_PER_IP}). "
                    "Please contact support if this is a mistake."
                ),
            )

        flags = await get_admin_flags()

        # ── Create the device record ──────────────────────────────────────
        # Use update_one with upsert=True instead of insert_one to avoid a
        # race-condition DuplicateKeyError when two requests arrive at the
        # same time for the same UUID.
        await devices_col.update_one(
            {"uuid": uuid_},
            {
                "$setOnInsert": {
                    "uuid": uuid_,
                    "isPremium": False,
                    "trialRemaining": flags.get("trialSeconds", 300),
                    "createdAt": now,
                },
                "$set": {"lastSeen": now},
            },
            upsert=True,
        )

        # ── Record this IP ↔ UUID mapping (ignore if it already exists) ──
        try:
            await ip_guard_col.insert_one(
                {"ip": client_ip, "uuid": uuid_, "createdAt": now}
            )
        except DuplicateKeyError:
            pass  # mapping already recorded — not an error

    else:
        # ── RETURNING DEVICE ─────────────────────────────────────────────
        # Premium users are never blocked; just touch lastSeen and return.
        is_premium = device.get("isPremium", False)
        client_ip = request.client.host if request.client else "unknown"

        if not is_premium:
            # Ensure the ip_guard record exists for this device (backfill for
            # devices that registered before ip_guard was introduced).
            try:
                await ip_guard_col.update_one(
                    {"ip": client_ip, "uuid": uuid_},
                    {"$setOnInsert": {"ip": client_ip, "uuid": uuid_, "createdAt": now}},
                    upsert=True,
                )
            except DuplicateKeyError:
                pass

        await devices_col.update_one(
            {"uuid": uuid_},
            {"$set": {"lastSeen": now}},
        )

    return {"status": "OK"}

@app.get("/device/status/{uuid}")
async def device_status(uuid: str):
    device = await devices_col.find_one({"uuid": uuid})
    if not device:
        raise HTTPException(404, "Device not found")

    # ✅ FIX: Use centralized expiry logic
    device = await maybe_auto_expire_premium(device)

    # If trial is exhausted, ensure it's 0
    trial_remaining = device.get("trialRemaining", 0)
    if device.get("trialUsed", False) and trial_remaining <= 0:
        trial_remaining = 0

    return {
        "isPremium": device.get("isPremium", False),
        "trialRemaining": trial_remaining
    }

@app.get("/entitlement")
async def get_entitlement(x_device_id: str = Header(None)):
    if not x_device_id:
        raise HTTPException(400, "X-DEVICE-ID header required")
    
    device = await devices_col.find_one({"uuid": x_device_id})
    now = datetime.utcnow()
    flags = await get_admin_flags()

    if not device:
        # Use upsert to avoid DuplicateKeyError if two requests race for the same device
        await devices_col.update_one(
            {"uuid": x_device_id},
            {
                "$setOnInsert": {
                    "uuid": x_device_id,
                    "isPremium": False,
                    "trialRemaining": flags.get("trialSeconds", 300),
                    "trialStartedAt": now,
                    "trialUsed": False,
                    "trialPausedAt": None,
                    "lastChannelId": None,
                    "createdAt": now,
                },
                "$set": {"lastSeen": now},
            },
            upsert=True,
        )
        device = await devices_col.find_one({"uuid": x_device_id})
        if not device:
            # Extremely unlikely fallback
            device = {"uuid": x_device_id, "isPremium": False,
                      "trialRemaining": flags.get("trialSeconds", 300),
                      "trialUsed": False}
    else:
        # ✅ NEW: Check if Premium has expired when checking entitlement
        device = await maybe_auto_expire_premium(device)
    
    # Check if trial was already used (one-time enforcement)
    trial_used = device.get("trialUsed", False)
    trial_remaining = device.get("trialRemaining", 0)
    
    return {
        "isPremium": device.get("isPremium", False),
        "subscriptionExpiresAt": int(device["premiumUntil"].timestamp() * 1000) if device.get("premiumUntil") else None,
        "trialRemainingSeconds": trial_remaining,
        "trialUsed": trial_used,  # Inform app if trial was already used
        "playerMode": flags.get("playerMode", "EXO"),
        "maintenance": flags.get("maintenance", False)
    }

@app.post("/session/start")
async def start_session(payload: dict, request: Request):
    # logger.info("========== /session/start ==========")
    # logger.info(f"Payload received: {payload}")
    # logger.info(f"Client IP: {request.client.host if request.client else 'unknown'}")
    
    uuid_ = payload.get("uuid")
    
    # -----------------------------------------------------------
    # ✅ FIX START: Handle both ID formats & Clean invisible spaces
    # -----------------------------------------------------------
    raw_id = payload.get("channelId") or payload.get("channel_id")
    # If an ID exists, strip whitespace. If not, keep it None.
    channel_id = raw_id.strip() if raw_id else None
    # -----------------------------------------------------------

    if not uuid_:
        raise HTTPException(400, "UUID required")

    device = await devices_col.find_one({"uuid": uuid_})
    if not device:
        raise HTTPException(404, "Device not found")

    # ✅ Ensure /session/start applies auto-expiry BEFORE building the session response
    device = await maybe_auto_expire_premium(device)

    flags = await get_admin_flags()
    if flags.get("maintenance"):
        raise HTTPException(503, "Maintenance")

    # Trial / Premium check
    is_premium = device.get("isPremium", False)
    premium_until = device.get("premiumUntil")
    trial_remaining = device.get("trialRemaining", 0)
    trial_used = device.get("trialUsed", False)
    
    # Generate a fresh token for this session
    token = secrets.token_urlsafe(32)
    expiry = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    # =====================================================
    # 1. MENU MODE (No Channel Selected)
    # =====================================================
    if not channel_id:
        logger.info(f"✅ Menu Mode (No channel ID) for {uuid_}")
        await sessions_col.insert_one({
            "uuid": uuid_,
            "token": token,
            "expiresAt": expiry,
            "active": True,
            "ip": request.client.host if request.client else "unknown",
            "createdAt": datetime.utcnow(),
            "headers": None
        })

        return {
            "token": token,
            "expires_at": int(expiry.timestamp()),
            "player_mode": flags.get("playerMode", "EXO"),
            "trialRemaining": trial_remaining,
            "isPremium": is_premium
        }

    # =====================================================
    # CHANNEL SESSION START
    # =====================================================
    channel_doc = await channels_col.find_one({"id": channel_id})
    if not channel_doc:
        logger.error(f"Channel NOT FOUND in database. ID: {channel_id}")
        raise HTTPException(404, "Channel not found")

    # 🔥 PATCH 2: Force ClearKey for known channels from hardcoded list
    channel_id_int = None
    try:
        channel_id_int = int(channel_id) if isinstance(channel_id, str) and channel_id.isdigit() else None
    except:
        pass

    if channel_id_int and channel_id_int in CLEARKEY_BY_CHANNEL_ID:
        if not channel_doc.get("drm_type") or channel_doc.get("drm_type", "").upper() == "NONE":
            logger.info(f"🔑 Setting ClearKey for channel {channel_id_int} (from hardcoded list)")
            channel_doc["drm_type"] = "CLEARKEY"
            channel_doc["license_url"] = CLEARKEY_BY_CHANNEL_ID[channel_id_int]

    # Use Pydantic model for validation and access
    channel = Channel(**channel_doc)

    # 🔥 TRIAL LOGIC: Only apply trial to PREMIUM channels for non-premium users
    if not is_premium and channel.is_premium:
        # Check if trial was ALREADY used up completely
        if trial_used and trial_remaining <= 0:
            # Trial is exhausted - BLOCK ACCESS
            logger.warning(f"Trial exhausted for device {uuid_} trying to access premium channel {channel_id}")
            raise HTTPException(
                status_code=403,
                detail="Trial exhausted for premium channels. Please upgrade to Premium to continue watching."
            )
        
        # If it's the first time watching a premium channel, mark as used
        if not trial_used:
            logger.info(f"First premium channel access for device {uuid_}, marking trial as used")
            await devices_col.update_one(
                {"uuid": uuid_},
                {"$set": {"trialUsed": True}}
            )
            trial_used = True
    
    # Store the channel being watched (for resume tracking)
    if channel_id:
        await devices_col.update_one(
            {"uuid": uuid_},
            {"$set": {"lastChannelId": channel_id}}
        )

    # Kill old sessions
    await sessions_col.update_many(
        {"uuid": uuid_, "active": True},
        {"$set": {"active": False}}
    )

    # =====================================================
    # Stream Resolution Logic
    # =====================================================
    
    stream_type = None

    # ✅ Alias support (preferred)
    alias_value = (getattr(channel, "alias", None) or "").strip()

    # ✅ Fallback: if someone mistakenly put alias into mpd_url, treat mpd_url as alias
    if not alias_value and _looks_like_alias(getattr(channel, "mpd_url", "")):
        alias_value = getattr(channel, "mpd_url", "").strip()

    if alias_value:
        logger.info(f"🕵️ ALIAS DETECTED: Looking up '{alias_value}' in database...")
        resolved_alias = await resolve_alias_to_stream(db, alias_value)
        
        if not resolved_alias:
            logger.error(f"❌ ALIAS FAILED: Could not resolve '{alias_value}'")
            raise HTTPException(status_code=400, detail=f"Alias not resolved: {alias_value}")

        stream_url = resolved_alias["streamUrl"]
        logger.info(f"✅ ALIAS RESOLVED! Hidden Stream URL: {stream_url}")
        low = stream_url.lower()

        # override mpd/hls based on resolved url
        if ".mpd" in low:
            mpd_url = stream_url
            hls_url = None
            stream_type = "DASH"
        elif ".m3u8" in low:
            hls_url = stream_url
            mpd_url = None
            stream_type = "HLS"
        else:
            raise HTTPException(status_code=400, detail=f"Unknown stream type from alias: {stream_url}")
        
        # ✅ NEW: Apply the DRM keys from the scraper database directly to the channel
        if resolved_alias.get("drmType"):
            channel.drm_type = resolved_alias["drmType"].upper()
            logger.info(f"🔑 Applied Alias DRM Type: {channel.drm_type}")
            
        if resolved_alias.get("licenseUrl"):
            channel.license_url = resolved_alias["licenseUrl"]
            logger.info(f"🔑 Applied Alias License Key: {channel.license_url}")
            
        if resolved_alias.get("headers"):
            if not channel.headers:
                channel.headers = {}
            channel.headers.update(resolved_alias["headers"])

        # ✅ Fallback: Attach ClearKey statically if defined in CLEARKEY_BY_CHANNEL_ID
        if "CLEARKEY_BY_CHANNEL_ID" in globals() and stream_type == "DASH":
            clearkey = CLEARKEY_BY_CHANNEL_ID.get(resolved_alias["channelId"])
            if clearkey and not channel.license_url:
                logger.info(f"🔑 Found Static ClearKey for Alias Channel {resolved_alias['channelId']}")
                channel.drm_type = "CLEARKEY"
                channel.license_url = clearkey
        
        # Update channel object with resolved values for the rest of the handler
        channel.mpd_url = mpd_url
    
    # 🔥 PHP handling (detects when admin saves a .php link)
    resolved = None
    mpd_url = channel.mpd_url

    # ✅ Catch unfinished channels before they crash the player
    if not mpd_url and not alias_value:
        raise HTTPException(status_code=400, detail="This channel is under construction (no stream configured yet).")

    # ✅ Add 'mpd_url and' to prevent NoneType errors
    if mpd_url and (mpd_url.endswith(".php") or ".php?" in mpd_url):
        logger.info("🧩 PHP URL detected, attempting resolution")
        
        # 1. Resolve the stream to get fresh token
        resolved = await resolve_php_player(mpd_url)

        if not resolved:
            # If resolution fails, don't crash, just try sending original (fallback)
            logger.error("❌ Failed to resolve PHP stream")
        else:
            mpd_url = resolved["stream_url"]
            stream_type = resolved["stream_type"]
            
            # 2. Check if we have a Hardcoded Key for this specific MPD file
            # Example: extracts 'AzamTwo.mpd' from the long URL
            try:
                filename = mpd_url.split('?')[0].split('/')[-1]
                logger.info(f"🔎 Checking keys for filename: {filename}")
                
                if filename in PHP_CHANNELS_CLEARKEYS:
                    logger.info(f"🔑 Found Static ClearKey for {filename}")
                    channel.drm_type = "CLEARKEY"
                    channel.license_url = PHP_CHANNELS_CLEARKEYS[filename]
                elif resolved.get("license_url"):
                    # Fallback to the license found by scraper if no static key exists
                    channel.drm_type = "WIDEVINE"
                    channel.license_url = resolved["license_url"]
            except Exception as e:
                logger.error(f"Error matching keys: {e}")

    else:
        stream_type = detect_stream_type(mpd_url)

    # logger.info(f"Detected stream type: {stream_type}")

    if channel.headers is None:
        channel.headers = {}
    
    if channel.user_agent:
        channel.headers["User-Agent"] = channel.user_agent
    if channel.referer:
        channel.headers["Referer"] = channel.referer
    if channel.origin:
        channel.headers["Origin"] = channel.origin
    
    upstream_mpd_url = mpd_url if stream_type == "DASH" else None
    use_clearkey_mpd_proxy = False
    if stream_type == "DASH":
        # 🔥 PATCH 3: Validate ClearKey format if present
        if channel.drm_type and channel.drm_type.upper() == "CLEARKEY":
            license_url = channel.license_url or ""
            
            if ":" not in license_url:
                logger.error(f"❌ ClearKey license_url format invalid: {license_url}")
                logger.error(f"   Expected format: 'kid:key' (hex values)")
                raise HTTPException(400, f"Invalid ClearKey format for channel {channel_id}")
            
            kid_hex, key_hex = license_url.split(":", 1)
            
            # Validate hex values
            try:
                int(kid_hex, 16)
                int(key_hex, 16)
                # logger.info(f"✅ ClearKey format valid for channel {channel_id}")
            except ValueError as e:
                logger.error(f"❌ ClearKey hex values invalid: {e}")
                raise HTTPException(400, f"Invalid ClearKey hex values for channel {channel_id}")
        
        use_clearkey_mpd_proxy = should_use_clearkey_mpd_proxy(upstream_mpd_url, channel.drm_type)
        # logger.info(
        #     f"DASH finalization | upstream_mpd={upstream_mpd_url} | drm_type={channel.drm_type} | clearkey_proxy={use_clearkey_mpd_proxy}"
        # )
        if use_clearkey_mpd_proxy:
            base_url = str(request.base_url).rstrip('/')
            mpd_url = f"{base_url}/api/mpd/clearkey-proxy/{token}"
            # logger.info(f"🔁 Using Android ClearKey MPD proxy: {mpd_url}")

    await sessions_col.insert_one({
        "uuid": uuid_,
        "token": token,
        "expiresAt": expiry,
        "active": True,
        "ip": request.client.host if request.client else "unknown",
        "createdAt": datetime.utcnow(),
        "headers": channel.headers,
        "drm_type": (channel.drm_type or "NONE").lower(),
        "license_url": channel.license_url,
        "upstream_mpd": upstream_mpd_url,
        "resolved_mpd": upstream_mpd_url,
        "stream_type": stream_type,
        "player_mode": flags.get("playerMode", "EXO"),
        "channel_id": channel_id,
        "clearkey_mpd_proxy": use_clearkey_mpd_proxy,
    })

    if stream_type == "HLS":
        # ✅ HLS Proxy Integration: Route through proxy if custom headers are present OR if it's a YCN provider
        # We check is_ycn_provider(mpd_url) to catch cases where admin only pasted the URL
        if channel.user_agent or channel.referer or channel.origin or is_ycn_provider(mpd_url):
            # Use absolute URL for the proxy to ensure the player can reach it
            # request.base_url is safer than hardcoding the domain
            base_url = str(request.base_url).rstrip('/')
            proxy_url = f"{base_url}/api/hls/proxy"
            mpd_url = f"{proxy_url}?url={quote(mpd_url)}"
            logger.info(f"🔗 HLS Proxying enabled for: {channel.name} (YCN or Custom Headers) | Proxy URL: {mpd_url}")

        # ✅ FIXED: Return all required fields with correct names for frontend
        return {
            "mpd_url": mpd_url,                       # ✅ Renamed from stream_url
            "stream_type": "HLS",                             # ✅ Keep for reference
            "license_url": None,                              # ✅ Explicitly null for HLS
            "token": token or "",                            # ✅ Add token (use empty string if None)
            "expires_at": int(expiry.timestamp()),            # ✅ Add expiry timestamp
            "player_mode": flags.get("playerMode", "EXO"),   # ✅ Add player mode
            "drm_type": "NONE",
            "drm_data": None,                                 # 🔥 PATCH 1: Add drm_data for app compatibility
            "headers": channel.headers,
            "trialRemaining": trial_remaining,                # ✅ Add trial remaining
            "channelIsPremium": channel.is_premium            # ✅ CRITICAL: Pass channel premium status
        }

    elif stream_type == "DASH":
        # 🔥 PATCH 4: Improved DASH response with explicit drm_data handling
        # logger.info(f"Building DASH response for channel {channel_id}")
        
        # ✅ NEW FIX: ExoPlayer needs a REAL URL to fetch the keys. 
        # If we have a raw kid:key, convert it into an endpoint URL on our server.
        final_license_url = channel.license_url
        if channel.drm_type and channel.drm_type.upper() == "CLEARKEY" and channel.license_url and ":" in channel.license_url:
            kid, key = channel.license_url.split(":")
            base_url = str(request.base_url).rstrip('/')
            final_license_url = f"{base_url}/api/clearkey/{kid.strip()}/{key.strip()}"
            # logger.info(f"🔗 Generated ClearKey Endpoint for Android: {final_license_url}")

        # Build base response
        response = {
            "mpd_url": mpd_url,                       
            "stream_type": "DASH",                            
            "license_url": final_license_url or "",
            "token": token or "",                            
            "expires_at": int(expiry.timestamp()),            
            "player_mode": flags.get("playerMode", "EXO"),   
            "drm_type": (channel.drm_type or "NONE").upper(),  # Normalize to uppercase
            "headers": channel.headers or {},  # Never null
            "trialRemaining": trial_remaining,                
            "channelIsPremium": channel.is_premium or False  # Never null
        }
        
        # 🔥 PATCH 4: For ClearKey, include drm_data (ALWAYS for app compatibility)
        if channel.drm_type and channel.drm_type.upper() == "CLEARKEY" and channel.license_url:
            response["drm_data"] = build_clearkey_json(channel.license_url)
        else:
            response["drm_data"] = None
        
        return response

    else:
        raise HTTPException(400, "Unsupported stream type")





@app.get("/admin/stats")
async def admin_stats(admin: dict = Depends(get_current_admin)):
    total_users = await devices_col.count_documents({})
    premium_users = await devices_col.count_documents({"isPremium": True})
    total_payments = await payments_col.count_documents({"status": {"$in": ["SUCCESS", "COMPLETED"]}})
    total_channels = await channels_col.count_documents({})
    total_categories = await categories_col.count_documents({})
    
    # Calculate total revenue
    pipeline = [
        {"$match": {"status": {"$in": ["SUCCESS", "COMPLETED"]}}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]
    revenue_result = await payments_col.aggregate(pipeline).to_list(1)
    total_revenue = revenue_result[0]["total"] if revenue_result else 0.0

    # User growth (last 7 days)
    now = datetime.utcnow()
    growth = []
    for i in range(6, -1, -1):
        day_start = (now - timedelta(days=i)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        count = await devices_col.count_documents({"createdAt": {"$gte": day_start, "$lt": day_end}})
        growth.append({"date": day_start.strftime("%Y-%m-%d"), "count": count})

    # Most watched channels (based on lastChannelId in devices)
    channel_pipeline = [
        {"$match": {"lastChannelId": {"$ne": None}}},
        {"$group": {"_id": "$lastChannelId", "watch_count": {"$sum": 1}}},
        {"$sort": {"watch_count": -1}},
        {"$limit": 10}
    ]
    most_watched_raw = await devices_col.aggregate(channel_pipeline).to_list(10)
    
    # Resolve channel names
    most_watched = []
    for item in most_watched_raw:
        ch = await channels_col.find_one({"id": item["_id"]})
        most_watched.append({
            "id": item["_id"],
            "name": ch.get("name") if ch else "Unknown",
            "count": item["watch_count"]
        })

    # Most returning customers (users with most successful payments)
    customer_pipeline = [
        {"$match": {"status": {"$in": ["SUCCESS", "COMPLETED"]}}},
        {"$group": {"_id": "$uuid", "payment_count": {"$sum": 1}, "total_spent": {"$sum": "$amount"}}},
        {"$sort": {"payment_count": -1}},
        {"$limit": 10}
    ]
    top_customers = await payments_col.aggregate(customer_pipeline).to_list(10)

    # Most loved channel (highest ratio of watchers to total users - or just top watched for now)
    # We'll use the top watched as "most loved"
    most_loved = most_watched[0] if most_watched else None

    return {
        "total_users": total_users,
        "premium_users": premium_users,
        "free_users": total_users - premium_users,
        "total_revenue": total_revenue,
        "total_payments": total_payments,
        "total_channels": total_channels,
        "total_categories": total_categories,
        "user_growth": growth,
        "most_watched": most_watched,
        "top_customers": top_customers,
        "most_loved": most_loved,
        "most_watched_channels": most_watched,
        "returning_customers": top_customers
    }

@app.get("/admin/users")
async def admin_users(admin: dict = Depends(get_current_admin)):
    users = await devices_col.find().sort("createdAt", -1).to_list(None)

    # Build uuid→ip lookup from ip_guard so admins can see each device's registered IP
    ip_records = await ip_guard_col.find({}, {"uuid": 1, "ip": 1, "_id": 0}).to_list(None)
    uuid_to_ip: dict = {}
    for rec in ip_records:
        uuid_to_ip.setdefault(rec["uuid"], rec["ip"])

    return [
        {
            "uuid": u.get("uuid"),
            "isPremium": u.get("isPremium", False),
            "trialRemaining": u.get("trialRemaining", 0),
            "premiumUntil": u.get("premiumUntil").isoformat() if u.get("premiumUntil") and hasattr(u.get("premiumUntil"), "isoformat") else u.get("premiumUntil"),
            "createdAt": u.get("createdAt").isoformat() if u.get("createdAt") and hasattr(u.get("createdAt"), "isoformat") else u.get("createdAt"),
            "lastSeen": u.get("lastSeen").isoformat() if u.get("lastSeen") and hasattr(u.get("lastSeen"), "isoformat") else u.get("lastSeen"),
            "is_blocked": u.get("isBlocked", False),
            "device_model": u.get("deviceModel", "Unknown"),
            "os_version": u.get("osVersion", "Unknown"),
            "app_version": u.get("appVersion", "1.0.0"),
            "registered_ip": uuid_to_ip.get(u.get("uuid")),
        }
        for u in users
    ]




# 🔧 PATCH 2 – USE for_admin=True IN ADMIN ENDPOINTS
@app.get("/api/channels")
async def admin_channels(admin: dict = Depends(get_current_admin)):
    channels = await channels_col.find().sort("order", 1).to_list(None)
    return [serialize_doc(c, for_admin=True) for c in channels]

@app.post("/admin/channels/reorder")
async def admin_reorder_channels(reorder_data: List[Dict[str, Any]] = Body(...), admin: dict = Depends(get_current_admin)):
    """
    Persist channel ordering in MongoDB.
    Body format: [{"id": "...", "order": 0}, ...]
    """
    for item in reorder_data:
        channel_id = item.get("id")
        new_order = item.get("order")
        if channel_id is not None and new_order is not None:
            await channels_col.update_one(
                {"id": channel_id},
                {"$set": {"order": int(new_order)}}
            )
    return {"status": "success"}

# 🔧 PATCH 3 – BLOCK INVALID WIDEVINE CHANNELS (CRITICAL)
@app.post("/api/channels")
async def admin_create_channel(channel: Channel, admin: dict = Depends(get_current_admin)):
    # 🔧 PATCH 3 – BLOCK INVALID WIDEVINE CHANNELS (CRITICAL)
    if channel.drm_type == "WIDEVINE":
        has_any_headers = any([
            channel.nv_authorizations,
            channel.nv_tenant_id,
            channel.user_agent,
            channel.referer,
            channel.origin,
            channel.headers
        ])

        # If admin started adding headers, enforce minimum
        if has_any_headers:
            if not (channel.nv_authorizations and channel.nv_tenant_id):
                raise HTTPException(
                    400,
                    "Incomplete Widevine headers: nv_authorizations and nv_tenant_id required together"
                )

    # ✅ FIX 2B: Prevent backend from filling it with junk
    # We explicitly ensure 'token' is None if it's an empty string
    channel_data = channel.dict(by_alias=False)
    
    # Normalize category fields for creation
    if "categoryId" in channel_data and channel_data["categoryId"]:
        channel_data["category_id"] = channel_data["categoryId"]
        del channel_data["categoryId"]

    if channel_data.get("token") == "":
        channel_data["token"] = None

    channel_dict = {
        k: v for k, v in channel_data.items()
        if v not in ("", None)
    }
    await channels_col.insert_one(channel_dict)
    return serialize_doc(channel_dict, for_admin=True)


@app.put("/api/channels/{channel_id}")
async def admin_update_channel(channel_id: str, channel: Channel, admin: dict = Depends(get_current_admin)):
    # Removed strict validation for mpd_url or alias presence

    if channel.drm_type == "WIDEVINE":
        has_any_headers = any([
            channel.nv_authorizations,
            channel.nv_tenant_id,
            channel.user_agent,
            channel.referer,
            channel.origin,
            channel.headers
        ])

        if has_any_headers:
            if not (channel.nv_authorizations and channel.nv_tenant_id):
                raise HTTPException(
                    400,
                    "Incomplete Widevine headers: nv_authorizations and nv_tenant_id required together"
                )

    if channel.drm_type == "CLEARKEY":
        if not is_valid_clearkey(channel.license_url):
            raise HTTPException(
                400,
                "ClearKey license_url must be in format kid:key"
            )

    # Ensure the ID in the body matches the ID in the URL
    channel.id = channel_id

    # ✅ FIX 2B: Prevent backend from filling it with junk
    # We explicitly ensure 'token' is None if it's an empty string
    channel_data = channel.dict(by_alias=False)
    
    # Normalize fields (handle both camelCase and snake_case from incoming request)
    mappings = {
        "categoryId": "category_id",
        "isPremium": "is_premium",
        "logoUrl": "logo_url",
        "mpdUrl": "mpd_url",
        "licenseUrl": "license_url",
        "drmType": "drm_type",
        "isProxied": "is_proxied",
        "sessionBasedDrm": "session_based_drm",
        "drmRobustness": "drm_robustness",
        "nvTenantId": "nv_tenant_id",
        "nvAuthorizations": "nv_authorizations",
        "userAgent": "user_agent",
        "referer": "referer",
        "origin": "origin",
    }
    
    for src, dst in mappings.items():
        if src in channel_data and channel_data[src] is not None:
            channel_data[dst] = channel_data[src]

    if channel_data.get("token") == "":
        channel_data["token"] = None

    # Fields we want to allow updating
    allowed_fields = {
        "name", "category_id", "logo_url", "mpd_url", "license_url", 
        "drm_type", "is_premium", "active", "is_proxied", 
        "session_based_drm", "drm_robustness", "nv_tenant_id", 
        "nv_authorizations", "user_agent", "referer", "origin", "token", "headers", "order", "alias"
    }

    # Always prefer category_id over categoryId for DB persistence
    if "categoryId" in channel_data and channel_data["categoryId"]:
        channel_data["category_id"] = channel_data["categoryId"]

    set_data = {}
    unset_data = {}

    for field in allowed_fields:
        val = channel_data.get(field)
        if val in (None, ""):
            unset_data[field] = ""
        else:
            set_data[field] = val
    
    # Explicitly unset categoryId to ensure normalization to category_id
    unset_data["categoryId"] = ""

    update_op = {}
    if set_data:
        update_op["$set"] = set_data
    if unset_data:
        update_op["$unset"] = unset_data

    if not update_op:
        return {"message": "No changes detected"}

    # ✅ Fix: Either mpd_url or alias must be provided
    existing = await channels_col.find_one({"id": channel_id})
    if not existing:
        raise HTTPException(404, "Channel not found")

    # Determine final mpd_url:
    # 1. If it's in set_data, use that.
    # 2. If it's in unset_data, it's being cleared (None).
    # 3. Otherwise, use existing value.
    if "mpd_url" in set_data:
        final_mpd = set_data["mpd_url"]
    elif "mpd_url" in unset_data:
        final_mpd = None
    else:
        final_mpd = existing.get("mpd_url")

    # Determine final alias:
    if "alias" in set_data:
        final_alias = set_data["alias"]
    elif "alias" in unset_data:
        final_alias = None
    else:
        final_alias = existing.get("alias")

    mpd_str = str(final_mpd or "").strip()
    alias_str = str(final_alias or "").strip()

    if not mpd_str and not alias_str:
        raise HTTPException(status_code=400, detail="Either mpd_url or alias must be provided")

    result = await channels_col.update_one(
        {"id": channel_id},
        update_op
    )
    
    logger.info(f"📝 ADMIN UPDATE CHANNEL | id={channel_id} | matched={result.matched_count} | modified={result.modified_count} | upserted_id={result.upserted_id}")
    
    # Fetch the updated document to return it
    updated_doc = await channels_col.find_one({"id": channel_id})
    return serialize_doc(updated_doc, for_admin=True)

@app.delete("/api/channels/{channel_id}")
async def admin_delete_channel(channel_id: str, admin: dict = Depends(get_current_admin)):
    await channels_col.delete_one({"id": channel_id})
    return {"status": "OK"}

@app.get("/admin/categories")
async def admin_categories(admin: dict = Depends(get_current_admin)):
    categories = await categories_col.find().sort("order", 1).to_list(None)
    return [serialize_doc(c, for_admin=True) for c in categories]

@app.post("/admin/categories")
async def admin_create_category(category: Category, admin: dict = Depends(get_current_admin)):
    category_dict = category.dict()
    await categories_col.update_one(
    {"id": category_dict["id"]},  # Look for existing doc with this ID
    {"$set": category_dict},      # Update it with new data
    upsert=True                   # Create it if it doesn't exist
)
    return serialize_doc(category_dict, for_admin=True)

@app.delete("/admin/categories/{category_id}")
async def admin_delete_category(category_id: str, admin: dict = Depends(get_current_admin)):
    await categories_col.delete_one({"id": category_id})
    return {"status": "OK"}

@app.post("/admin/categories/reorder")
async def admin_reorder_categories(order_data: List[Dict[str, Any]], admin: dict = Depends(get_current_admin)):
    """
    Update the order of multiple categories at once.
    Expects a list of objects with 'id' and 'order'.
    """
    for item in order_data:
        if "id" in item and "order" in item:
            await categories_col.update_one(
                {"id": item["id"]},
                {"$set": {"order": item["order"]}}
            )
    return {"status": "OK"}

@app.get("/admin/banners")
async def admin_banners(admin: dict = Depends(get_current_admin)):
    banners = await banners_col.find().to_list(None)
    return [serialize_doc(b, for_admin=True) for b in banners]

@app.post("/admin/banners")
async def admin_create_banner(banner: Banner, admin: dict = Depends(get_current_admin)):
    banner_dict = banner.dict()
    
    # Ensure 'id' field is used consistently for upsert
    if not banner_dict.get("id"):
        banner_dict["id"] = str(uuid.uuid4())
    
    # POST handles both Create and Update via upsert
    await banners_col.update_one(
        {"id": banner_dict["id"]},
        {"$set": banner_dict},
        upsert=True
    )
    
    # Return the saved document
    saved_doc = await banners_col.find_one({"id": banner_dict["id"]})
    return serialize_doc(saved_doc, for_admin=True)

@app.put("/admin/banners/{banner_id}")
async def admin_update_banner(banner_id: str, banner: Banner, admin: dict = Depends(get_current_admin)):
    banner_dict = banner.dict()
    banner_dict["id"] = banner_id # Force ID consistency
    
    # PUT also uses upsert to prevent 404s if the app sends a new UUID
    result = await banners_col.update_one(
        {"id": banner_id},
        {"$set": banner_dict},
        upsert=True
    )
    
    updated_doc = await banners_col.find_one({"id": banner_id})
    return serialize_doc(updated_doc, for_admin=True)

@app.delete("/admin/banners/{banner_id}")
async def admin_delete_banner(banner_id: str, admin: dict = Depends(get_current_admin)):
    from bson import ObjectId
    
    # 1. Try deleting by custom 'id' field
    result = await banners_col.delete_one({"id": banner_id})
    
    # 2. If not deleted, try deleting by MongoDB '_id'
    if result.deleted_count == 0 and ObjectId.is_valid(banner_id):
        result = await banners_col.delete_one({"_id": ObjectId(banner_id)})
            
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail=f"Banner not found with ID: {banner_id}")
    return {"status": "success"}

from bson import ObjectId

@app.get("/admin/schedules")
async def admin_schedules(admin: dict = Depends(get_current_admin)):
    schedules = await schedules_col.find().sort("startTime", 1).to_list(None)
    return [serialize_doc(s, for_admin=True) for s in schedules]

@app.post("/admin/schedules")
async def admin_create_schedule(schedule: Schedule, admin: dict = Depends(get_current_admin)):
    schedule_dict = schedule.dict()

    # ✅ Ensure we always have a stable "id" (admin panel relies on this)
    if not schedule_dict.get("id"):
        schedule_dict["id"] = str(uuid.uuid4())

    # ✅ Upsert so POST can act like "create/update" safely
    await schedules_col.update_one(
        {"id": schedule_dict["id"]},
        {"$set": schedule_dict},
        upsert=True
    )

    saved = await schedules_col.find_one({"id": schedule_dict["id"]})
    return serialize_doc(saved, for_admin=True)

@app.put("/admin/schedules/{schedule_id}")
async def admin_update_schedule(schedule_id: str, schedule: Schedule, admin: dict = Depends(get_current_admin)):
    schedule_dict = schedule.dict()
    schedule_dict["id"] = schedule_id  # force consistency

    # ✅ Upsert prevents “not found” issues if client uses a new UUID
    await schedules_col.update_one(
        {"id": schedule_id},
        {"$set": schedule_dict},
        upsert=True
    )

    updated = await schedules_col.find_one({"id": schedule_id})
    return serialize_doc(updated, for_admin=True)

@app.delete("/admin/schedules/{schedule_id}")
async def admin_delete_schedule(schedule_id: str, admin: dict = Depends(get_current_admin)):
    # 1) Try delete by your custom "id"
    res = await schedules_col.delete_one({"id": schedule_id})

    # 2) Fallback: if client passed MongoDB _id
    if res.deleted_count == 0 and ObjectId.is_valid(schedule_id):
        res = await schedules_col.delete_one({"_id": ObjectId(schedule_id)})

    if res.deleted_count == 0:
        raise HTTPException(status_code=404, detail=f"Schedule not found: {schedule_id}")

    return {"status": "OK"}

@app.get("/admin/config")
async def admin_get_config(admin: dict = Depends(get_current_admin)):
    config = await config_col.find_one({"name": "global"})

    if not config:
        config = {
            "name": "global",
            "maintenance": False,
            "forceLogout": False,
            "playerMode": "EXO",
            "trialSeconds": 300,
            "packages": [],
            "currency": "TZS",
            "support": {}
        }
        await config_col.insert_one(config)

    return serialize_doc(config, for_admin=True)



@app.put("/admin/config")
async def admin_update_config(config: dict, admin: dict = Depends(get_current_admin)):
    # normalize types (admin panel sometimes sends strings)
    if "trialSeconds" in config:
        try:
            config["trialSeconds"] = int(config["trialSeconds"])
        except Exception:
            raise HTTPException(400, "trialSeconds must be an integer (seconds)")

        # safety bounds (0..24h)
        if config["trialSeconds"] < 0 or config["trialSeconds"] > 86400:
            raise HTTPException(400, "trialSeconds out of allowed range (0..86400)")

    # allow-list config keys to avoid accidental garbage writes
    allowed = {"maintenance", "forceLogout", "playerMode", "trialSeconds", "packages", "currency", "support", "whatsapp_support"}
    cleaned = {k: v for k, v in config.items() if k in allowed}
    
    # 🔧 Compatibility Patch: If admin panel sends whatsapp_support directly, move it to support object
    if "whatsapp_support" in cleaned:
        if "support" not in cleaned or not isinstance(cleaned["support"], dict):
            cleaned["support"] = {}
        cleaned["support"]["whatsapp"] = cleaned["whatsapp_support"]
        # We can keep it or remove it, let's remove to keep DB clean
        del cleaned["whatsapp_support"]

    cleaned["name"] = "global"

    await config_col.update_one(
        {"name": "global"},
        {"$set": cleaned},
        upsert=True
    )

    return {"status": "OK"}



@app.post("/admin/login")
async def admin_login(payload: dict):
    # 1. Print what we actually received (for debugging)
    print(f"DEBUG LOGIN PAYLOAD: {payload}") 

    # 2. Check for 'username' OR 'email'
    raw_username = payload.get("username") or payload.get("email")
    raw_password = payload.get("password")

    # 3. Safety check: handle empty values
    if not raw_username or not raw_password:
        raise HTTPException(401, "Missing username or password")

    # 4. Strip whitespace (fix "admin " vs "admin")
    username = raw_username.strip()
    password = raw_password.strip()
    
    # 5. Compare with Environment Variables
    if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
        token = jwt.encode({
            "sub": username,
            "role": "admin",
            "exp": datetime.utcnow() + timedelta(days=7)
        }, SECRET_KEY, algorithm=ALGORITHM)
        return {"access_token": token, "token_type": "bearer"}
    
    # If we get here, credentials didn't match
    print(f"DEBUG: Login failed. Expected '{ADMIN_USERNAME}', got '{username}'")
    raise HTTPException(401, "Invalid credentials")

@app.get("/api/settings")
async def get_public_settings():
    config = await config_col.find_one({"name": "global"}) or {}
    support = config.get("support", {})

    return {
        "whatsapp": support.get("whatsapp", "https://wa.me/255745100014"),
        "telegram": support.get("telegram", "https://t.me/azammax"),
        "email": support.get("email"),
        "support_url": support.get("support_url"),
        "currency": config.get("currency", "TZS"),
    }



@app.get("/api/categories")
async def get_categories():
    categories = await categories_col.find().sort("order", 1).to_list(None)
    return [serialize_doc(c) for c in categories]

# Redundant endpoint removed in favor of the unified /api/schedules above


@app.get("/api/categories/{category_id}/channels")
async def get_category_channels(category_id: str):
    query = {
        "active": True,
        "$or": [
            {"category_id": category_id},
            {"categoryId": category_id}
        ]
    }
    channels = await channels_col.find(query).sort("order", 1).to_list(None)
    return [serialize_doc(c) for c in channels]

@app.get("/api/channels/public")
async def get_channels(category_id: Optional[str] = None):
    query = {"active": True}
    if category_id:
        # Normalize: Accept both category_id and categoryId by checking both or normalizing query
        # But per requirements, we treat category_id as canonical.
        # If the DB has inconsistent fields, we might need an $or query.
        query["$or"] = [
            {"category_id": category_id},
            {"categoryId": category_id}
        ]
    
    channels = await channels_col.find(query).sort("order", 1).to_list(None)
    return [serialize_doc(c) for c in channels]

@app.get("/api/banners")
async def get_banners():
    banners = await banners_col.find({"active": True}).to_list(None)
    return [serialize_doc(b) for b in banners]

# ✅ Step 3 — Harden /api/relay/stream/{token} for ExoPlayer
from fastapi.responses import StreamingResponse

def modify_mpd_manifest(mpd_xml: str, session_token: str, base_url: str = "https://p01--relayapp--mylcvdlxjz2r.code.run") -> str:
    """
    Modify MPD manifest to replace all license URLs with relay URLs.
    
    Handles multiple formats:
    - <ms:laurl>https://...</ms:laurl>
    - dashif:laurl="https://..."
    - licenseServerUrl="https://..."
    - Any Widevine/DRM license URLs
    """
    try:
        # ✅ FIX (IMPORTANT) - ROOT CAUSE #3: Fix BaseURL / relative URLs
        mpd_xml = re.sub(
            r'<BaseURL>.*?</BaseURL>',
            f'<BaseURL>{base_url}/</BaseURL>',
            mpd_xml,
            flags=re.DOTALL
        )

        # Build relay license URL
        relay_license_url = f"{base_url}/api/relay/license/{session_token}"
        
        # Method 1: Replace ms:laurl tags
        mpd_xml = re.sub(
            r'<ms:laurl[^>]*>.*?</ms:laurl>',
            f'<ms:laurl>{relay_license_url}</ms:laurl>',
            mpd_xml,
            flags=re.DOTALL
        )
        
        # Method 2: Replace dashif:laurl attributes
        mpd_xml = re.sub(
            r'dashif:laurl="[^"]*"',
            f'dashif:laurl="{relay_license_url}"',
            mpd_xml
        )
        
        # Method 3: Replace licenseServerUrl attributes
        mpd_xml = re.sub(
            r'licenseServerUrl="[^"]*"',
            f'licenseServerUrl="{relay_license_url}"',
            mpd_xml
        )
        
        # Method 4: Replace any Widevine/DRM license URLs (aggressive)
        # FIXED: Improved regex to catch Nagra/Azam license URLs specifically
        mpd_xml = re.sub(
            r'https?://[^"<>\s]*(?:wvls|license|drm|nagra|anycast)[^"<>\s]*',
            relay_license_url,
            mpd_xml
        )
        
        logger.info(f"📝 MPD MODIFIED | session_token={session_token} | relay_url={relay_license_url}")
        
        return mpd_xml
        
    except Exception as e:
        logger.error(f"❌ MPD MODIFICATION FAILED | error={e}")
        return mpd_xml  # Fallback to original


# -------------------------------------------------
# HLS Proxy Logic (Integrated)
# -------------------------------------------------

def rewrite_m3u8(content: str, base_url: str, proxy_base: str) -> str:
    """Rewrites M3U8 content to route all links through the proxy."""
    logger.info(f"📝 Rewriting M3U8 | base_url={base_url} | proxy_base={proxy_base}")
    lines = content.splitlines()
    new_lines = []
    for line in lines:
        line = line.strip()
        if not line: continue
        if line.startswith("#"):
            if 'URI="' in line:
                parts = line.split('URI="')
                uri_part = parts[1].split('"')[0]
                full_uri = urljoin(base_url, uri_part)
                proxied_uri = f"{proxy_base}?url={quote(full_uri)}"
                line = line.replace(uri_part, proxied_uri)
            new_lines.append(line)
        else:
            full_url = urljoin(base_url, line)
            new_lines.append(f"{proxy_base}?url={quote(full_url)}")
    logger.info(f"✅ M3U8 Rewritten | lines={len(new_lines)}")
    return "\n".join(new_lines)

@app.get("/api/hls/proxy")
async def hls_proxy(url: str, request: Request, user_agent: Optional[str] = Header(None, alias="User-Agent"), referer: Optional[str] = Header(None, alias="Referer"), origin: Optional[str] = Header(None, alias="Origin")):
    """Proxy endpoint for HLS streams with custom headers."""
    target_url = unquote(url)
    logger.info(f"🔍 HLS Proxy Request | target_url={target_url}")
    
    # Default headers (can be improved by passing a session token)
    
    if is_ycn_provider(target_url):
        logger.info(f"✅ YCN Provider Detected | host={_host(target_url)}")
        # For YCN, we MUST use the preset User-Agent to match the Referer/Origin
        # We only override if the current UA is missing or is the default ExoPlayer UA
        if not user_agent or "ExoPlayer" in user_agent or "ReactNativeVideo" in user_agent:
            user_agent = HET140_PRESET["user_agent"]
            logger.info(f"   -> Forced Preset User-Agent: {user_agent[:50]}...")
        
        if not referer:
            referer = HET140_PRESET["referer"]
            logger.info(f"   -> Applied Preset Referer: {referer}")
        
        if invalid_origin(origin):
            origin = HET140_PRESET["origin"]
            logger.info(f"   -> Applied Preset Origin: {origin}")
    else:
        logger.info(f"ℹ️ Not a YCN Provider | host={_host(target_url)}")

    # normalize referer slash
    if referer and not referer.endswith("/"):
        referer += "/"

    is_playlist = target_url.split("?")[0].lower().endswith(".m3u8")

    headers = {
        "User-Agent": user_agent or DEFAULT_UA,
        "Connection": "keep-alive",
        "Accept": "application/vnd.apple.mpegurl, application/x-mpegURL, */*" if is_playlist else "*/*",
    }
    if referer:
        headers["Referer"] = referer
    if origin:
        headers["Origin"] = origin
    
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        try:
            resp = await client.get(target_url, headers=headers)
            
            if resp.status_code == 403:
                logger.error(f"❌ 403 Forbidden from Provider | target_url={target_url}")
                logger.error(f"   -> Request Headers Sent: {headers}")
                # Log a bit of the response body to see the error message
                body_preview = (await resp.aread())[:200]
                logger.error(f"   -> Response Body Preview: {body_preview}")

            allow = {
                "content-type", "content-length", "accept-ranges", "content-range",
                "cache-control", "expires", "date", "etag", "last-modified",
            }
            passthrough = {k: v for k, v in resp.headers.items() if k.lower() in allow}
            content_type = resp.headers.get("content-type", "").lower()
            
            if "mpegurl" in content_type or target_url.split('?')[0].endswith(".m3u8"):
                # It's a playlist, rewrite it
                # Use absolute URL for proxy_base to ensure rewritten links are reachable
                base_url = str(request.base_url).rstrip('/')
                proxy_base = f"{base_url}/api/hls/proxy"
                rewritten = rewrite_m3u8(resp.text, target_url, proxy_base)
                return Response(
                    content=rewritten,
                    media_type="application/vnd.apple.mpegurl",
                    headers={"Cache-Control": "no-cache, no-store, must-revalidate"}
                )
            else:
                # It's a segment, stream it
                return StreamingResponse(
                    resp.aiter_bytes(),
                    status_code=resp.status_code,
                    media_type=content_type,
                    headers=passthrough
                )
        except Exception as e:
            logger.error(f"HLS Proxy error: {e}")
            raise HTTPException(500, str(e))

def _build_proxy_headers(session: Dict[str, Any], accept: str = "*/*") -> Dict[str, str]:
    headers = {
        "User-Agent": EXO_OR_BROWSER_UA,
        "Accept": accept,
    }
    for k, v in (session.get("headers") or {}).items():
        if v and k.lower() not in ("user-agent", "accept-encoding", "host", "content-length"):
            headers[k] = v
    return headers


def _build_proxy_target(upstream_mpd: str, resource_path: str = "") -> str:
    base = upstream_mpd.rsplit('/', 1)[0] + '/'
    clean_path = (resource_path or '').lstrip('/')
    return urljoin(base, clean_path) if clean_path else upstream_mpd


@app.get("/api/mpd/clearkey-proxy/{token}")
@app.get("/api/mpd/clearkey-proxy/{token}/{resource_path:path}")
async def clearkey_mpd_proxy(token: str, resource_path: str = ""):
    session = await sessions_col.find_one({"token": token, "active": True})
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    if session.get("expiresAt") and session["expiresAt"] < datetime.utcnow():
        logger.warning(f"⏰ ClearKey MPD proxy session expired | token={token}")
        raise HTTPException(status_code=401, detail="Session expired")

    upstream_mpd = session.get("resolved_mpd") or session.get("upstream_mpd")
    if not upstream_mpd:
        raise HTTPException(status_code=400, detail="No upstream MPD configured")

    target_url = _build_proxy_target(upstream_mpd, resource_path)
    is_manifest_request = not resource_path
    accept = "application/dash+xml,application/xml,text/xml,*/*" if is_manifest_request else "*/*"
    headers = _build_proxy_headers(session, accept=accept)

    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        try:
            upstream = await client.get(target_url, headers=headers)
            content = await upstream.aread()
            if upstream.status_code != 200:
                logger.error(
                    f"❌ ClearKey proxy upstream fetch failed | target={target_url} | status={upstream.status_code} | body={content[:300]}"
                )
                raise HTTPException(status_code=upstream.status_code if upstream.status_code in (401, 403, 404) else 502, detail="Upstream proxy fetch failed")

            if is_manifest_request:
                charset = upstream.encoding or 'utf-8'
                mpd_text = content.decode(charset, errors='replace')
                manifest_base_url = f"/api/mpd/clearkey-proxy/{token}/"
                
                # Extract KID from session for explicit injection
                kid_hex = None
                license_url = session.get("license_url")
                if license_url and ":" in license_url:
                    kid_hex = license_url.split(":")[0].strip()
                
                rewritten = rewrite_mpd_for_clearkey_android(mpd_text, manifest_base_url=manifest_base_url, kid_hex=kid_hex)
                return Response(
                    content=rewritten.encode('utf-8'),
                    media_type="application/dash+xml",
                    headers={
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Expose-Headers": "*",
                        "Cache-Control": "no-store, no-cache, must-revalidate",
                        "Pragma": "no-cache",
                        "Expires": "0",
                    },
                )

            content_type = upstream.headers.get("content-type") or "application/octet-stream"
            passthrough_headers = {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Expose-Headers": "*",
                "Cache-Control": upstream.headers.get("cache-control", "no-store"),
                "Accept-Ranges": upstream.headers.get("accept-ranges", "bytes"),
            }
            if upstream.headers.get("content-length"):
                passthrough_headers["Content-Length"] = upstream.headers["content-length"]
            return Response(content=content, media_type=content_type, headers=passthrough_headers)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"❌ ClearKey MPD proxy error | target={target_url} | error={e}")
            raise HTTPException(status_code=500, detail="ClearKey MPD proxy error")


@app.get("/api/relay/stream/{token}")
async def relay_stream(token: str):
    session = await sessions_col.find_one({"token": token, "active": True})
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    # ⚠️ ISSUE #4 — Session expiry enforcement
    if session.get("expiresAt") and session["expiresAt"] < datetime.utcnow():
        logger.warning(f"⏰ Session expired | token={token}")
        raise HTTPException(401, "Session expired")

    session = normalize_stream_caps(session)

    logger.info(
        f"📡 RELAY MPD | token={token} | "
        f"url={session.get('resolved_mpd') or session.get('upstream_mpd')}"
    )

    headers = {
        "User-Agent": EXO_OR_BROWSER_UA,
        "Accept": "*/*",
    }
    
    # ⚠️ ISSUE #5 — Headers duplication / pollution (safe merge)
    for k, v in (session.get("headers") or {}).items():
        if v and k.lower() not in ("user-agent",):
            headers[k] = v

    async with httpx.AsyncClient(timeout=15.0) as client: # Set a hard timeout of 15 seconds
        upstream = await client.get(
            session.get("resolved_mpd") or session.get("upstream_mpd"),
            headers=headers,
            follow_redirects=True
        )

        # ❌ ISSUE #3 — MPD relay validation + logging
        content_type = upstream.headers.get("content-type", "")
        logger.info(
            f"📺 MPD FETCH | token={token} | status={upstream.status_code} | "
            f"content_type={content_type}"
        )

        # ✅ FIX (SAFE + CORRECT) - ROOT CAUSE #2: Relaxed MPD validation
        if upstream.status_code != 200:
            body_preview = (await upstream.aread())[:500]
            logger.error(
                f"❌ INVALID MPD RESPONSE | status={upstream.status_code} | "
                f"content_type={content_type} | body={body_preview}"
            )
            raise HTTPException(502, "Upstream MPD fetch failed")

        # ✅ FIX: Read and modify MPD before returning
        mpd_content = await upstream.aread()
        mpd_text = mpd_content.decode('utf-8')
        
        # Debug logging
        logger.info(f"📄 ORIGINAL MPD SIZE: {len(mpd_text)} bytes")
        
        # Replace license URLs with relay URLs
        modified_mpd = modify_mpd_manifest(mpd_text, token)
        
        logger.info(f"📄 MODIFIED MPD SIZE: {len(modified_mpd)} bytes")
        
        # Verify modification
        if "sphd--thesportshd" in modified_mpd or "azy4sj9b.anycast.nagra.com" in modified_mpd:
            logger.warning(f"⚠️ MPD STILL CONTAINS ORIGINAL LICENSE URLs!")
        else:
            logger.info(f"✅ MPD MODIFICATION SUCCESSFUL - All license URLs replaced")

        return Response(
            content=modified_mpd.encode('utf-8'),
            status_code=upstream.status_code,
            media_type="application/dash+xml",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Expose-Headers": "*",
                "Cache-Control": "no-store, no-cache, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            }
        )

    
    # ✅ PATCH #2 — Make sure relay_license() logs upstream
    logger.warning(
        f"🔐 RELAY LICENSE START | "
        f"token={token} | "
        f"drm={session.get('drm_type') if session else 'N/A'} | "
        f"license_url=Pending..."
    )
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    # ⚠️ ISSUE #4 — Session expiry enforcement
    if session.get("expiresAt") and session["expiresAt"] < datetime.utcnow():
        logger.warning(f"⏰ Session expired | token={token}")
        raise HTTPException(401, "Session expired")

    session = normalize_stream_caps(session)

    # ✅ Step 5 — Allow multiple DRM types (future-proof)
    if session["drm_type"] == "clearkey":
        # Exo handles clearkey locally
        return Response(status_code=204)

    if session["drm_type"] and session["drm_type"] != "widevine":
        # For now, we only proxy widevine. Playready can be added here.
        raise HTTPException(status_code=400, detail=f"DRM type {session['drm_type']} not supported for proxy")

    body = await request.body()

    headers = {
        "Content-Type": "application/octet-stream",
        "User-Agent": EXO_OR_BROWSER_UA,
    }

    # Inject upstream-required DRM headers
    drm_headers = session.get("resolved_headers") or session.get("drm_headers") or {}
    # ⚠️ ISSUE #5 — Headers duplication / pollution (safe merge)
    for k, v in drm_headers.items():
        if v and k.lower() not in ("user-agent",):
            headers[k] = v

    license_url = session.get("resolved_license") or session.get("license_url") or "https://azy4sj9b.anycast.nagra.com/AZY4SJ9B/wvls/contentlicenseservice/v1/licenses"

    # 🛡️ SAFETY CHECK (Recommended)
    # Prevents infinite loops if resolved_license accidentally points back to this relay
    parsed = urllib.parse.urlparse(license_url)
    if parsed.hostname == request.url.hostname:
        logger.error(f"❌ License URL points to relay itself: {license_url}")
        raise HTTPException(500, "Invalid license URL configuration (Infinite Loop)")

    # 🔐 RELAY LICENSE LOG
    logger.warning(
        f"🔐 RELAY LICENSE | token={token} | drm={session['drm_type']} | "
        f"url={license_url}"
    )

    async with httpx.AsyncClient(timeout=15) as client:
        try:
            # 🔧 Before calling upstream POST
            logger.warning(
                f"🎫 DRM LICENSE REQUEST → UPSTREAM | "
                f"url={license_url} | "
                f"headers={dict(headers)} | "
                f"body_size={len(body)}"
            )

            upstream = await client.post(
                license_url,
                content=body,
                headers=headers
            )

            # 🔧 After upstream response
            logger.warning(
                f"🎫 DRM LICENSE RESPONSE ← UPSTREAM | "
                f"status={upstream.status_code} | "
                f"response_size={len(upstream.content)}"
            )

            # 🔧 On non-200 response (CRITICAL)
            if upstream.status_code != 200:
                logger.error(
                    f"❌ DRM LICENSE FAILED | "
                    f"status={upstream.status_code} | "
                    f"body={upstream.text[:500]}"
                )

            return Response(
                content=upstream.content,
                status_code=upstream.status_code,
                media_type="application/octet-stream",
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Expose-Headers": "*",
                }
            )
        except Exception as e:
            logger.error(f"Relay License Error: {e}")
            raise HTTPException(500, "Relay License Server Error")

@app.post("/api/relay/license/{token}")
async def drm_license_relay(token: str, request: Request):
    logger.info(f"🎯 HIT RELAY LICENSE ENDPOINT | token={token}")
    return await relay_license(token, request)

@app.post("/drm/license")
async def drm_license(request: Request):
    """
    Legacy proxy DRM license requests.
    """
    logger.info(f"License request headers received: {dict(request.headers)}")
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        token = request.query_params.get("token", "")
    return await relay_license(token, request)



@app.post("/admin/users/manage")
async def admin_manage_user(
    payload: dict = Body(...),
    admin: dict = Depends(get_current_admin)
):
    uuid_ = payload.get("uuid")

    if not uuid_:
        raise HTTPException(400, "UUID required")

    device = await devices_col.find_one({"uuid": uuid_})
    if not device:
        raise HTTPException(404, "Device not found")

    update = {}

    # Support admin-panel action style: {uuid, action, seconds}
    action = (payload.get("action") or "").upper().strip()

    if action == "UPDATE_TRIAL":
        if "seconds" not in payload:
            raise HTTPException(400, "Missing seconds")
        update["trialRemaining"] = int(payload.get("seconds") or 0)

    elif action == "UPGRADE":
        days = payload.get("days")
        
        try:
            # Convert days to integer, default to 30 if not provided
            days = int(days) if days is not None else 30
        except (TypeError, ValueError):
            raise HTTPException(400, "Days must be a valid integer")

        if days <= 0:
            raise HTTPException(400, "Days must be a positive number")

        now = datetime.utcnow()
        
        # Get current premium end date
        premium_until = device.get("premiumUntil")
        
        # Determine the start date for the new extension
        # If current premium is active (in the future), extend from there.
        # Otherwise, start from now.
        start_date = premium_until if premium_until and premium_until > now else now
        
        new_premium_until = start_date + timedelta(days=days)

        update["isPremium"] = True
        update["premiumUntil"] = new_premium_until
        update["trialRemaining"] = 0
        update["currentPackage"] = f"MANUAL_{days}D"
        update["upgradedAt"] = now

    elif action == "DOWNGRADE":
        update["isPremium"] = False
        update["premiumUntil"] = None
        update["currentPackage"] = None

    elif action in ("BLOCK", "UNBLOCK"):
        update["isBlocked"] = True if action == "BLOCK" else False
        # When unblocking, also clear ip_guard records so the IP slot is freed
        if action == "UNBLOCK":
            await ip_guard_col.delete_many({"uuid": uuid_})

    # Also support direct field updates
    if "trialRemaining" in payload:
        update["trialRemaining"] = int(payload["trialRemaining"])

    if "isPremium" in payload:
        update["isPremium"] = bool(payload["isPremium"])

    if "premiumUntil" in payload:
        update["premiumUntil"] = payload["premiumUntil"]

    if not update:
        raise HTTPException(400, "No valid fields to update")

    await devices_col.update_one(
        {"uuid": uuid_},
        {"$set": update}
    )

    return {"status": "OK"}



@app.api_route("/api/clearkey/{kid_hex}/{key_hex}", methods=["GET", "POST"])
async def clearkey_license_server(kid_hex: str, key_hex: str, request: Request):
    """
    ClearKey license endpoint.

    Note: The Android app primarily uses LocalMediaDrmCallback (keys embedded in drm_data),
    but keeping this endpoint correct helps WebView/Shaka testing and any future clients.
    """
    logger.info(f"🔑 ClearKey license requested | KID: {kid_hex}")
    payload = build_clearkey_json(f"{kid_hex}:{key_hex}")
    # Some players expect 'type' in the JSON (e.g., 'temporary')
    if isinstance(payload, dict) and 'type' not in payload:
        payload['type'] = 'temporary'
    return JSONResponse(
        content=payload,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Expose-Headers": "*",
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
        },
    )
@app.post("/session/heartbeat")
async def session_heartbeat(payload: dict):
    token = payload.get("token")
    uuid_ = payload.get("uuid")
    seconds = payload.get("seconds", 30)
    
    if not token or not uuid_:
        raise HTTPException(400, "Token and UUID required")
    
    session = await sessions_col.find_one({"token": token, "uuid": uuid_, "active": True})
    if not session:
        raise HTTPException(401, "Session expired or invalid")
    
    # Update session expiry
    new_expiry = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    await sessions_col.update_one(
        {"token": token},
        {"$set": {"expiresAt": new_expiry}}
    )
    
    device = await devices_col.find_one({"uuid": uuid_})
    if not device:
        raise HTTPException(404, "Device not found")
    
    # ✅ FIX: Check if Premium has expired MID-STREAM
    is_premium = device.get("isPremium", False)
    premium_until = device.get("premiumUntil")
    now = datetime.utcnow()

    if is_premium:
        if premium_until and premium_until <= now:
            logger.info(f"🚫 Premium expired mid-stream for {uuid_}")

            TRIAL_AFTER_DOWNGRADE = 5

            # ✅ Downgrade immediately + restore trial seconds to DB
            await devices_col.update_one(
                {"uuid": uuid_},
                {"$set": {
                    "isPremium": False,
                    "premiumUntil": None,
                    "currentPackage": None,
                    "trialRemaining": TRIAL_AFTER_DOWNGRADE,
                    "trialUsed": False,
                    "trialPausedAt": None,
                    "lastChannelId": None,
                    "downgradedAt": now
                }}
            )

            # ✅ Return trial seconds so app can continue / show countdown (prevents black screen)
            return {
                "success": True,
                "action": "CONTINUE",
                "isPremium": False,
                "trialRemaining": TRIAL_AFTER_DOWNGRADE,
                "message": "Subscription expired. Trial started."
            }

        # User is valid premium
        return {
            "success": True,
            "action": "CONTINUE",
            "isPremium": True,
            "trialRemaining": 0
        }

    # ✅ (Strongly recommended) Patch 3 — If any free user hits 0, snap back to 5
    if not device.get("isPremium", False):
        remaining = int(device.get("trialRemaining") or 0)
        if remaining <= 0:
            TRIAL_AFTER_DOWNGRADE = 5
            await devices_col.update_one(
                {"uuid": uuid_},
                {"$set": {"trialRemaining": TRIAL_AFTER_DOWNGRADE, "trialUsed": False, "trialPausedAt": None}}
            )
            remaining = TRIAL_AFTER_DOWNGRADE
            # Update device dict so subsequent logic uses the new value
            device["trialRemaining"] = remaining
            device["trialUsed"] = False

    # --- BELOW IS EXISTING FREE/TRIAL LOGIC (UNCHANGED) ---
    
    # We need to know if the current session is for a premium channel
    last_channel_id = device.get("lastChannelId")
    is_watching_premium = False
    
    if last_channel_id:
        channel = await channels_col.find_one({"id": last_channel_id})
        if channel and channel.get("is_premium"):
            is_watching_premium = True
    
    if is_watching_premium:
        trial_remaining = device.get("trialRemaining", 0)
        trial_used = device.get("trialUsed", False)
        
        if trial_used and trial_remaining <= 0:
            return {
                "success": False,
                "action": "EXPIRED",
                "trialRemaining": 0,
                "message": "Trial expired. Please upgrade to Premium."
            }

        new_trial = max(0, trial_remaining - seconds)
        
        await devices_col.update_one(
            {"uuid": uuid_},
            {"$set": {
                "trialRemaining": new_trial,
                "trialPausedAt": datetime.utcnow()
            }}
        )
        
        return {
            "success": True if new_trial > 0 else False,
            "action": "EXPIRED" if new_trial <= 0 else "CONTINUE",
            "trialRemaining": new_trial,
            "message": "Trial expired." if new_trial <= 0 else None
        }
    else:
        # Watching free channel
        return {
            "success": True,
            "action": "CONTINUE",
            "trialRemaining": device.get("trialRemaining", 0)
        }

# ✅ THIS IS THE NEW PART YOU ARE ADDING:
@app.post("/playback/heartbeat")
async def playback_heartbeat_alias(payload: dict):
    """
    🔀 ALIAS: Redirects /playback/heartbeat -> /session/heartbeat
    This ensures the Android app connects successfully.
    """
    return await session_heartbeat(payload)


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║                    ✅ SONICPESA INTEGRATION (PATCHED)                      ║
# ╚══════════════════════════════════════════════════════════════════════════╝

@app.post("/payment/start")
async def start_payment(payload: dict):
    """
    🚀 INITIATE SONICPESA PAYMENT

    Keeps the existing app flow unchanged:
      1. create local pending record
      2. trigger SonicPesa Push USSD
      3. return order_id for polling
    """
    phone = payload.get("phone") or payload.get("phone_number") or ""
    normalized_phone = normalize_phone(phone) if phone else ""
    package_id = payload.get("package")
    uuid_ = payload.get("uuid")
    name = payload.get("name", "pixtvmax User")
    email = payload.get("email", "noemail@pixtv.app")

    if not package_id or not uuid_:
        raise HTTPException(400, "package and uuid are required")

    if not SONICPESA_API_KEY:
        logger.error("❌ SONICPESA_API_KEY not configured!")
        raise HTTPException(500, "Payment system not configured")

    config = await config_col.find_one({"name": "global"})
    if not config:
        raise HTTPException(500, "Server config missing")

    package_doc = next(
        (p for p in config.get("packages", []) if p.get("id") == package_id),
        None
    )
    if not package_doc:
        raise HTTPException(404, "Invalid package")

    amount = int(package_doc.get("price", 0))
    duration_td = _duration_td_from_package_or_payment(package_doc, None)
    duration_seconds = int(duration_td.total_seconds())
    if duration_seconds <= 0:
        raise HTTPException(500, "Package duration misconfigured")

    duration_days = max(1, int((duration_seconds + 86399) // 86400))
    internal_payment_id = str(uuid.uuid4())

    logger.info(f"💳 Starting SonicPesa payment: internal_payment_id={internal_payment_id}, phone={normalized_phone}, amount={amount}")

    try:
        await payments_col.insert_one({
            "internal_payment_id": internal_payment_id,
            "order_id": None,
            "orderId": None,
            "uuid": uuid_,
            "package": package_id,
            "package_name": package_doc.get("name"),
            "duration_days": duration_days,
            "duration_seconds": duration_seconds,
            "durationSeconds": duration_seconds,
            "duration_unit": (package_doc.get("durationUnit") or package_doc.get("duration_unit")),
            "duration_value": (package_doc.get("durationValue") or package_doc.get("duration_value")),
            "phone": phone,
            "buyer_phone": normalized_phone,
            "buyer_name": name,
            "buyer_email": email,
            "amount": amount,
            "currency": "TZS",
            "status": "PENDING",
            "gateway": "SONICPESA",
            "createdAt": datetime.utcnow(),
            "sonic_response": None,
            "webhook_payload": None
        })
    except Exception as e:
        logger.error(f"❌ Database error: {e}")
        raise HTTPException(500, "Failed to create payment record")

    sonic_payload = {
        "buyer_email": email,
        "buyer_name": name,
        "buyer_phone": normalized_phone,
        "amount": amount,
        "currency": "TZS"
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{SONICPESA_BASE_URL}/payment/create_order",
                json=sonic_payload,
                headers={
                    "Content-Type": "application/json",
                    "X-API-KEY": SONICPESA_API_KEY,
                },
            )
    except httpx.TimeoutException:
        logger.error("❌ SonicPesa API timeout")
        await payments_col.update_one(
            {"internal_payment_id": internal_payment_id},
            {"$set": {"status": "FAILED", "error": "Gateway timeout"}}
        )
        raise HTTPException(504, "Payment gateway timeout. Please try again.")
    except Exception as e:
        logger.error(f"❌ SonicPesa API error: {e}")
        await payments_col.update_one(
            {"internal_payment_id": internal_payment_id},
            {"$set": {"status": "FAILED", "error": str(e)}}
        )
        raise HTTPException(502, "Payment gateway error")

    if resp.status_code not in (200, 201, 202):
        error_msg = resp.text
        logger.error(f"❌ SonicPesa rejected request: {error_msg}")
        await payments_col.update_one(
            {"internal_payment_id": internal_payment_id},
            {"$set": {
                "status": "FAILED",
                "sonic_response": error_msg,
                "error": f"Gateway rejected with status {resp.status_code}"
            }}
        )
        raise HTTPException(502, f"Payment gateway rejected request: {error_msg}")

    try:
        sonic_data = resp.json()
        data = sonic_data.get("data") or {}
        sonic_order_id = data.get("order_id")
        reference = data.get("reference")
        gateway_status = extract_sonicpesa_gateway_status(sonic_data) or "PENDING"
        mapped_status = map_sonicpesa_status(gateway_status)

        if not sonic_order_id:
            await payments_col.update_one(
                {"internal_payment_id": internal_payment_id},
                {"$set": {
                    "status": "FAILED",
                    "sonic_response": sonic_data,
                    "error": "Missing order_id in SonicPesa response"
                }}
            )
            raise HTTPException(502, "Invalid response from payment gateway")

        await payments_col.update_one(
            {"internal_payment_id": internal_payment_id},
            {"$set": {
                "order_id": sonic_order_id,
                "orderId": sonic_order_id,
                "reference": reference,
                "gateway_status": gateway_status,
                "status": mapped_status if mapped_status != "COMPLETED" else "PENDING",
                "sonic_response": sonic_data,
                "updatedAt": datetime.utcnow(),
            }}
        )

        logger.info(f"✅ SonicPesa payment initiated successfully: order_id={sonic_order_id}")
        return {
            "order_id": sonic_order_id,
            "reference": reference,
            "status": "PENDING",
            "message": sonic_data.get("message", "USSD request sent. Please confirm on your phone.")
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Failed to parse SonicPesa response: {e}")
        raise HTTPException(502, "Invalid response from payment gateway")

@app.get("/payment/status/{order_id}")
async def payment_status(order_id: str):
    """
    📊 CHECK PAYMENT STATUS
    Keeps the existing frontend contract intact while polling SonicPesa as fallback.
    """
    latest_payment = await find_payment_by_order_id(order_id)
    if not latest_payment:
        raise HTTPException(404, "Payment not found")

    current_status = latest_payment.get("status", "PENDING")

    if current_status == "PENDING" and SONICPESA_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    f"{SONICPESA_BASE_URL}/payment/order_status",
                    json={"order_id": order_id},
                    headers={
                        "Content-Type": "application/json",
                        "X-API-KEY": SONICPESA_API_KEY,
                    },
                )

            if resp.status_code == 200:
                sonic_data = resp.json()
                gateway_status = extract_sonicpesa_gateway_status(sonic_data)
                mapped_status = map_sonicpesa_status(gateway_status)
                data = sonic_data.get("data") if isinstance(sonic_data.get("data"), dict) else {}

                update_data = {
                    "gateway_status": gateway_status or latest_payment.get("gateway_status"),
                    "sonic_status_response": sonic_data,
                    "updatedAt": datetime.utcnow(),
                }
                if data.get("reference"):
                    update_data["reference"] = data.get("reference")
                if data.get("transid"):
                    update_data["transid"] = data.get("transid")
                if data.get("channel"):
                    update_data["channel"] = data.get("channel")

                if mapped_status == "COMPLETED":
                    latest_payment = await complete_payment_and_upgrade_user(
                        latest_payment,
                        gateway_payload=sonic_data,
                        source="status_poll"
                    )
                    current_status = "COMPLETED"
                else:
                    if mapped_status == "FAILED":
                        update_data["status"] = "FAILED"
                        current_status = "FAILED"
                    else:
                        current_status = latest_payment.get("status", "PENDING")
                    await payments_col.update_one({"_id": latest_payment["_id"]}, {"$set": update_data})
            else:
                logger.warning(f"⚠️ SonicPesa order_status returned {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.error(f"⚠️ SonicPesa status polling failed: {e}")

    final_status_for_app = "SUCCESS" if current_status == "COMPLETED" else current_status
    device = await devices_col.find_one({"uuid": latest_payment["uuid"]})

    return {
        "order_id": order_id,
        "status": final_status_for_app,
        "amount": latest_payment.get("amount"),
        "package": latest_payment.get("package_name"),
        "isPremium": device.get("isPremium", False) if device else False,
        "premiumUntil": device.get("premiumUntil").isoformat() if device and device.get("premiumUntil") else None,
        "createdAt": latest_payment.get("createdAt").isoformat() if latest_payment.get("createdAt") else None
    }

@app.post("/payment/manual-upgrade/{order_id}")
async def manual_upgrade_trigger(order_id: str):
    """
    🔧 MANUAL UPGRADE TRIGGER (Fallback if webhook fails)

    Keeps the existing endpoint, but verifies payment with SonicPesa.
    """
    logger.info(f"🔧 Manual SonicPesa upgrade triggered for order_id: {order_id}")

    payment = await find_payment_by_order_id(order_id)
    if not payment:
        raise HTTPException(404, "Payment not found")

    if payment.get("status") == "COMPLETED":
        device = await devices_col.find_one({"uuid": payment["uuid"]})
        return {
            "status": "already_upgraded",
            "isPremium": device.get("isPremium", False) if device else False,
            "message": "User already upgraded"
        }

    if not SONICPESA_API_KEY:
        raise HTTPException(500, "Payment system not configured")

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{SONICPESA_BASE_URL}/payment/order_status",
                json={"order_id": order_id},
                headers={
                    "Content-Type": "application/json",
                    "X-API-KEY": SONICPESA_API_KEY,
                },
            )

        if resp.status_code != 200:
            logger.error(f"❌ SonicPesa status check failed: {resp.status_code} - {resp.text}")
            raise HTTPException(502, "Could not verify payment status")

        sonic_data = resp.json()
        gateway_status = extract_sonicpesa_gateway_status(sonic_data)
        mapped_status = map_sonicpesa_status(gateway_status)

        if mapped_status != "COMPLETED":
            if mapped_status == "FAILED":
                await payments_col.update_one(
                    {"_id": payment["_id"]},
                    {"$set": {
                        "status": "FAILED",
                        "gateway_status": gateway_status,
                        "updatedAt": datetime.utcnow(),
                        "sonic_status_response": sonic_data,
                    }}
                )
            return {
                "status": "not_completed",
                "payment_status": gateway_status or "PENDING",
                "message": "Payment not yet completed"
            }

        payment = await complete_payment_and_upgrade_user(
            payment,
            gateway_payload=sonic_data,
            source="manual_upgrade"
        )

        logger.info(f"✅ Manual upgrade successful for {payment['uuid']}")
        return {
            "status": "upgraded",
            "isPremium": True,
            "premiumUntil": payment.get("premiumUntil").isoformat() if payment.get("premiumUntil") else None,
            "message": "Upgrade successful"
        }
    except HTTPException:
        raise
    except httpx.TimeoutException:
        raise HTTPException(504, "Payment verification timeout")
    except Exception as e:
        logger.error(f"❌ Manual SonicPesa upgrade error: {e}")
        raise HTTPException(500, "Upgrade failed")

# ==========================================
# SonicPesa webhook aliases
# ==========================================
@app.post("/webhooks/sonic")
@app.post("/webhooks/zeno")      # Backward compatibility alias
@app.post("/payment-webhook")    # Keep this for backward compatibility
async def sonicpesa_webhook(request: Request):
    """🔔 SONICPESA WEBHOOK HANDLER"""
    logger.info("=" * 80)
    logger.info("🔔 SONICPESA WEBHOOK RECEIVED - Starting processing")
    logger.info("=" * 80)

    raw_body = await request.body()
    signature = request.headers.get("X-SonicPesa-Signature")

    try:
        payload = json.loads(raw_body.decode("utf-8"))
        logger.info(f"📦 SonicPesa Webhook Payload: {payload}")
    except Exception as e:
        logger.error(f"❌ Could not parse SonicPesa webhook body: {e}")
        return JSONResponse(status_code=400, content={"status": "error", "message": "Invalid JSON payload"})

    if not verify_sonicpesa_signature(raw_body, signature or ""):
        return JSONResponse(status_code=401, content={"status": "error", "message": "Invalid signature"})

    order_id = payload.get("order_id")
    event_name = (payload.get("event") or "").strip().lower()
    gateway_status = extract_sonicpesa_gateway_status(payload)
    mapped_status = map_sonicpesa_status(gateway_status)

    if not order_id:
        logger.warning("⚠️ SonicPesa webhook missing order_id")
        return {"status": "ignored", "reason": "missing order_id"}

    payment = await find_payment_by_order_id(order_id)
    if not payment:
        logger.warning(f"⚠️ Payment not found for SonicPesa order_id: {order_id}")
        return {"status": "ignored", "reason": "order_id not found in DB"}

    if payment.get("status") == "COMPLETED":
        logger.info(f"✅ Payment {order_id} already processed")
        return {"status": "already processed"}

    if mapped_status == "COMPLETED" and event_name == "payment.completed":
        logger.info(f"✅ SonicPesa payment completed: {order_id}")
        await complete_payment_and_upgrade_user(payment, gateway_payload=payload, source="webhook")
        return {"status": "ok"}

    update_data = {
        "webhook_payload": payload,
        "gateway_status": gateway_status or payment.get("gateway_status"),
        "updatedAt": datetime.utcnow(),
    }
    if payload.get("reference"):
        update_data["reference"] = payload.get("reference")
    if payload.get("transid"):
        update_data["transid"] = payload.get("transid")
    if payload.get("channel"):
        update_data["channel"] = payload.get("channel")
    if mapped_status == "FAILED":
        update_data["status"] = "FAILED"

    await payments_col.update_one({"_id": payment["_id"]}, {"$set": update_data})
    return {"status": "ok"}

@app.get("/payment/check-upgrade/{uuid}")
async def check_upgrade_status(uuid: str):
    """
    ✅ CHECK IF DEVICE HAS BEEN UPGRADED (Polled by App)
    This fixes the 404 errors in your logs.
    """
    device = await devices_col.find_one({"uuid": uuid})
    if not device:
        raise HTTPException(404, "Device not found")
    
    # Check if upgrade just happened (within last 15 seconds)
    upgraded_at = device.get("upgradedAt")
    upgrade_ready = False
    
    if upgraded_at:
        # Calculate seconds since upgrade
        time_since = (datetime.utcnow() - upgraded_at).total_seconds()
        # If it happened less than 15 seconds ago, tell app to refresh
        if time_since < 15:
            upgrade_ready = True
    
    return {
        "isPremium": device.get("isPremium", False),
        "premiumUntil": device.get("premiumUntil").isoformat() if device.get("premiumUntil") else None,
        "upgraded": upgraded_at.isoformat() if upgraded_at else None,
        "upgradeReady": upgrade_ready
    }

    


@app.get("/admin/expire-subscriptions")
async def expire_subscriptions(admin: dict = Depends(get_current_admin)):
    """
    ⏰ EXPIRE SUBSCRIPTIONS (Admin/Cron endpoint)
    
    Automatically downgrade users whose premiumUntil has passed.
    Can be called manually or via scheduled cron job.
    """
    now = datetime.utcnow()
    
    # ✅ pull trialSeconds from config (defaults to 5)
    flags = await get_admin_flags()
    trial_seconds = int(flags.get("trialSeconds", 5) or 5)

    result = await devices_col.update_many(
        {"isPremium": True, "premiumUntil": {"$lte": now}},
        {"$set": {
            "isPremium": False,
            "premiumUntil": None,
            "currentPackage": None,
            "trialRemaining": trial_seconds,
            "trialPausedAt": None,
            "lastChannelId": None,
            "downgradedAt": now
        }}
    )

    logger.info(f"⏰ Expired {result.modified_count} subscriptions")

    return {
        "downgraded": result.modified_count,
        "message": "Expired subscriptions processed"
    }


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║                    📊 ADMIN: VIEW PAYMENT HISTORY                          ║
# ╚══════════════════════════════════════════════════════════════════════════╝

@app.get("/admin/users/{uuid}/payments")
async def admin_user_payments(
    uuid: str,
    admin: dict = Depends(get_current_admin)
):
    """
    View payment history for a specific user
    """
    payments = await payments_col.find({"uuid": uuid}).sort("createdAt", -1).to_list(100)
    return [
        {
            "order_id": p.get("order_id") or p.get("orderId"),
            "uuid": p.get("uuid"),
            "phone": p.get("phone"),
            "amount": p.get("amount"),
            "package": p.get("package_name"),
            "status": p.get("status"),
            "reference": p.get("reference"),
            "createdAt": p.get("createdAt").isoformat() if p.get("createdAt") and hasattr(p.get("createdAt"), "isoformat") else None,
            "updatedAt": p.get("updatedAt").isoformat() if p.get("updatedAt") and hasattr(p.get("updatedAt"), "isoformat") else None
        }
        for p in payments
    ]

@app.get("/admin/payments")
async def admin_payments(
    admin: dict = Depends(get_current_admin),
    status: Optional[str] = None,
    limit: int = 100
):
    """
    View payment history with optional filtering and duration-based stats
    """
    query = {}
    if status:
        query["status"] = status.upper()
    
    payments_cursor = payments_col.find(query).sort("createdAt", -1).limit(limit)
    payments = await payments_cursor.to_list(limit)
    
    # Calculate duration-based totals for successful payments
    pipeline = [
        {"$match": {"status": {"$in": ["SUCCESS", "COMPLETED"]}}},
        {"$group": {
            "_id": "$package_name",
            "total_amount": {"$sum": "$amount"},
            "count": {"$sum": 1}
        }}
    ]
    duration_stats = await payments_col.aggregate(pipeline).to_list(None)
    
    stats_map = {
        "DAILY": {"amount": 0.0, "count": 0},
        "WEEKLY": {"amount": 0.0, "count": 0},
        "MONTHLY": {"amount": 0.0, "count": 0}
    }
    
    for stat in duration_stats:
        pkg = stat["_id"]
        if pkg in stats_map:
            stats_map[pkg] = {"amount": stat["total_amount"], "count": stat["count"]}

    return {
        "payments": [
            {
                "order_id": p.get("order_id") or p.get("orderId"),
                "uuid": p.get("uuid"),
                "phone": p.get("phone"),
                "amount": p.get("amount"),
                "package": p.get("package_name"),
                "status": p.get("status"),
                "reference": p.get("reference"),
                "createdAt": p.get("createdAt").isoformat() if p.get("createdAt") else None,
                "updatedAt": p.get("updatedAt").isoformat() if p.get("updatedAt") else None
            }
            for p in payments
        ],
        "duration_stats": stats_map
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
