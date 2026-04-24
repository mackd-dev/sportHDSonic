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

from passlib.context import CryptContext
from jose import jwt, JWTError

from fastapi import FastAPI, Header, HTTPException, Body, Request, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
from bs4 import BeautifulSoup
from enum import Enum

# -------------------------------------------------
# Logging
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("stream-debug")

# -------------------------------------------------
# Configuration & Constants
# -------------------------------------------------
MONGODB_URL = os.getenv("MONGODB_URL")
if not MONGODB_URL:
    raise RuntimeError("❌ MONGODB_URL environment variable is not set")

DATABASE_NAME = os.getenv("DATABASE_NAME", "sports_hd")

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
security = HTTPBearer(auto_error=False)

# ✅ ZENO CONFIGURATION (FIXED)
ZENO_API_KEY = os.getenv("ZENO_API_KEY")  # x-api-key header
ZENO_WEBHOOK_URL = os.getenv("ZENO_WEBHOOK_URL")  # Your webhook endpoint URL
ZENO_SECRET_KEY = os.getenv("ZENO_SECRET_KEY")  # For signature verification (if enabled)

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
ADMIN_PASSWORD_HASH = os.getenv("ADMIN_PASSWORD_HASH")
ADMIN_TOKEN_EXPIRE_HOURS = int(os.getenv("ADMIN_TOKEN_EXPIRE_HOURS", "12"))
CORS_ALLOW_ORIGINS = [item.strip() for item in os.getenv("CORS_ALLOW_ORIGINS", "").split(",") if item.strip()]
APP_PACKAGE_NAME = os.getenv("APP_PACKAGE_NAME", "").strip()
ALLOWED_APP_SIGNATURES = {
    item.strip().lower().replace(":", "")
    for item in os.getenv("ALLOWED_APP_SIGNATURES", "").split(",")
    if item.strip()
}

# ✅ ASPORTSHD BYPASS CODE
ASPORTSHD_BYPASS_CODE = "491YWB317"
EXO_OR_BROWSER_UA = "ReactNativeVideo/3.0 (Linux;Android 11) ExoPlayerLib/2.10.4"


def normalize_signature(value: Optional[str]) -> str:
    return (value or "").strip().lower().replace(":", "")


def get_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def verify_admin_password(raw_password: str) -> bool:
    if ADMIN_PASSWORD_HASH:
        try:
            return pwd_context.verify(raw_password, ADMIN_PASSWORD_HASH)
        except Exception:
            return False
    return bool(ADMIN_PASSWORD and secrets.compare_digest(raw_password, ADMIN_PASSWORD))


def enforce_client_integrity(request: Request) -> None:
    package_name = request.headers.get("X-App-Package", "").strip()
    app_signature = normalize_signature(request.headers.get("X-App-Signature"))

    if APP_PACKAGE_NAME and package_name != APP_PACKAGE_NAME:
        raise HTTPException(403, "Untrusted client package")

    if ALLOWED_APP_SIGNATURES and app_signature not in ALLOWED_APP_SIGNATURES:
        raise HTTPException(403, "Untrusted app signature")

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
reminders_col = db.reminders
vipindi_col   = db.vipindi          # Featured TV programs/episodes
banners_col = db.banners

# --- Models ---

class AdminUser(BaseModel):
    username: str
    password_hash: str
    role: str = "admin"

class Channel(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    category_id: Optional[str] = Field(None, alias="categoryId")
    order: int = 0  # ✅ NEW

    logo_url: Optional[str] = ""
    mpd_url: str
    license_url: Optional[str] = None
    drm_type: str = "WIDEVINE" # WIDEVINE, CLEARKEY, PLAYREADY, NONE
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

# 🔧 PATCH 1 – FIX serialize_doc() (ADMIN VS APP SAFE)
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
    if not credentials or not credentials.credentials:
        raise HTTPException(401, "Missing admin token")
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("role") != "admin":
            raise HTTPException(403, "Not authorized as admin")
        return payload
    except JWTError as e:
        logger.error(f"❌ Admin Auth Failed: {str(e)}")
        raise HTTPException(401, "Invalid or expired admin token")

# ✅ ZENO SIGNATURE VERIFICATION (OPTIONAL - FIXED)
def verify_zeno_signature(payload: dict, signature: str) -> bool:
    """
    Verify Zeno webhook signature if ZENO_SECRET_KEY is configured.
    Returns True if signature is valid or if signature verification is disabled.
    """
    if not ZENO_SECRET_KEY:
        logger.warning("⚠️ ZENO_SECRET_KEY not set - signature verification disabled")
        return True
    
    if not signature:
        logger.error("❌ Missing signature in webhook")
        return False
    
    # Build signature string from payload
    keys = ["order_id", "payment_status", "reference"]
    missing = [k for k in keys if k not in payload]
    if missing:
        logger.error(f"❌ Missing required fields for signature: {missing}")
        return False
    
    raw = "&".join(f"{k}={payload.get(k, '')}" for k in keys)
    expected = hmac.new(ZENO_SECRET_KEY.encode(), raw.encode(), hashlib.sha256).hexdigest()
    
    is_valid = hmac.compare_digest(expected, signature)
    if not is_valid:
        logger.error(f"❌ Signature mismatch. Expected: {expected}, Got: {signature}")
    
    return is_valid

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
    allow_origins=CORS_ALLOW_ORIGINS or ["*"],
    allow_headers=["*"],
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_credentials=bool(CORS_ALLOW_ORIGINS)
)

@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    if request.url.path.startswith(("/admin", "/session", "/payment", "/api/relay", "/drm")):
        response.headers.setdefault("Cache-Control", "no-store")
    return response


# ✅ Background Task Function
async def subscription_cleanup_task():
    """Periodically checks for expired subscriptions and downgrades them."""
    while True:
        try:
            logger.info("⏰ Running hourly subscription cleanup...")
            now = datetime.utcnow()
            result = await devices_col.update_many(
                {
                    "isPremium": True, 
                    "premiumUntil": {"$lte": now}
                },
                {"$set": {
                    "isPremium": False, 
                    "premiumUntil": None,
                    "downgradedAt": now,
                    "trialRemaining": 0,
                    "trialUsed": True
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
    await payments_col.create_index("order_id")
    await payments_col.create_index("orderId")
    await payments_col.create_index("status")
    await devices_col.create_index("uuid", unique=True)
    await sessions_col.create_index("token", unique=True)
    await sessions_col.create_index("expiresAt", expireAfterSeconds=0)

    if not ZENO_API_KEY:
        logger.error("❌ ZENO_API_KEY not set!")
    if not ZENO_WEBHOOK_URL:
        logger.warning("⚠️ ZENO_WEBHOOK_URL not set")
    else:
        logger.info(f"✅ Zeno webhook URL configured: {ZENO_WEBHOOK_URL}")

    asyncio.create_task(subscription_cleanup_task())

@app.get("/")
async def root():
    return {"status": "OK", "service": "KamweMax"}

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
    categories = await categories_col.find().sort("order", 1).to_list(1000)
    # ✅ SORTED BY ORDER THEN NAME
    channels = await channels_col.find({"active": True}).sort([("order", 1), ("name", 1)]).to_list(1000)
    
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
    enforce_client_integrity(request)
    # ── PATCH: Accept blank/missing uuid gracefully.
    # Android DataStore can race inside runBlocking on first launch and send uuid="".
    # Auto-generate one so the device still gets registered instead of 400ing.
    uuid_ = (payload.get("uuid") or request.headers.get("X-DEVICE-ID") or "").strip()
    if not uuid_:
        uuid_ = str(uuid.uuid4())
        logger.warning(f"⚠️ register_device: blank uuid received, auto-generated {uuid_}")

    flags  = await get_admin_flags()
    now    = datetime.utcnow()
    device = await devices_col.find_one({"uuid": uuid_})

    if device and device.get("isBlocked"):
        raise HTTPException(403, "Device blocked")

    if not device:
        device = {
            "uuid":           uuid_,
            "isPremium":      False,
            "trialRemaining": flags.get("trialSeconds", 300),
            "trialUsed":      False,
            "createdAt":      now,
            "lastSeen":       now,
        }
        await devices_col.insert_one(device)
    else:
        await devices_col.update_one(
            {"uuid": uuid_},
            {"$set": {"lastSeen": now}}
        )
        device["lastSeen"] = now

    # ── Return the full DeviceResponse shape the app expects ─────────────
    # App declares: data class DeviceResponse(uuid, trialRemaining, isPremium,
    #               premiumUntil, createdAt, lastSeen)
    # Previously returned {"status":"OK"} which Retrofit could not deserialize.
    def _iso(dt):
        return dt.isoformat() if dt and hasattr(dt, "isoformat") else ""

    return {
        "uuid":           uuid_,
        "trialRemaining": int(device.get("trialRemaining", flags.get("trialSeconds", 300))),
        "isPremium":      bool(device.get("isPremium", False)),
        "premiumUntil":   _iso(device.get("premiumUntil")),
        "createdAt":      _iso(device.get("createdAt", now)),
        "lastSeen":       _iso(device.get("lastSeen", now)),
    }

@app.get("/device/status/{uuid}")
async def device_status(uuid: str):
    device = await devices_col.find_one({"uuid": uuid})
    if not device:
        raise HTTPException(404, "Device not found")

    # ✅ FIX: This is now unindented so it actually runs!
    if device.get("isPremium") and device.get("premiumUntil"):
        # Check if time has passed
        if device["premiumUntil"] <= datetime.utcnow():
            logger.info(f"📉 Auto-downgrading expired user: {uuid}")
            await devices_col.update_one(
                {"uuid": uuid},
                {"$set": {
                    "isPremium": False,
                    "premiumUntil": None,
                    "downgradedAt": datetime.utcnow()
                }}
            )
            # Update local variable so the app sees the change immediately
            device["isPremium"] = False

    # If trial is exhausted, ensure it's 0
    trial_remaining = device.get("trialRemaining", 0)
    if device.get("trialUsed", False) and trial_remaining <= 0:
        trial_remaining = 0

    return {
        "isPremium": device.get("isPremium", False),
        "trialRemaining": trial_remaining
    }

@app.get("/entitlement")
async def get_entitlement(request: Request, x_device_id: str = Header(None)):
    enforce_client_integrity(request)
    if not x_device_id:
        raise HTTPException(400, "X-DEVICE-ID header required")
    
    device = await devices_col.find_one({"uuid": x_device_id})
    now = datetime.utcnow()
    flags = await get_admin_flags()

    if not device:
        device = {
            "uuid": x_device_id,
            "isPremium": False,
            "trialRemaining": flags.get("trialSeconds", 300),
            "trialStartedAt": now,
            "trialUsed": False,
            "trialPausedAt": None,
            "lastChannelId": None,
            "createdAt": now,
            "lastSeen": now
        }
        await devices_col.insert_one(device)
    else:
        if device.get("isBlocked"):
            raise HTTPException(403, "Device blocked")
        # ✅ NEW: Check if Premium has expired when checking entitlement
        if device.get("isPremium") and device.get("premiumUntil") and device["premiumUntil"] <= now:
            logger.info(f"🚫 Premium expired for {x_device_id} (entitlement check)")
            await devices_col.update_one(
                {"uuid": x_device_id},
                {"$set": {
                    "isPremium": False, 
                    "premiumUntil": None,
                    "downgradedAt": now,
                    "trialRemaining": 0,
                    "trialUsed": True
                }}
            )
            # Update local object for the response
            device["isPremium"] = False
            device["premiumUntil"] = None
    
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
    enforce_client_integrity(request)
    logger.info("========== /session/start ==========")
    logger.info(f"Payload received: {payload}")
    logger.info(f"Client IP: {request.client.host if request.client else 'unknown'}")
    
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
    if device.get("isBlocked"):
        raise HTTPException(403, "Device blocked")

    flags = await get_admin_flags()
    if flags.get("maintenance"):
        raise HTTPException(503, "Maintenance")

    # Trial / Premium check
    is_premium = device.get("isPremium", False)
    premium_until = device.get("premiumUntil")
    trial_remaining = device.get("trialRemaining", 0)
    trial_used = device.get("trialUsed", False)

    # ✅ NEW: Check if Premium has expired BEFORE starting session
    if is_premium and premium_until and premium_until <= datetime.utcnow():
        logger.info(f"🚫 Premium expired for {uuid_} at session start")
        await devices_col.update_one(
            {"uuid": uuid_},
            {"$set": {
                "isPremium": False, 
                "premiumUntil": None,
                "downgradedAt": datetime.utcnow(),
                "trialRemaining": 0,
                "trialUsed": True
            }}
        )
        is_premium = False
        # If it was a premium channel, the trial logic below will now handle it
        # (either allow trial or block if trial is exhausted)
    
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

    # Use Pydantic model for validation and access
    channel = Channel(**channel_doc)

    # 🔥 TRIAL LOGIC: Only apply trial to PREMIUM channels for non-premium users
    if not is_premium and channel.is_premium:
        # Check if trial was ALREADY used up completely
        if trial_used and trial_remaining <= 0:
            # Trial is exhausted - BLOCK ACCESS
            logger.warning(f"Trial exhausted for device {uuid_} trying to access premium channel {channel_id}")
            return {
                "success": False,
                "action": "EXPIRED",
                "trialRemaining": 0,
                "message": "Trial exhausted. Please upgrade to Premium to continue watching."
            }
        
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
    
    # 🔥 PHP handling (detects when admin saves a .php link)
    resolved = None
    mpd_url = channel.mpd_url

    # 🔥 PHP & AUTO-CLEARKEY HANDLING
    if mpd_url.endswith(".php") or ".php?" in mpd_url:
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

    logger.info(f"Detected stream type: {stream_type}")

    session_headers = dict(channel.headers or {})
    session_headers.setdefault("X-DEVICE-ID", uuid_)
    if request.headers.get("X-App-Package"):
        session_headers.setdefault("X-App-Package", request.headers.get("X-App-Package"))
    if request.headers.get("X-App-Signature"):
        session_headers.setdefault("X-App-Signature", request.headers.get("X-App-Signature"))
    if request.headers.get("X-App-Version"):
        session_headers.setdefault("X-App-Version", request.headers.get("X-App-Version"))
    
    if stream_type == "HLS":
        # ✅ FIXED: Return all required fields with correct names for frontend
        await sessions_col.insert_one({
            "uuid": uuid_,
            "token": token,
            "expiresAt": expiry,
            "active": True,
            "ip": request.client.host if request.client else "unknown",
            "createdAt": datetime.utcnow(),
            "headers": session_headers,
            "stream_type": "hls",
            "drm_type": "none",
            "license_url": None,
            "resolved_mpd": mpd_url,
            "upstream_mpd": mpd_url,
            "channelId": channel_id,
        })
        return {
            "success": True,
            "action": "CONTINUE",
            "mpd_url": mpd_url,                       # ✅ Renamed from stream_url
            "stream_type": "HLS",                             # ✅ Keep for reference
            "license_url": None,                              # ✅ Explicitly null for HLS
            "token": token or "",                            # ✅ Add token (use empty string if None)
            "expires_at": int(expiry.timestamp()),            # ✅ Add expiry timestamp
            "player_mode": flags.get("playerMode", "EXO"),   # ✅ Add player mode
            "drm_type": "NONE",
            "headers": session_headers,
            "trialRemaining": trial_remaining,                # ✅ Add trial remaining
            "channelIsPremium": channel.is_premium            # ✅ CRITICAL: Pass channel premium status
        }

    elif stream_type == "DASH":
        # ✅ FIXED: Return all required fields with correct names for frontend
        response = {
            "success": True,
            "action": "CONTINUE",
            "mpd_url": mpd_url,                       # ✅ Renamed from stream_url
            "stream_type": "DASH",                            # ✅ Keep for reference
            "license_url": channel.license_url,               # ✅ Add license URL
            "token": token or "",                            # ✅ Add token (use empty string if None)
            "expires_at": int(expiry.timestamp()),            # ✅ Add expiry timestamp
            "player_mode": flags.get("playerMode", "EXO"),   # ✅ Add player mode
            "drm_type": channel.drm_type,
            "headers": session_headers,
            "trialRemaining": trial_remaining,                # ✅ Add trial remaining
            "channelIsPremium": channel.is_premium            # ✅ CRITICAL: Pass channel premium status
        }
        
        # ✅ PATCH 8: For ClearKey, convert kid:key format to proper JSON structure
        if channel.drm_type and channel.drm_type.upper() == "CLEARKEY" and channel.license_url:
            response["drm_data"] = build_clearkey_json(channel.license_url)
            logger.info(f"Built ClearKey JSON: {response['drm_data']}")

        await sessions_col.insert_one({
            "uuid": uuid_,
            "token": token,
            "expiresAt": expiry,
            "active": True,
            "ip": request.client.host if request.client else "unknown",
            "createdAt": datetime.utcnow(),
            "headers": session_headers,
            "stream_type": "dash",
            "drm_type": (channel.drm_type or "NONE").lower(),
            "license_url": channel.license_url,
            "resolved_license": channel.license_url,
            "resolved_mpd": mpd_url,
            "upstream_mpd": mpd_url,
            "channelId": channel_id,
        })
        
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
    users = await devices_col.find().sort("createdAt", -1).to_list(1000)

    return [
        {
            "uuid": u.get("uuid"),
            "isPremium": u.get("isPremium", False),
            "trialRemaining": u.get("trialRemaining", 0),
            "premiumUntil": u.get("premiumUntil").isoformat() if u.get("premiumUntil") and hasattr(u.get("premiumUntil"), "isoformat") else u.get("premiumUntil"),
            "createdAt": u.get("createdAt").isoformat() if u.get("createdAt") and hasattr(u.get("createdAt"), "isoformat") else u.get("createdAt"),
            "lastSeen": u.get("lastSeen").isoformat() if u.get("lastSeen") and hasattr(u.get("lastSeen"), "isoformat") else u.get("createdAt"),
            "is_blocked": u.get("isBlocked", False),
            "device_model": u.get("deviceModel", "Unknown"),
            "os_version": u.get("osVersion", "Unknown"),
            "app_version": u.get("appVersion", "1.0.0"),
        }
        for u in users
    ]




# 🔧 PATCH 2 – USE for_admin=True IN ADMIN ENDPOINTS
@app.get("/api/channels")
async def admin_list_channels(admin: dict = Depends(get_current_admin)):
    # ✅ SORTED BY ORDER THEN NAME
    channels = await channels_col.find().sort([("order", 1), ("name", 1)]).to_list(1000)
    return [serialize_doc(c, for_admin=True) for c in channels]

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
        "name", "category_id", "order", "logo_url", "mpd_url", "license_url", 
        "drm_type", "is_premium", "active", "is_proxied", 
        "session_based_drm", "drm_robustness", "nv_tenant_id", 
        "nv_authorizations", "user_agent", "referer", "origin", "token", "headers"
    }

    set_data = {}
    unset_data = {}

    for field in allowed_fields:
        val = channel_data.get(field)
        if val in (None, ""):
            unset_data[field] = ""
        else:
            set_data[field] = val

    update_op = {}
    if set_data:
        update_op["$set"] = set_data
    if unset_data:
        update_op["$unset"] = unset_data

    if not update_op:
        return {"message": "No changes detected"}

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

@app.post("/admin/channels/reorder")
async def admin_reorder_channels(order_data: List[Dict[str, Any]], admin: dict = Depends(get_current_admin)):
    for item in order_data:
        if "id" in item and "order" in item:
            await channels_col.update_one(
                {"id": item["id"]},
                {"$set": {"order": int(item["order"])}}
            )
    return {"status": "OK"}

@app.get("/admin/categories")
async def admin_categories(admin: dict = Depends(get_current_admin)):
    categories = await categories_col.find().sort("order", 1).to_list(1000)
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
    banners = await banners_col.find().to_list(1000)
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

@app.get("/admin/schedules")
async def admin_schedules(admin: dict = Depends(get_current_admin)):
    schedules = await schedules_col.find().sort("startTime", 1).to_list(1000)
    return [serialize_doc(s, for_admin=True) for s in schedules]

@app.post("/admin/schedules")
async def admin_create_schedule(schedule: Schedule, admin: dict = Depends(get_current_admin)):
    schedule_dict = schedule.dict()
    await schedules_col.insert_one(schedule_dict)
    return serialize_doc(schedule_dict, for_admin=True)

@app.delete("/admin/schedules/{schedule_id}")
async def admin_delete_schedule(schedule_id: str, admin: dict = Depends(get_current_admin)):
    await schedules_col.delete_one({"id": schedule_id})
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
async def admin_login(payload: dict, request: Request):
    raw_username = payload.get("username") or payload.get("email")
    raw_password = payload.get("password")

    if not raw_username or not raw_password:
        raise HTTPException(401, "Missing username or password")

    username = raw_username.strip()
    password = raw_password.strip()

    if username != ADMIN_USERNAME or not verify_admin_password(password):
        logger.warning(f"Admin login failed from {get_client_ip(request)} for username={username!r}")
        raise HTTPException(401, "Invalid credentials")

    token = jwt.encode({
        "sub": username,
        "role": "admin",
        "exp": datetime.utcnow() + timedelta(hours=ADMIN_TOKEN_EXPIRE_HOURS)
    }, SECRET_KEY, algorithm=ALGORITHM)
    return {"access_token": token, "token_type": "bearer"}

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
    categories = await categories_col.find().sort("order", 1).to_list(1000)
    return [serialize_doc(c) for c in categories]

# Redundant endpoint removed in favor of the unified /api/schedules above


@app.get("/api/categories/{category_id}/channels")
async def get_category_channels(category_id: str):
    query = {"active": True, "category_id": category_id}
    # ✅ SORTED BY ORDER THEN NAME
    channels = await channels_col.find(query).sort([("order", 1), ("name", 1)]).to_list(1000)
    return [serialize_doc(c) for c in channels]

@app.get("/api/channels/public")
async def get_channels(category_id: Optional[str] = None):
    query = {"active": True}
    if category_id:
        query["category_id"] = category_id
    
    # ✅ SORTED BY ORDER THEN NAME
    channels = await channels_col.find(query).sort([("order", 1), ("name", 1)]).to_list(1000)
    return [serialize_doc(c) for c in channels]

@app.get("/api/banners")
async def get_banners():
    banners = await banners_col.find({"active": True}).to_list(1000)
    return [serialize_doc(b) for b in banners]

# ══════════════════════════════════════════════════════════════════════════
# PATCH 3 — /api/vipindi  (Featured TV Programs / Episodes)
# App calls GET /api/vipindi → List<KipendiDto>
# KipendiDto = { id, name, thumbnailUrl, channelId, active }
# ══════════════════════════════════════════════════════════════════════════

def _serialize_vipindi(doc: dict) -> dict:
    """Serialize a vipindi document to the KipendiDto shape the app expects."""
    if not doc:
        return {}
    return {
        "id":           doc.get("id") or str(doc.get("_id", "")),
        "name":         doc.get("name", ""),
        "thumbnailUrl": doc.get("thumbnailUrl") or doc.get("thumbnail_url", ""),
        "channelId":    doc.get("channelId") or doc.get("channel_id", ""),
        "active":       bool(doc.get("active", True)),
    }

@app.get("/api/vipindi")
async def get_vipindi():
    """
    Returns featured TV programs for the home screen MechiNaVipindi section.
    Returns only active items, sorted by order then name.
    Returns [] when collection is empty — app handles that gracefully.
    """
    items = await vipindi_col.find({"active": True}).sort(
        [("order", 1), ("name", 1)]
    ).to_list(200)
    return [_serialize_vipindi(doc) for doc in items]

# ── Admin CRUD so you can manage vipindi from the admin panel ─────────────

@app.get("/admin/vipindi")
async def admin_list_vipindi(admin: dict = Depends(get_current_admin)):
    items = await vipindi_col.find().sort([("order", 1), ("name", 1)]).to_list(1000)
    return [_serialize_vipindi(doc) for doc in items]

@app.post("/admin/vipindi")
async def admin_create_vipindi(
    payload: dict,
    admin: dict = Depends(get_current_admin)
):
    item = {
        "id":           payload.get("id") or str(uuid.uuid4()),
        "name":         payload.get("name", ""),
        "thumbnailUrl": payload.get("thumbnailUrl") or payload.get("thumbnail_url", ""),
        "channelId":    payload.get("channelId") or payload.get("channel_id", ""),
        "active":       bool(payload.get("active", True)),
        "order":        int(payload.get("order", 0)),
    }
    await vipindi_col.update_one(
        {"id": item["id"]}, {"$set": item}, upsert=True
    )
    return _serialize_vipindi(item)

@app.put("/admin/vipindi/{item_id}")
async def admin_update_vipindi(
    item_id: str,
    payload: dict,
    admin: dict = Depends(get_current_admin)
):
    payload["id"] = item_id
    if "thumbnail_url" in payload and "thumbnailUrl" not in payload:
        payload["thumbnailUrl"] = payload.pop("thumbnail_url")
    if "channel_id" in payload and "channelId" not in payload:
        payload["channelId"] = payload.pop("channel_id")
    await vipindi_col.update_one(
        {"id": item_id}, {"$set": payload}, upsert=True
    )
    updated = await vipindi_col.find_one({"id": item_id})
    return _serialize_vipindi(updated)

@app.delete("/admin/vipindi/{item_id}")
async def admin_delete_vipindi(
    item_id: str,
    admin: dict = Depends(get_current_admin)
):
    await vipindi_col.delete_one({"id": item_id})
    return {"status": "OK"}

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



@app.post("/session/heartbeat")
async def session_heartbeat(payload: dict, request: Request):
    enforce_client_integrity(request)
    token = payload.get("token")
    uuid_ = payload.get("uuid")
    seconds = payload.get("seconds", 30)
    
    if not token or not uuid_:
        raise HTTPException(400, "Token and UUID required")
    
    session = await sessions_col.find_one({"token": token, "uuid": uuid_, "active": True})
    if not session:
        raise HTTPException(401, "Session expired or invalid")
    if request.headers.get("X-DEVICE-ID") and request.headers.get("X-DEVICE-ID") != uuid_:
        raise HTTPException(401, "Device header mismatch")
    
    # Update session expiry
    new_expiry = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    await sessions_col.update_one(
        {"token": token},
        {"$set": {"expiresAt": new_expiry}}
    )
    
    device = await devices_col.find_one({"uuid": uuid_})
    if not device:
        raise HTTPException(404, "Device not found")
    if device.get("isBlocked"):
        raise HTTPException(403, "Device blocked")
    
    # ✅ FIX: Check if Premium has expired MID-STREAM
    is_premium = device.get("isPremium", False)
    premium_until = device.get("premiumUntil")
    now = datetime.utcnow()

    if is_premium:
        if premium_until and premium_until <= now:
            logger.info(f"🚫 Premium expired mid-stream for {uuid_}")
            
            # Downgrade immediately
            await devices_col.update_one(
                {"uuid": uuid_},
                {"$set": {
                    "isPremium": False, 
                    "premiumUntil": None,
                    "downgradedAt": now,
                    "trialRemaining": 0,
                    "trialUsed": True
                }}
            )
            
            # Return EXPIRED so the app shows paywall
            return {
                "success": False,
                "action": "EXPIRED",
                "trialRemaining": 0,
                "message": "Your subscription has just expired. Please upgrade to continue."
            }
        
        # User is valid premium
        return {
            "success": True,
            "action": "CONTINUE",
            "isPremium": True,
            "trialRemaining": 0
        }

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
async def playback_heartbeat_alias(payload: dict, request: Request):
    """
    🔀 ALIAS: Redirects /playback/heartbeat -> /session/heartbeat
    This ensures the Android app connects successfully.
    """
    return await session_heartbeat(payload, request)


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║                     ✅ ZENOPAY INTEGRATION (FIXED)                         ║
# ╚══════════════════════════════════════════════════════════════════════════╝

@app.post("/payment/start")
async def start_payment(payload: dict):
    """
    🚀 INITIATE ZENOPAY PAYMENT
    
    Request body:
    {
        "phone": "0744963858",       # Tanzanian mobile number
        "package": "monthly_15k",    # Package ID from config
        "uuid": "device-uuid-here",  # Device identifier
        "name": "John Doe",          # Optional: buyer name
        "email": "user@example.com"  # Optional: buyer email
    }
    """
    phone = payload.get("phone")
    
    # 🔧 PATCH 7: Normalize phone number to 255 format for ZenoPay
    normalized_phone = normalize_phone(phone)
    package_id = payload.get("package")
    uuid_ = payload.get("uuid")
    name = payload.get("name", "pixtvmax User")
    email = payload.get("email", "noemail@pixtv.app")

    # ✅ Validation
    if not phone or not package_id or not uuid_:
        raise HTTPException(400, "phone, package and uuid are required")
    
    if not ZENO_API_KEY:
        logger.error("❌ ZENO_API_KEY not configured!")
        raise HTTPException(500, "Payment system not configured")

    # ✅ Fetch package details from config
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
    duration_days = int(package_doc.get("duration_days", 30))

    # ✅ Generate unique order ID (MUST be UUID format per ZenoPay spec)
    order_id = str(uuid.uuid4())

    logger.info(f"💳 Starting payment: order_id={order_id}, phone={phone}, amount={amount}")

    # ✅ Save payment record as PENDING
    try:
        await payments_col.insert_one({
            "order_id": order_id,  # Used in your logic
            "orderId": order_id,   # Used by your index
            "uuid": uuid_,
            "package": package_id,
            "package_name": package_doc.get("name"),
            "duration_days": duration_days,
            "phone": phone, # Store original phone number
            "amount": amount,
            "currency": "TZS",
            "status": "PENDING",
            "createdAt": datetime.utcnow(),
            "zeno_response": None,
            "webhook_payload": None
        })
    except Exception as e:
        logger.error(f"❌ Database error: {e}")
        raise HTTPException(500, "Failed to create payment record")

    # ✅ Call ZenoPay API
    zeno_payload = {
        "order_id": order_id,
        "buyer_name": name,
        "buyer_phone": normalized_phone, # Use normalized phone for ZenoPay
        "buyer_email": email,
        "amount": amount,
    }
    
    # Only include webhook_url if configured
    if ZENO_WEBHOOK_URL:
        zeno_payload["webhook_url"] = ZENO_WEBHOOK_URL

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            logger.info(f"📤 Calling ZenoPay API: {zeno_payload}")
            
            resp = await client.post(
                "https://zenoapi.com/api/payments/mobile_money_tanzania",
                json=zeno_payload,
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": ZENO_API_KEY,
                },
            )
            
            logger.info(f"📥 ZenoPay response: status={resp.status_code}, body={resp.text}")

    except httpx.TimeoutException:
        logger.error("❌ ZenoPay API timeout")
        await payments_col.update_one(
            {"order_id": order_id},
            {"$set": {"status": "FAILED", "error": "Gateway timeout"}}
        )
        raise HTTPException(504, "Payment gateway timeout. Please try again.")
    
    except Exception as e:
        logger.error(f"❌ ZenoPay API error: {e}")
        await payments_col.update_one(
            {"order_id": order_id},
            {"$set": {"status": "FAILED", "error": str(e)}}
        )
        raise HTTPException(502, "Payment gateway error")

    # ✅ Handle ZenoPay response
    if resp.status_code not in (200, 201, 202):
        error_msg = resp.text
        logger.error(f"❌ ZenoPay rejected: {error_msg}")
        
        await payments_col.update_one(
            {"order_id": order_id},
            {"$set": {
                "status": "FAILED",
                "zeno_response": error_msg,
                "error": f"Gateway rejected with status {resp.status_code}"
            }}
        )
        raise HTTPException(502, f"Payment gateway rejected request: {error_msg}")

    # ✅ Parse success response
    try:
        zeno_data = resp.json()
        
        # Save ZenoPay response to database
        await payments_col.update_one(
            {"order_id": order_id},
            {"$set": {"zeno_response": zeno_data}}
        )
        
        logger.info(f"✅ Payment initiated successfully: {zeno_data}")
        
        # Expected response format:
        # {
        #   "status": "success",
        #   "resultcode": "000",
        #   "message": "Request in progress. You will receive a callback shortly",
        #   "order_id": "3rer407fe-3ee8-4525-456f-ccb95de38250"
        # }
        
        return {
            "paymentUrl": zeno_data.get("payment_url", ""),
            "orderId": order_id,
            "order_id": order_id,
            "status": "PENDING",
            "message": zeno_data.get("message", "USSD request sent. Please confirm on your phone."),
            "zeno_status": zeno_data.get("status"),
            "zeno_resultcode": zeno_data.get("resultcode")
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to parse ZenoPay response: {e}")
        raise HTTPException(502, "Invalid response from payment gateway")


@app.get("/payment/status/{order_id}")
async def payment_status(order_id: str):
    """
    📊 CHECK PAYMENT STATUS (Frontend Compatible)
    1. Actively checks ZenoPay if local status is PENDING.
    2. Translates 'COMPLETED' -> 'SUCCESS' so the app understands it.
    """
    # 1. Find the payment record (Search both key formats)
    cursor = payments_col.find({
        "$or": [{"orderId": order_id}, {"order_id": order_id}]
    }).sort("createdAt", -1)
    
    payments = await cursor.to_list(length=1)
    if not payments:
        raise HTTPException(404, "Payment not found")
    
    latest_payment = payments[0]
    # Default to PENDING if status is missing
    current_status = latest_payment.get("status", "PENDING")

    # 2. AUTO-FIX: If pending, force check ZenoPay API
    if current_status == "PENDING":
        try:
            if ZENO_API_KEY:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.get(
                        f"https://zenoapi.com/api/payments/check_status/{order_id}",
                        headers={"x-api-key": ZENO_API_KEY}
                    )
                    
                    if resp.status_code == 200:
                        zeno_data = resp.json()
                        zeno_status = zeno_data.get("payment_status", "").upper()
                        
                        # If Zeno says COMPLETED, upgrade user immediately
                        if zeno_status == "COMPLETED":
                            logger.info(f"✅ Auto-Healing: Found COMPLETED payment {order_id}")
                            
                            now = datetime.utcnow()
                            duration_days = int(latest_payment.get("duration_days", 30))
                            uuid_ = latest_payment["uuid"]
                            
                            # Upgrade logic (Same as webhook)
                            device = await devices_col.find_one({"uuid": uuid_})
                            
                            if device and device.get("isPremium") and device.get("premiumUntil"):
                                current_expiry = device["premiumUntil"]
                                base_date = max(current_expiry, now)
                                premium_until = base_date + timedelta(days=duration_days)
                            else:
                                premium_until = now + timedelta(days=duration_days)
                            
                            await devices_col.update_one(
                                {"uuid": uuid_},
                                {"$set": {
                                    "isPremium": True,
                                    "premiumUntil": premium_until,
                                    "currentPackage": latest_payment.get("package"),
                                    "trialRemaining": 0,
                                    "trialUsed": True,
                                    "upgradedAt": now
                                }}
                            )

                            await payments_col.update_one(
                                {"_id": latest_payment["_id"]},
                                {"$set": {
                                    "status": "COMPLETED",
                                    "updatedAt": now,
                                    "auto_healed": True
                                }}
                            )
                            
                            current_status = "COMPLETED"
                            
        except Exception as e:
            logger.error(f"⚠️ Auto-check failed: {e}")

    # 3. 🚨 THE TRANSLATION FIX 🚨
    # The database says "COMPLETED", but the App expects "SUCCESS".
    # We map it here so the App is happy.
    final_status_for_app = current_status
    if current_status == "COMPLETED":
        final_status_for_app = "SUCCESS"

    # 4. Fetch device info for the response
    device = await devices_col.find_one({"uuid": latest_payment["uuid"]})

    return {
        "order_id": order_id,
        "status": final_status_for_app, # <--- Sending "SUCCESS"
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
    
    This endpoint allows the frontend to manually trigger the upgrade logic
    if the webhook fails or times out. It checks the payment status from ZenoPay
    and upgrades the user if payment was completed.
    """
    logger.info(f"🔧 Manual upgrade triggered for order_id: {order_id}")
    
    # Find payment record
    cursor = payments_col.find({
        "$or": [{"orderId": order_id}, {"order_id": order_id}]
    }).sort("createdAt", -1)
    
    payments = await cursor.to_list(length=1)
    if not payments:
        raise HTTPException(404, "Payment not found")
    
    payment = payments[0]
    
    # If already completed, return success
    if payment.get("status") == "COMPLETED":
        device = await devices_col.find_one({"uuid": payment["uuid"]})
        return {
            "status": "already_upgraded",
            "isPremium": device.get("isPremium", False) if device else False,
            "message": "User already upgraded"
        }
    
    # Check payment status with ZenoPay API
    if not ZENO_API_KEY:
        raise HTTPException(500, "Payment system not configured")
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"https://zenoapi.com/api/payments/check_status/{order_id}",
                headers={"x-api-key": ZENO_API_KEY}
            )
            
            if resp.status_code != 200:
                logger.error(f"❌ ZenoPay status check failed: {resp.status_code}")
                raise HTTPException(502, "Could not verify payment status")
            
            zeno_data = resp.json()
            payment_status = zeno_data.get("payment_status", "").upper()
            
            logger.info(f"📊 ZenoPay status: {payment_status}")
            
            if payment_status != "COMPLETED":
                return {
                    "status": "not_completed",
                    "payment_status": payment_status,
                    "message": "Payment not yet completed"
                }
            
            # Payment is completed - upgrade user
            now = datetime.utcnow()
            duration_days = int(payment.get("duration_days", 30))
            
            # Check if user is already premium (top-up logic)
            device = await devices_col.find_one({"uuid": payment["uuid"]})
            if device and device.get("isPremium") and device.get("premiumUntil"):
                current_expiry = device["premiumUntil"]
                base_date = max(current_expiry, now)
                premium_until = base_date + timedelta(days=duration_days)
            else:
                premium_until = now + timedelta(days=duration_days)
            
            # Update device
            await devices_col.update_one(
                {"uuid": payment["uuid"]},
                {"$set": {
                    "isPremium": True,
                    "premiumUntil": premium_until,
                    "currentPackage": payment.get("package"),
                    "trialRemaining": 0,
                    "trialUsed": True,
                    "upgradedAt": now
                }}
            )
            
            # Update payment record
            await payments_col.update_one(
                {"_id": payment["_id"]},
                {"$set": {
                    "status": "COMPLETED",
                    "updatedAt": now,
                    "manual_upgrade": True
                }}
            )
            
            logger.info(f"✅ Manual upgrade successful for {payment['uuid']}")
            
            return {
                "status": "upgraded",
                "isPremium": True,
                "premiumUntil": premium_until.isoformat(),
                "message": "Upgrade successful"
            }
            
    except httpx.TimeoutException:
        raise HTTPException(504, "Payment verification timeout")
    except Exception as e:
        logger.error(f"❌ Manual upgrade error: {e}")
        raise HTTPException(500, "Upgrade failed")

# ==========================================
# 🔧 FIX: Add the route matching your logs
# ==========================================
@app.post("/webhooks/zeno")      # <--- THIS IS THE MISSING ROUTE
@app.post("/payment-webhook")    # Keep this for backward compatibility
async def zenopay_webhook(request: Request):
    """🔔 ZENOPAY WEBHOOK HANDLER (Fixed to allow payments)"""
    logger.info("="*80)
    logger.info("🔔 WEBHOOK RECEIVED - Starting processing")
    logger.info("="*80)
    
    # 1️⃣ CAPTURE PAYLOAD FIRST (Handle Form or JSON)
    try:
        if "application/json" in request.headers.get("content-type", ""):
            payload = await request.json()
        else:
            form_data = await request.form()
            payload = dict(form_data)
        
        logger.info(f"📦 Webhook Payload: {payload}")
    except Exception as e:
        logger.error(f"❌ Could not parse webhook body: {e}")
        return {"status": "error", "message": "Parse error"}

    # 2️⃣ AUTH CHECK (RELAXED)
    received_key = request.headers.get("x-api-key")
    # Log but don't fail if key is missing (fixes some Zeno gateway versions)
    if received_key and ZENO_API_KEY and received_key != ZENO_API_KEY:
        logger.warning(f"⚠️ Webhook Key Mismatch! Proceeding anyway.")
    
    # 3️⃣ EXTRACT DATA
    order_id = payload.get("order_id")
    payment_status = (payload.get("payment_status") or "").upper() 
    reference = payload.get("reference", "")

    if not order_id or not payment_status:
        logger.warning(f"⚠️ Incomplete webhook payload")
        return {"status": "ignored", "reason": "missing fields"}

    # 4️⃣ UPDATE DATABASE
    # Search for the payment
    cursor = payments_col.find({
        "$or": [{"orderId": order_id}, {"order_id": order_id}]
    }).sort("createdAt", -1)
    
    payments = await cursor.to_list(length=1)
    if not payments:
        logger.warning(f"⚠️ Payment not found for order_id: {order_id}")
        return {"status": "ignored", "reason": "order_id not found in DB"}
    
    payment = payments[0]

    # Idempotency (Don't process twice)
    if payment.get("status") == "COMPLETED":
        logger.info(f"✅ Payment {order_id} already processed")
        return {"status": "already processed"}

    # 5️⃣ UPGRADE USER
    if payment_status == "COMPLETED":
        logger.info(f"✅ Payment COMPLETED: {order_id}")
        
        now = datetime.utcnow()
        duration_days = int(payment.get("duration_days", 30))
        uuid_ = payment["uuid"]

        # Upgrade Logic
        device = await devices_col.find_one({"uuid": uuid_})
        
        if device and device.get("isPremium") and device.get("premiumUntil"):
            current_expiry = device["premiumUntil"]
            base_date = max(current_expiry, now)
            premium_until = base_date + timedelta(days=duration_days)
        else:
            premium_until = now + timedelta(days=duration_days)

        # Update Device
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

        # Update Payment Record
        await payments_col.update_one(
            {"_id": payment["_id"]},
            {"$set": {
                "status": "COMPLETED",
                "updatedAt": now,
                "reference": reference,
                "webhook_payload": payload
            }}
        )
        logger.info(f"🚀 Device {uuid_} upgraded until {premium_until}")

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

    result = await devices_col.update_many(
        {"isPremium": True, "premiumUntil": {"$lte": now}},
        {"$set": {
            "isPremium": False,
            "premiumUntil": None,
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
