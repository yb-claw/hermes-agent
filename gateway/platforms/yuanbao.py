"""
Yuanbao platform adapter.

Connects to the Yuanbao WebSocket gateway, handles authentication (AUTH_BIND),
heartbeat, and reconnection.  Message receive (T05) and send (T06) logic
will be added in follow-up tasks.

Configuration in config.yaml (or via env vars):
    platforms:
      yuanbao:
        yuanbao_app_id: "..."         # or YUANBAO_APP_ID
        yuanbao_app_secret: "..."     # or YUANBAO_APP_SECRET
        yuanbao_bot_id: "..."         # or YUANBAO_BOT_ID  (optional, returned by sign-token)
        yuanbao_ws_url: "wss://..."          # or YUANBAO_WS_URL
        yuanbao_api_domain: "https://..."      # or YUANBAO_API_DOMAIN
"""

from __future__ import annotations

import asyncio
import collections
import dataclasses
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import time
import urllib.parse
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import sys

import httpx

try:
    import websockets
    import websockets.exceptions
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
    websockets = None  # type: ignore[assignment]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_document_from_bytes,
    cache_image_from_bytes,
)
from gateway.platforms.helpers import MessageDeduplicator
from gateway.platforms.yuanbao_media import (
    download_url as media_download_url,
    get_cos_credentials,
    upload_to_cos,
    build_image_msg_body,
    build_file_msg_body,
    guess_mime_type,
    is_image,
    md5_hex,
)
from gateway.platforms.yuanbao_proto import (
    CMD_TYPE,
    _BIZ_PKG,
    _encode_field,
    _encode_string,
    _encode_message,
    _encode_varint,
    _fields_to_dict,
    _get_string,
    _get_varint,
    _parse_fields,
    WT_LEN,
    WT_VARINT,
    WS_HEARTBEAT_RUNNING,
    WS_HEARTBEAT_FINISH,
    HERMES_INSTANCE_ID,
    decode_conn_msg,
    decode_inbound_push,
    decode_query_group_info_rsp,
    decode_get_group_member_list_rsp,
    encode_auth_bind,
    encode_biz_msg,
    encode_ping,
    encode_push_ack,
    encode_send_c2c_message,
    encode_send_group_message,
    encode_send_private_heartbeat,
    encode_send_group_heartbeat,
    encode_query_group_info,
    encode_get_group_member_list,
    next_seq_no,
)
from gateway.session import SessionSource, build_session_key

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Version / platform constants (used in AUTH_BIND and sign-token headers)
# ---------------------------------------------------------------------------
try:
    from hermes_cli import __version__ as _HERMES_VERSION
except ImportError:
    _HERMES_VERSION = "0.0.0"

_APP_VERSION = _HERMES_VERSION
_BOT_VERSION = _HERMES_VERSION
_YUANBAO_INSTANCE_ID = str(HERMES_INSTANCE_ID)  # single source: yuanbao_proto.HERMES_INSTANCE_ID
_OPERATION_SYSTEM = sys.platform

# ---------------------------------------------------------------------------
# Module-level active adapter singleton.
# Set by YuanbaoAdapter.connect(), cleared by disconnect().
# Consumers (cron delivery, send_message tool) import get_active_adapter()
# to obtain the running instance without going through yuanbao_tools.
# ---------------------------------------------------------------------------
_active_adapter: Optional["YuanbaoAdapter"] = None


def get_active_adapter() -> Optional["YuanbaoAdapter"]:
    """Return the currently connected YuanbaoAdapter, or None."""
    return _active_adapter


DEFAULT_WS_GATEWAY_URL = "wss://bot-wss.yuanbao.tencent.com/wss/connection"
DEFAULT_API_DOMAIN = "https://bot.yuanbao.tencent.com"

HEARTBEAT_INTERVAL_SECONDS = 30.0
CONNECT_TIMEOUT_SECONDS = 15.0
AUTH_TIMEOUT_SECONDS = 10.0
MAX_RECONNECT_ATTEMPTS = 100
DEFAULT_SEND_TIMEOUT = 30.0  # WS biz request timeout (was 10s)

# Close codes that indicate permanent errors — do NOT reconnect.
NO_RECONNECT_CLOSE_CODES = {4012, 4013, 4014, 4018, 4019, 4021}

# Heartbeat timeout threshold — N consecutive missed pongs trigger reconnect.
HEARTBEAT_TIMEOUT_THRESHOLD = 2

# Auth error code classification
AUTH_FAILED_CODES = {4001, 4002, 4003}      # permanent auth failure, re-sign token
AUTH_RETRYABLE_CODES = {4010, 4011, 4099}   # transient, can retry with same token

# Maximum number of distinct chat IDs tracked in _chat_locks.
# Oldest entry is evicted when the limit is reached to prevent unbounded growth.
_CHAT_DICT_MAX_SIZE = 1000

# Reply Heartbeat configuration
REPLY_HEARTBEAT_INTERVAL_S = 2.0   # Send RUNNING every 2 seconds
REPLY_HEARTBEAT_TIMEOUT_S = 30.0   # Auto-stop after 30 seconds of inactivity

# Reply-to reference configuration
REPLY_REF_TTL_S = 300.0            # Reference dedup TTL (5 minutes)
_REPLY_REF_MAX_ENTRIES = 500       # Max capacity of reference dedup dict

# Slow-response hint: push a waiting message when agent produces no data for this duration (seconds)
SLOW_RESPONSE_TIMEOUT_S = 120.0
SLOW_RESPONSE_MESSAGE = "任务有点复杂，正在努力处理中，请耐心等待..."

# Placeholder message filter (skip these pure placeholders when no actual media content)
_SKIPPABLE_PLACEHOLDERS = frozenset({
    "[image]", "[图片]", "[file]", "[文件]",
    "[video]", "[视频]", "[voice]", "[语音]",
})

# Anchor regex: matches [image|ybres:xxx], [file:name|ybres:xxx], [voice|ybres:xxx], [video|ybres:xxx]
_YB_RES_REF_RE = re.compile(
    r"\[(image|voice|video|file(?::[^|\]]*)?)\|ybres:([A-Za-z0-9_\-]+)\]"
)
OBSERVED_MEDIA_BACKFILL_LOOKBACK = 50
OBSERVED_MEDIA_BACKFILL_MAX_RESOLVE_PER_TURN = 12


def _strip_cron_wrapper_for_yuanbao(content: str) -> str:
    """Strip scheduler cron header/footer wrapper for cleaner Yuanbao output."""
    if not content.startswith("Cronjob Response: "):
        return content

    divider = "\n-------------\n\n"
    footer_prefix = '\n\nTo stop or manage this job, send me a new message (e.g. "stop reminder '
    divider_pos = content.find(divider)
    footer_pos = content.rfind(footer_prefix)
    if divider_pos < 0 or footer_pos < 0 or footer_pos <= divider_pos:
        return content

    header = content[:divider_pos]
    if "\n(job_id: " not in header:
        return content

    body_start = divider_pos + len(divider)
    body = content[body_start:footer_pos].strip()
    return body or content

class YuanbaoAdapter(BasePlatformAdapter):
    """Yuanbao AI Bot adapter backed by a persistent WebSocket connection."""

    PLATFORM = Platform.YUANBAO
    MAX_TEXT_CHUNK: int = 4000  # Yuanbao single message character limit

    def __init__(self, config: PlatformConfig, **kwargs: Any) -> None:
        super().__init__(config, Platform.YUANBAO)

        # Credentials / endpoints from config (populated by config.py from env/yaml)
        self._app_key: str = (config.yuanbao_app_id or "").strip()
        self._app_secret: str = (config.yuanbao_app_secret or "").strip()
        self._bot_id: Optional[str] = config.yuanbao_bot_id or None
        self._ws_url: str = (config.yuanbao_ws_url or DEFAULT_WS_GATEWAY_URL).strip()
        self._api_domain: str = (config.yuanbao_api_domain or DEFAULT_API_DOMAIN).rstrip("/")
        self._route_env: str = (config.yuanbao_route_env or "").strip()

        # Runtime state
        self._ws = None                          # websockets connection
        self._connect_id: Optional[str] = None  # received from BIND_ACK
        self._pending_acks: Dict[str, asyncio.Future] = {}  # req_id (str) -> Future
        self._chat_locks: collections.OrderedDict[str, asyncio.Lock] = collections.OrderedDict()

        # Background tasks
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._recv_task: Optional[asyncio.Task] = None

        # Inbound dispatch tasks — tracked so disconnect() can cancel them
        self._inbound_tasks: set[asyncio.Task] = set()

        # Reconnection state
        self._reconnect_attempts: int = 0
        self._reconnecting: bool = False         # guards against concurrent reconnects
        self._consecutive_hb_timeouts: int = 0  # heartbeat timeout counter
        self._pending_pong: Optional[asyncio.Future] = None  # current in-flight ping future
        self._reconnect_in_progress: bool = False  # guards against concurrent reconnect tasks

        # Set of background tasks — prevent GC from collecting fire-and-forget tasks
        self._background_tasks: set[asyncio.Task] = set()

        # Internal sequence counter (separate from proto-level next_seq_no)
        self._seq: int = 0

        # Reply Heartbeat state: chat_id -> asyncio.Task (the periodic sender)
        self._reply_heartbeat_tasks: Dict[str, asyncio.Task] = {}

        # Member cache: group_code -> [{"user_id":..., "nickname":..., ...}, ...]
        # Populated by get_group_member_list(), used by @mention resolution.
        self._member_cache: Dict[str, list] = {}

        # Inbound message deduplication (WS reconnect / network jitter)
        self._dedup = MessageDeduplicator(ttl_seconds=300)

        # Reply-to dedup: inbound_msg_id -> expire_ts
        # Only the first outbound message carries refMsgId for a given inbound msg.
        self._reply_ref_used: Dict[str, float] = {}

        # Slow-response notifier: chat_id -> asyncio.Task
        # Fires a "please wait" message if the agent takes > SLOW_RESPONSE_TIMEOUT_S
        self._slow_response_tasks: Dict[str, asyncio.Task] = {}

        # Reply Heartbeat last-active timestamps: chat_id -> unix timestamp
        self._reply_hb_last_active: Dict[str, float] = {}

        # replyToMode config: 'off' | 'first' | 'all' (default: 'first')
        self._reply_to_mode: str = (
            getattr(config, 'yuanbao_reply_to_mode', None)
            or os.getenv("YUANBAO_REPLY_TO_MODE", "first")
        ).strip().lower()

        # ------------------------------------------------------------------
        # DM / Group access control policy (platform-level filter, aligned with wecom.py)
        # ------------------------------------------------------------------
        self._dm_policy: str = (
            config.yuanbao_dm_policy
            or os.getenv("YUANBAO_DM_POLICY", "open")
        ).strip().lower()

        _dm_allow_from_raw: str = (
            config.yuanbao_dm_allow_from
            or os.getenv("YUANBAO_DM_ALLOW_FROM", "")
        )
        self._dm_allow_from: list[str] = [x.strip() for x in _dm_allow_from_raw.split(",") if x.strip()]

        self._group_policy: str = (
            config.yuanbao_group_policy
            or os.getenv("YUANBAO_GROUP_POLICY", "open")
        ).strip().lower()

        _group_allow_from_raw: str = (
            config.yuanbao_group_allow_from
            or os.getenv("YUANBAO_GROUP_ALLOW_FROM", "")
        )
        self._group_allow_from: list[str] = [x.strip() for x in _group_allow_from_raw.split(",") if x.strip()]

        # ------------------------------------------------------------------
        # Auto-sethome: first user to message the bot becomes the owner.
        # If no home channel is configured, the first conversation will be
        # automatically set as the home channel.  When the existing home
        # channel is a group chat (group:xxx), it stays eligible for
        # upgrade — the first DM will override it with direct:xxx.
        # ------------------------------------------------------------------
        _existing_home = os.getenv("YUANBAO_HOME_CHANNEL") or (
            config.home_channel.chat_id if config.home_channel else ""
        )
        self._auto_sethome_done: bool = bool(_existing_home) and not _existing_home.startswith("group:")

    # ------------------------------------------------------------------
    # Task tracking helper
    # ------------------------------------------------------------------

    def _track_task(self, task: asyncio.Task) -> asyncio.Task:
        """Register a fire-and-forget task so it won't be GC'd prematurely."""
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    # ------------------------------------------------------------------
    # Abstract method implementations
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        """
        Connect to Yuanbao WS gateway and authenticate.

        Flow:
          1. Get sign token via HTTP API
          2. Open WebSocket connection (websockets library, no built-in ping)
          3. Send AUTH_BIND, wait for BIND_ACK, extract connectId
          4. Start heartbeat task and receive task
        """
        if not WEBSOCKETS_AVAILABLE:
            msg = "Yuanbao startup failed: 'websockets' package not installed"
            self._set_fatal_error("yuanbao_missing_dependency", msg, retryable=True)
            logger.warning("[%s] %s. Run: pip install websockets", self.name, msg)
            return False

        if not self._app_key or not self._app_secret:
            msg = (
                "Yuanbao startup failed: "
                "YUANBAO_APP_ID and YUANBAO_APP_SECRET are required"
            )
            self._set_fatal_error("yuanbao_missing_credentials", msg, retryable=False)
            logger.error("[%s] %s", self.name, msg)
            return False

        # Idempotency guard
        if self._ws is not None:
            try:
                # websockets >= 10 uses .open; fallback attribute check
                is_open = getattr(self._ws, "open", None)
                if is_open is True or (callable(is_open) and is_open()):
                    logger.debug("[%s] Already connected, skipping connect()", self.name)
                    return True
            except Exception:
                pass

        # Acquire platform-scoped lock to prevent duplicate connections
        if not self._acquire_platform_lock(
            'yuanbao-app-key', self._app_key, 'Yuanbao app key'
        ):
            return False

        try:
            # Step 1: Get sign token
            logger.info("[%s] Fetching sign token from %s", self.name, self._api_domain)
            token_data = await get_sign_token(
                self._app_key, self._app_secret, self._api_domain,
                route_env=self._route_env,
            )

            # Update bot_id if returned by sign-token API
            if token_data.get("bot_id"):
                self._bot_id = str(token_data["bot_id"])

            # Step 2: Open WebSocket connection (disable built-in ping/pong)
            logger.info("[%s] Connecting to %s", self.name, self._ws_url)
            self._ws = await asyncio.wait_for(
                websockets.connect(  # type: ignore[attr-defined]
                    self._ws_url,
                    ping_interval=None,   # we manage heartbeat ourselves
                    ping_timeout=None,
                    close_timeout=5,
                ),
                timeout=CONNECT_TIMEOUT_SECONDS,
            )

            # Step 3: Authenticate (AUTH_BIND + wait for BIND_ACK)
            authed = await self._authenticate(token_data)
            if not authed:
                await self._cleanup_ws()
                return False

            # Step 4: Start background tasks
            self._reconnect_attempts = 0
            self._mark_connected()
            self._loop = asyncio.get_running_loop()
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(), name=f"yuanbao-heartbeat-{self._connect_id}"
            )
            self._recv_task = asyncio.create_task(
                self._receive_loop(), name=f"yuanbao-recv-{self._connect_id}"
            )
            logger.info(
                "[%s] Connected. connectId=%s botId=%s",
                self.name, self._connect_id, self._bot_id,
            )

            global _active_adapter
            _active_adapter = self

            return True

        except asyncio.TimeoutError:
            logger.error("[%s] Connection timed out", self.name)
            await self._cleanup_ws()
            self._release_platform_lock()
            return False
        except Exception as exc:
            logger.error("[%s] connect() failed: %s", self.name, exc, exc_info=True)
            await self._cleanup_ws()
            self._release_platform_lock()
            return False

    async def disconnect(self) -> None:
        """Cancel background tasks and close the WebSocket connection."""
        global _active_adapter
        if _active_adapter is self:
            _active_adapter = None

        self._running = False
        self._mark_disconnected()
        self._release_platform_lock()

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        if self._recv_task:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
            self._recv_task = None

        # Fail any pending ACK futures
        disc_exc = RuntimeError("YuanbaoAdapter disconnected")
        for fut in self._pending_acks.values():
            if not fut.done():
                fut.set_exception(disc_exc)
        self._pending_acks.clear()

        # Cancel all reply heartbeat tasks
        for task in list(self._reply_heartbeat_tasks.values()):
            if not task.done():
                task.cancel()
        self._reply_heartbeat_tasks.clear()

        # Cancel all slow-response notifiers
        for task in list(self._slow_response_tasks.values()):
            if not task.done():
                task.cancel()
        self._slow_response_tasks.clear()

        # Cancel all in-flight inbound dispatch tasks
        for task in list(self._inbound_tasks):
            if not task.done():
                task.cancel()
        self._inbound_tasks.clear()

        # Clear module-level refresh locks to avoid stale locks from a previous event loop
        _refresh_locks.clear()

        await self._cleanup_ws()
        logger.info("[%s] Disconnected", self.name)

    def _get_chat_lock(self, chat_id: str) -> asyncio.Lock:
        """Return (or create) a per-chat-id lock with safe LRU eviction."""
        if chat_id in self._chat_locks:
            # Move to end (most-recently-used) so LRU eviction targets idle keys
            self._chat_locks.move_to_end(chat_id)
            return self._chat_locks[chat_id]
        if len(self._chat_locks) >= _CHAT_DICT_MAX_SIZE:
            # Evict the oldest *unlocked* entry to avoid breaking in-flight sends
            evicted = False
            for key in list(self._chat_locks):
                if not self._chat_locks[key].locked():
                    self._chat_locks.pop(key)
                    evicted = True
                    break
            if not evicted:
                # All locks are held — pop the oldest anyway as a last resort
                self._chat_locks.pop(next(iter(self._chat_locks)))
        self._chat_locks[chat_id] = asyncio.Lock()
        return self._chat_locks[chat_id]

    def _should_attach_reply_ref(self, inbound_msg_id: str) -> bool:
        """
        Determine whether to attach refMsgId to this outbound message.

        Rules:
        - replyToMode='off' → never attach
        - replyToMode='all' → always attach
        - replyToMode='first' (default) → only attach for the first reply to the same inbound message

        With TTL cleanup to prevent memory leaks.
        """
        if self._reply_to_mode == "off":
            return False
        if self._reply_to_mode == "all":
            return True
        if not inbound_msg_id:
            return False

        now = time.time()

        # Clean up expired entries
        if len(self._reply_ref_used) > _REPLY_REF_MAX_ENTRIES:
            expired = [k for k, ts in self._reply_ref_used.items() if now - ts > REPLY_REF_TTL_S]
            for k in expired:
                self._reply_ref_used.pop(k, None)

        # Check if already used
        if inbound_msg_id in self._reply_ref_used:
            return False

        self._reply_ref_used[inbound_msg_id] = now
        return True

    @staticmethod
    def _is_self_reference(from_account: str, bot_id: Optional[str]) -> bool:
        """Detect whether the message is from the bot itself (self-reference)."""
        if not from_account or not bot_id:
            return False
        return from_account == bot_id

    @staticmethod
    def _is_skippable_placeholder(text: str, media_count: int = 0) -> bool:
        """Detect whether the message is a pure placeholder (should be skipped when no actual content)."""
        if media_count > 0:
            return False
        stripped = text.strip()
        return stripped in _SKIPPABLE_PLACEHOLDERS

    @staticmethod
    def _extract_quote_context(cloud_custom_data: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract quote context from cloud_custom_data, mapping to MessageEvent.reply_to_*.

        Returns:
          (reply_to_message_id, reply_to_text)
        """
        if not cloud_custom_data:
            return None, None
        try:
            parsed = json.loads(cloud_custom_data)
        except (json.JSONDecodeError, TypeError):
            return None, None

        quote = parsed.get("quote") if isinstance(parsed, dict) else None
        if not isinstance(quote, dict):
            return None, None

        # type=2 corresponds to image reference; desc may be empty in some cases, provide a placeholder.
        quote_type = int(quote.get("type") or 0)
        desc = str(quote.get("desc") or "").strip()
        if quote_type == 2 and not desc:
            desc = "[image]"
        if not desc:
            return None, None

        quote_id = str(quote.get("id") or "").strip() or None
        sender = str(quote.get("sender_nickname") or quote.get("sender_id") or "").strip()
        quote_text = f"{sender}: {desc}" if sender else desc
        return quote_id, quote_text

    @staticmethod
    def _extract_inbound_media_refs(msg_body: list) -> List[Dict[str, str]]:
        """
        Extract inbound image/file references from TIM msg_body.

        Return example:
          [{"kind": "image", "url": "https://..."}, {"kind": "file", "url": "...", "name": "a.pdf"}]
        """
        refs: List[Dict[str, str]] = []
        for elem in msg_body or []:
            if not isinstance(elem, dict):
                continue
            msg_type = elem.get("msg_type", "")
            content = elem.get("msg_content", {}) or {}
            if not isinstance(content, dict):
                continue

            if msg_type == "TIMImageElem":
                # Align with openclaw plugin: prefer medium image (index 1), fallback to index 0.
                image_info_array = content.get("image_info_array")
                if not isinstance(image_info_array, list):
                    image_info_array = []
                image_info = None
                if len(image_info_array) > 1 and isinstance(image_info_array[1], dict):
                    image_info = image_info_array[1]
                elif len(image_info_array) > 0 and isinstance(image_info_array[0], dict):
                    image_info = image_info_array[0]
                image_url = str((image_info or {}).get("url") or "").strip()
                if image_url:
                    refs.append({"kind": "image", "url": image_url})
                continue

            if msg_type == "TIMFileElem":
                file_url = str(content.get("url") or "").strip()
                file_name = (
                    str(content.get("file_name") or "").strip()
                    or str(content.get("fileName") or "").strip()
                    or str(content.get("filename") or "").strip()
                )
                if file_url:
                    ref: Dict[str, str] = {"kind": "file", "url": file_url}
                    if file_name:
                        ref["name"] = file_name
                    refs.append(ref)
        return refs

    @staticmethod
    def _guess_image_ext_from_url(url: str) -> str:
        """Guess image extension from URL path."""
        path = urllib.parse.urlparse(url).path
        ext = os.path.splitext(path)[1].lower()
        if ext in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".heic", ".tiff"}:
            return ext
        return ".jpg"

    async def _resolve_inbound_media_urls(
        self, media_refs: List[Dict[str, str]]
    ) -> Tuple[List[str], List[str]]:
        """Download inbound image/file to local cache, return (local_paths, mime_types).

        Yuanbao COS hostnames resolve to private IPs, tripping the SSRF guard
        in vision_tools. We download ourselves and return local cache paths.
        """
        media_urls: List[str] = []
        media_types: List[str] = []

        for ref in media_refs:
            kind = str(ref.get("kind") or "").strip().lower()
            url = str(ref.get("url") or "").strip()
            if kind not in {"image", "file"} or not url:
                continue

            try:
                fetch_url = await self._resolve_inbound_download_url(url)
            except Exception as exc:
                logger.warning("[%s] inbound media resolve failed: kind=%s url=%s err=%s", self.name, kind, url, exc)
                continue

            cached = await self._download_and_cache_yuanbao_resource(
                fetch_url=fetch_url,
                kind=kind,
                file_name=str(ref.get("name") or "").strip() or None,
                log_tag=f"placeholder_url={url[:80]}",
            )
            if cached is None:
                continue
            local_path, mime = cached
            media_urls.append(local_path)
            media_types.append(mime)

        return media_urls, media_types

    async def _download_and_cache_yuanbao_resource(
        self,
        *,
        fetch_url: str,
        kind: str,
        file_name: Optional[str] = None,
        log_tag: str = "",
    ) -> Optional[Tuple[str, str]]:
        """Download a Yuanbao resource to local cache. Returns ``(local_path, mime)`` or ``None``."""
        try:
            file_bytes, content_type = await media_download_url(
                fetch_url, max_size_mb=self.MEDIA_MAX_SIZE_MB,
            )
        except Exception as exc:
            logger.warning(
                "[%s] inbound media download failed: kind=%s %s err=%s",
                self.name, kind, log_tag, exc,
            )
            return None

        if kind == "image":
            ext = self._guess_image_ext_from_url(fetch_url)
            try:
                local_path = cache_image_from_bytes(file_bytes, ext=ext)
            except ValueError as exc:
                logger.warning(
                    "[%s] inbound image cache rejected: %s err=%s",
                    self.name, log_tag, exc,
                )
                return None
            mime = guess_mime_type(f"image{ext}")
            if not mime.startswith("image/"):
                mime = content_type if content_type.startswith("image/") else "image/jpeg"
            return local_path, mime

        # kind == "file"
        if not file_name:
            parsed = urllib.parse.urlparse(fetch_url)
            file_name = os.path.basename(parsed.path) or "file"
        try:
            local_path = cache_document_from_bytes(file_bytes, file_name)
        except Exception as exc:
            logger.warning(
                "[%s] inbound file cache failed: %s err=%s",
                self.name, log_tag, exc,
            )
            return None
        mime = guess_mime_type(file_name) or content_type or "application/octet-stream"
        return local_path, mime

    @staticmethod
    def _parse_resource_id(url: str) -> str:
        """Extract ``resourceId`` from a Yuanbao resource placeholder URL."""
        if not url:
            return ""
        try:
            parsed = urllib.parse.urlparse(url)
        except Exception:
            return ""
        query = urllib.parse.parse_qs(parsed.query)
        resource_ids = query.get("resourceId") or query.get("resourceid") or []
        return str(resource_ids[0]).strip() if resource_ids else ""

    async def _resolve_inbound_download_url(self, url: str) -> str:
        """Resolve a Yuanbao resource placeholder URL to a fetchable real URL.

        Falls back to the original URL on failure.
        """
        resource_id = self._parse_resource_id(url)
        if not resource_id:
            return url
        try:
            return await self._download_url_by_resource_id(resource_id)
        except Exception:
            return url

    async def _download_url_by_resource_id(self, resource_id: str) -> str:
        """Exchange a Yuanbao ``resourceId`` for a short-lived direct download URL. Raises on failure."""
        resource_id = resource_id.strip()
        if not resource_id:
            raise RuntimeError("missing resource_id")

        token_data = await self._get_cached_token()
        token = str(token_data.get("token") or "").strip()
        source = str(token_data.get("source") or "web").strip() or "web"
        bot_id = str(token_data.get("bot_id") or self._bot_id or self._app_key).strip()
        if not token or not bot_id:
            raise RuntimeError("missing token or bot_id for resource download")

        api_url = f"{self._api_domain}/api/resource/v1/download"

        headers = {
            "Content-Type": "application/json",
            "X-ID": bot_id,
            "X-Token": token,
            "X-Source": source,
        }

        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            for attempt in range(2):
                resp = await client.get(api_url, params={"resourceId": resource_id}, headers=headers)
                if resp.status_code == 401 and attempt == 0:
                    # Force refresh token once on expiry and retry
                    token_data = await force_refresh_sign_token(self._app_key, self._app_secret, self._api_domain)
                    token = str(token_data.get("token") or "").strip()
                    source = str(token_data.get("source") or source or "web").strip() or "web"
                    bot_id = str(token_data.get("bot_id") or self._bot_id or self._app_key).strip()
                    if not token or not bot_id:
                        break
                    headers["X-ID"] = bot_id
                    headers["X-Token"] = token
                    headers["X-Source"] = source
                    continue

                resp.raise_for_status()
                payload = resp.json()
                code = payload.get("code")
                if code not in (None, 0):
                    raise RuntimeError(
                        f"resource/v1/download failed: code={code}, msg={payload.get('msg', '')}"
                    )
                data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
                real_url = str((data or {}).get("url") or (data or {}).get("realUrl") or "").strip()
                if real_url:
                    return real_url
                raise RuntimeError("resource/v1/download missing url/realUrl")

        raise RuntimeError("resource/v1/download did not return a URL")

    @staticmethod
    def truncate_message(
        content: str,
        max_length: int = 4000,
        len_fn: Optional[Callable[[str], int]] = None,
    ) -> List[str]:
        """
        Split a long message into chunks with table-awareness.

        Tables (contiguous ``| ... |`` rows) are kept as indivisible blocks.
        Code blocks use the base-class close/reopen fence logic.
        Page indicators like ``(1/3)`` are stripped from the output.

        Falls back to ``BasePlatformAdapter.truncate_message`` for non-table
        content and for overall text that fits in a single chunk.
        """
        _len = len_fn or len
        if _len(content) <= max_length:
            return [content]

        _INDICATOR_RE = re.compile(r'\s*\(\d+/\d+\)$')

        def _base_split_no_indicator(text: str) -> List[str]:
            parts = BasePlatformAdapter.truncate_message(text, max_length, len_fn)
            return [_INDICATOR_RE.sub('', p) for p in parts]

        atoms = _split_into_atoms(content)

        chunks: List[str] = []
        current_parts: List[str] = []
        current_len = 0  # tracks actual merged length including separators

        def _flush() -> None:
            if current_parts:
                merged = '\n\n'.join(current_parts)
                if _len(merged) <= max_length:
                    chunks.append(merged)
                else:
                    chunks.extend(_base_split_no_indicator(merged))

        for atom in atoms:
            atom_len = _len(atom)
            sep_len = 2 if current_parts else 0
            projected = current_len + sep_len + atom_len

            if projected > max_length and current_parts:
                _flush()
                current_parts = []
                current_len = 0
                sep_len = 0

            if not current_parts and atom_len > max_length:
                if _is_table_atom(atom):
                    chunks.append(atom)
                    continue
                chunks.extend(_base_split_no_indicator(atom))
                continue

            current_parts.append(atom)
            current_len += sep_len + atom_len

        _flush()

        # Merge small trailing/leading chunks with their neighbours
        if len(chunks) > 1:
            merged_chunks: List[str] = [chunks[0]]
            for chunk in chunks[1:]:
                prev = merged_chunks[-1]
                combined = prev + '\n\n' + chunk
                if _len(combined) <= max_length:
                    merged_chunks[-1] = combined
                else:
                    merged_chunks.append(chunk)
            chunks = merged_chunks

        return chunks if chunks else [content]

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """
        Send text message with auto-chunking and per-chat-id ordering guarantee.

        chat_id format:
          - C2C:  "direct:{account_id}" or plain "{account_id}" (no prefix)
          - Group: "group:{group_code}"

        Enhanced logic:
          1. Acquire or create a lock for the chat_id to ensure serial sending per chat_id
          2. Call truncate_message() for chunking (preserving code fence integrity)
          3. Send each chunk via _send_text_chunk() (with retry); stop immediately on any failure
        """
        if self._ws is None:
            return SendResult(success=False, error="Not connected", retryable=True)

        self._cancel_slow_response_notifier(chat_id)

        lock = self._get_chat_lock(chat_id)
        async with lock:
            content_to_send = _strip_cron_wrapper_for_yuanbao(content)
            chunks = self.truncate_message(content_to_send, self.MAX_TEXT_CHUNK)
            logger.info(
                "[%s] truncate_message: input=%d chars, max=%d, output=%d chunk(s) sizes=%s",
                self.name, len(content_to_send), self.MAX_TEXT_CHUNK,
                len(chunks), [len(c) for c in chunks],
            )
            for i, chunk in enumerate(chunks):
                r_to = reply_to if i == 0 else None
                result = await self._send_text_chunk(chat_id, chunk, r_to)
                if not result.success:
                    return result

        # Send FINISH heartbeat after message delivery to ensure ordering: RUNNING → message arrives → FINISH
        try:
            await self._send_heartbeat_once(chat_id, WS_HEARTBEAT_FINISH)
        except Exception:
            pass
        return SendResult(success=True)

    async def _send_text_chunk(
        self,
        chat_id: str,
        text: str,
        reply_to: Optional[str] = None,
        retry: int = 3,
    ) -> SendResult:
        """
        Send a single text chunk with retry (exponential backoff: 1s, 2s, 4s).

        - Retry up to `retry` times
        - Wait 2^attempt seconds after each failure (0→1s, 1→2s, 2→4s)
        - Return the final result
        """
        last_error: str = "Unknown error"
        for attempt in range(retry):
            try:
                if chat_id.startswith("group:"):
                    group_code = chat_id[len("group:"):]
                    raw = await self._send_group_message(group_code, text, reply_to)
                else:
                    to_account = chat_id.removeprefix("direct:")
                    raw = await self._send_c2c_message(to_account, text)

                if raw.get("success"):
                    return SendResult(success=True, message_id=raw.get("msg_key"))

                last_error = raw.get("error", "Unknown error")
                logger.warning(
                    "[%s] _send_text_chunk attempt %d/%d failed: %s",
                    self.name, attempt + 1, retry, last_error,
                )
            except Exception as exc:
                last_error = str(exc)
                logger.warning(
                    "[%s] _send_text_chunk attempt %d/%d exception: %s",
                    self.name, attempt + 1, retry, last_error,
                )

            if attempt < retry - 1:
                await asyncio.sleep(2 ** attempt)  # 1s, 2s, 4s

        logger.error(
            "[%s] _send_text_chunk max retries (%d) exceeded. Last error: %s",
            self.name, retry, last_error,
        )
        return SendResult(success=False, error=f"Max retries exceeded: {last_error}")

    async def _send_c2c_message(self, to_account: str, text: str) -> dict:
        """Send C2C text message, return {success: bool, msg_key: str}."""
        msg_body = [{"msg_type": "TIMTextElem", "msg_content": {"text": text}}]
        req_id = f"c2c_{next_seq_no()}"
        encoded = encode_send_c2c_message(
            to_account=to_account,
            msg_body=msg_body,
            from_account=self._bot_id or "",
            msg_id=req_id,
        )
        try:
            response = await self._send_biz_request(encoded, req_id=req_id)
            return {"success": True, "msg_key": response.get("msg_id", "")}
        except asyncio.TimeoutError:
            return {"success": False, "error": f"Request timeout after {DEFAULT_SEND_TIMEOUT}s"}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    # @mention pattern: (whitespace or start) + @ + nickname + (whitespace or end)
    _AT_USER_RE = re.compile(r'(?:(?<=\s)|(?<=^))@(\S+?)(?=\s|$)', re.MULTILINE)

    def _build_msg_body_with_mentions(self, text: str, group_code: str) -> list:
        """Parse @nickname patterns and build mixed TIMTextElem + TIMCustomElem msg_body."""
        members = self._member_cache.get(group_code, [])
        if not members:
            return [{"msg_type": "TIMTextElem", "msg_content": {"text": text}}]

        nickname_to_uid = {}
        for m in members:
            nick = m.get("nickname") or m.get("nick_name") or ""
            uid = m.get("user_id") or ""
            if nick and uid:
                nickname_to_uid[nick.lower()] = (nick, uid)

        msg_body: list = []
        last_idx = 0
        for match in self._AT_USER_RE.finditer(text):
            start = match.start()
            if start > last_idx:
                seg = text[last_idx:start].strip()
                if seg:
                    msg_body.append({"msg_type": "TIMTextElem", "msg_content": {"text": seg}})

            nickname = match.group(1)
            entry = nickname_to_uid.get(nickname.lower())
            if entry:
                real_nick, uid = entry
                msg_body.append({
                    "msg_type": "TIMCustomElem",
                    "msg_content": {
                        "data": json.dumps({"elem_type": 1002, "text": f"@{real_nick}", "user_id": uid}),
                    },
                })
            else:
                msg_body.append({"msg_type": "TIMTextElem", "msg_content": {"text": f"@{nickname}"}})

            last_idx = match.end()

        if last_idx < len(text):
            tail = text[last_idx:].strip()
            if tail:
                msg_body.append({"msg_type": "TIMTextElem", "msg_content": {"text": tail}})

        if not msg_body:
            msg_body.append({"msg_type": "TIMTextElem", "msg_content": {"text": text}})

        return msg_body

    async def _send_group_message(
        self,
        group_code: str,
        text: str,
        reply_to: Optional[str] = None,
    ) -> dict:
        """Send group text message, auto-converting @nickname to TIMCustomElem."""
        msg_body = self._build_msg_body_with_mentions(text, group_code)
        req_id = f"grp_{next_seq_no()}"
        encoded = encode_send_group_message(
            group_code=group_code,
            msg_body=msg_body,
            from_account=self._bot_id or "",
            msg_id=req_id,
            ref_msg_id=reply_to or "",
        )
        try:
            response = await self._send_biz_request(encoded, req_id=req_id)
            return {"success": True, "msg_key": response.get("msg_id", "")}
        except asyncio.TimeoutError:
            return {"success": False, "error": f"Request timeout after {DEFAULT_SEND_TIMEOUT}s"}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    async def _send_biz_request(
        self,
        encoded_conn_msg: bytes,
        req_id: str,
        timeout: float = DEFAULT_SEND_TIMEOUT,
    ) -> dict:
        """
        Send a business-layer request and wait for the response.

        1. Register a Future in self._pending_acks[req_id]
        2. Send encoded_conn_msg (bytes) to WS
        3. asyncio.wait_for(future, timeout)
        4. Clean up pending_acks on timeout/exception
        """
        if self._ws is None:
            raise RuntimeError("Not connected")

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._pending_acks[req_id] = future
        try:
            await self._ws.send(encoded_conn_msg)
            result = await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
            return result
        except asyncio.TimeoutError:
            raise
        except Exception as exc:
            raise
        finally:
            self._pending_acks.pop(req_id, None)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return basic chat metadata derived from the chat_id prefix.

        chat_id conventions:
          "group:<group_code>"  → group chat
          "direct:<account>"   → C2C / direct message (default)

        TODO (T06): fetch real chat name/member-count from Yuanbao API.
        """
        if chat_id.startswith("group:"):
            return {"name": chat_id, "type": "group"}
        return {"name": chat_id, "type": "dm"}

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    async def _authenticate(self, token_data: dict) -> bool:
        """
        Send AUTH_BIND and read frames until BIND_ACK is received.

        Because _receive_loop is a placeholder (T05), we read frames directly
        here until we get the BIND_ACK response, then hand off to the loop.

        Returns True on success, False on failure/timeout.
        """
        if self._ws is None:
            return False

        token = token_data.get("token", "")
        uid = self._bot_id or token_data.get("bot_id", "")
        source = token_data.get("source") or "bot"  # use API-returned source; default to 'bot' (not 'web') to receive inbound pushes
        route_env = self._route_env or token_data.get("route_env", "") or ""

        msg_id = str(uuid.uuid4())

        # Build and send AUTH_BIND
        auth_bytes = encode_auth_bind(
            biz_id="ybBot",
            uid=uid,
            source=source,
            token=token,
            msg_id=msg_id,
            app_version=_APP_VERSION,
            operation_system=_OPERATION_SYSTEM,
            bot_version=_BOT_VERSION,
            route_env=route_env,
        )
        await self._ws.send(auth_bytes)
        logger.debug("[%s] AUTH_BIND sent (msg_id=%s uid=%s)", self.name, msg_id, uid)

        # Read frames synchronously until BIND_ACK (cmd_type=Response, cmd=auth-bind)
        try:
            _loop = asyncio.get_running_loop()
            deadline = _loop.time() + AUTH_TIMEOUT_SECONDS
            while True:
                remaining = deadline - _loop.time()
                if remaining <= 0:
                    logger.error("[%s] AUTH_BIND timeout waiting for BIND_ACK", self.name)
                    return False

                raw = await asyncio.wait_for(self._ws.recv(), timeout=remaining)
                if not isinstance(raw, (bytes, bytearray)):
                    continue

                try:
                    msg = decode_conn_msg(bytes(raw))
                except Exception as dec_exc:
                    logger.debug("[%s] Failed to decode frame during auth: %s", self.name, dec_exc)
                    continue

                head = msg.get("head", {})
                cmd_type = head.get("cmd_type", -1)
                cmd = head.get("cmd", "")

                # BIND_ACK: Response to auth-bind
                if cmd_type == CMD_TYPE["Response"] and cmd == "auth-bind":
                    status = head.get("status", 0)
                    if status != 0:
                        logger.error(
                            "[%s] BIND_ACK error: status=%d", self.name, status
                        )
                        return False

                    # Extract connectId from BIND_ACK payload
                    connect_id = self._extract_connect_id(msg)
                    self._connect_id = connect_id
                    logger.info("[%s] BIND_ACK received. connectId=%s", self.name, connect_id)
                    return True

                # Ignore other frames (e.g. Ping responses) during auth
                logger.debug(
                    "[%s] Ignoring frame during auth: cmd_type=%d cmd=%s",
                    self.name, cmd_type, cmd,
                )

        except asyncio.TimeoutError:
            logger.error("[%s] Timeout waiting for BIND_ACK", self.name)
            return False
        except Exception as exc:
            logger.error("[%s] Error during authentication: %s", self.name, exc, exc_info=True)
            return False

    def _extract_connect_id(self, decoded_msg: dict) -> Optional[str]:
        """
        Extract connectId from decoded BIND_ACK message.

        The BIND_ACK data payload is an AuthBindRsp protobuf message.
        AuthBindRsp fields (from conn.json):
          field 1: code      (int32)
          field 2: message   (string)
          field 3: connectId (string)  <-- this is what we need
        """
        data: bytes = decoded_msg.get("data", b"")
        if not data:
            return None
        try:
            fdict = _fields_to_dict(_parse_fields(data))
            code = _get_varint(fdict, 1)
            if code != 0:
                message = _get_string(fdict, 2)
                logger.error(
                    "[%s] AuthBindRsp error: code=%d message=%r",
                    self.name, code, message,
                )
                return None
            # connectId is field 3 (not field 1)
            connect_id = _get_string(fdict, 3)
            return connect_id if connect_id else None
        except Exception as exc:
            logger.warning("[%s] Failed to extract connectId: %s", self.name, exc)
            return None

    async def _heartbeat_loop(self) -> None:
        """
        Send HEARTBEAT (ping) every 30 seconds while connected.
        Tracks consecutive missed pongs; triggers reconnect after HEARTBEAT_TIMEOUT_THRESHOLD.
        """
        try:
            while self._running:
                await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
                if self._ws is None:
                    continue
                try:
                    msg_id = str(uuid.uuid4())
                    ping_bytes = encode_ping(msg_id)
                    # Use a dedicated future for pong — server may not echo msg_id back
                    loop = asyncio.get_running_loop()
                    pong_future: asyncio.Future = loop.create_future()
                    self._pending_pong = pong_future
                    # Also register by msg_id as a fallback (in case server does echo it)
                    self._pending_acks[msg_id] = pong_future
                    await self._ws.send(ping_bytes)
                    logger.debug("[%s] PING sent (msg_id=%s)", self.name, msg_id)
                    try:
                        await asyncio.wait_for(pong_future, timeout=10.0)
                        self._consecutive_hb_timeouts = 0
                    except asyncio.TimeoutError:
                        self._pending_acks.pop(msg_id, None)
                        self._consecutive_hb_timeouts += 1
                        logger.warning(
                            "[%s] PONG timeout (%d/%d)",
                            self.name, self._consecutive_hb_timeouts, HEARTBEAT_TIMEOUT_THRESHOLD,
                        )
                        if self._consecutive_hb_timeouts >= HEARTBEAT_TIMEOUT_THRESHOLD:
                            logger.warning("[%s] Heartbeat threshold exceeded, triggering reconnect", self.name)
                            self._schedule_reconnect()
                            return
                    finally:
                        self._pending_acks.pop(msg_id, None)
                        self._pending_pong = None
                except Exception as exc:
                    logger.debug("[%s] Heartbeat send failed: %s", self.name, exc)
        except asyncio.CancelledError:
            pass  # Normal shutdown

    async def _receive_loop(self) -> None:
        """
        Receive loop:
        1. Read WS frame (raw bytes)
        2. Decode via decode_conn_msg()
        3. Dispatch by cmd_type:
           - Response + "ping"      : HEARTBEAT_ACK, log only
           - Response (other)       : RPC response, resolve pending_acks
           - Push (cmd_type=2)     : Server push, determine if inbound message or RPC response
           - Other                  : Ignore or log
        4. Send PushAck for every Push with need_ack=True
        5. Trigger _reconnect_with_backoff() on WS disconnect (ConnectionClosed)
        """
        try:
            async for raw in self._ws:  # type: ignore[union-attr]
                if not isinstance(raw, (bytes, bytearray)):
                    continue
                await self._handle_frame(bytes(raw))
        except asyncio.CancelledError:
            pass
        except websockets.exceptions.ConnectionClosed as close_exc:  # type: ignore[union-attr]
            close_code = getattr(close_exc, 'code', None)
            logger.warning(
                "[%s] WebSocket connection closed: code=%s reason=%s",
                self.name, close_code, getattr(close_exc, 'reason', ''),
            )
            if close_code and close_code in NO_RECONNECT_CLOSE_CODES:
                logger.error(
                    "[%s] Close code %d is non-recoverable, NOT reconnecting",
                    self.name, close_code,
                )
                self._mark_disconnected()
            else:
                self._schedule_reconnect()
        except Exception as exc:
            logger.warning("[%s] receive_loop exited: %s", self.name, exc)
            self._schedule_reconnect()

    async def _handle_frame(self, raw: bytes) -> None:
        """Handle a single WebSocket frame."""
        try:
            msg = decode_conn_msg(raw)
        except Exception as exc:
            logger.debug("[%s] Failed to decode frame: %s", self.name, exc)
            return

        head = msg.get("head", {})
        cmd_type = head.get("cmd_type", -1)
        cmd = head.get("cmd", "")
        msg_id = head.get("msg_id", "")
        need_ack = head.get("need_ack", False)
        data: bytes = msg.get("data", b"")

        # HEARTBEAT_ACK: Response to our ping — resolve the pending pong_future.
        # The server may not echo back the msg_id, so we use _pending_pong as the
        # primary signal and fall back to msg_id lookup for forward-compatibility.
        if cmd_type == CMD_TYPE["Response"] and cmd == "ping":
            logger.debug("[%s] HEARTBEAT_ACK received (msg_id=%s)", self.name, msg_id)
            if self._pending_pong is not None and not self._pending_pong.done():
                self._pending_pong.set_result(True)
            elif msg_id and msg_id in self._pending_acks:
                fut = self._pending_acks.pop(msg_id)
                if not fut.done():
                    fut.set_result(True)
            return

        # Response to an outbound RPC call (e.g. send-message, query_group_info)
        if cmd_type == CMD_TYPE["Response"]:
            if msg_id and msg_id in self._pending_acks:
                fut = self._pending_acks.pop(msg_id)
                if not fut.done():
                    result = {"head": head}
                    if data:
                        result["data"] = data
                    fut.set_result(result)
            else:
                logger.debug(
                    "[%s] Unmatched Response: cmd=%s msg_id=%s",
                    self.name, cmd, msg_id,
                )
            return

        # Server-initiated Push
        if cmd_type == CMD_TYPE["Push"]:
            logger.info("[%s] Push received: cmd=%s msg_id=%s data_len=%d", self.name, cmd, msg_id, len(data))
            # Send PushAck if required
            if need_ack and self._ws is not None:
                try:
                    ack_bytes = encode_push_ack(head)
                    await self._ws.send(ack_bytes)
                except Exception as ack_exc:
                    logger.debug("[%s] Failed to send PushAck: %s", self.name, ack_exc)

            # Some push frames echo an outbound msg_id as confirmation
            if msg_id and msg_id in self._pending_acks:
                fut = self._pending_acks.pop(msg_id)
                if not fut.done():
                    try:
                        decoded = decode_inbound_push(data) if data else {"head": head}
                        fut.set_result(decoded)
                    except Exception as exc:
                        fut.set_exception(exc)
                return

            # Genuine inbound message — dispatch to AI
            if data:
                logger.info(
                    "[%s] WS received inbound push, decoding and dispatching: cmd=%s, data_len=%d",
                    self.name, cmd, len(data),
                )
                self._push_to_inbound(data)
            return

        logger.debug(
            "[%s] Ignoring frame: cmd_type=%d cmd=%s msg_id=%s",
            self.name, cmd_type, cmd, msg_id,
        )

    # ------------------------------------------------------------------
    # Group chat helpers
    # ------------------------------------------------------------------

    def _is_at_bot(self, msg_body: list) -> bool:
        """
        Detect whether the message @Bot.

        AT element format: TIMCustomElem, msg_content.data is a JSON string:
            {"elem_type": 1002, "text": "@xxx", "user_id": "<botId>"}
        Considered @Bot when elem_type == 1002 and user_id == self._bot_id.
        """
        if not self._bot_id:
            return False
        for elem in msg_body:
            if elem.get("msg_type") != "TIMCustomElem":
                continue
            data_str = elem.get("msg_content", {}).get("data", "")
            if not data_str:
                continue
            try:
                custom = json.loads(data_str)
            except (json.JSONDecodeError, TypeError):
                continue
            if custom.get("elem_type") == 1002 and custom.get("user_id") == self._bot_id:
                return True
        return False

    def _extract_bot_mention_text(self, msg_body: list) -> str:
        """Extract the display text used to @-mention this bot (e.g. ``@yuanbao-bot``)."""
        if not self._bot_id:
            return ""
        for elem in msg_body:
            if elem.get("msg_type") != "TIMCustomElem":
                continue
            data_str = elem.get("msg_content", {}).get("data", "")
            if not data_str:
                continue
            try:
                custom = json.loads(data_str)
            except (json.JSONDecodeError, TypeError):
                continue
            if custom.get("elem_type") == 1002 and custom.get("user_id") == self._bot_id:
                mention_text = str(custom.get("text") or "").strip()
                if mention_text:
                    return mention_text
        return ""

    def _build_group_channel_prompt(self, msg_body: list) -> str:
        """Build a per-turn group-chat prompt that highlights which message to respond to."""
        bot_id = str(self._bot_id or "unknown")
        bot_mention = self._extract_bot_mention_text(msg_body) or "unknown"
        return (
            "You are handling a Yuanbao group chat message.\n"
            f"- Your identity: user_id={bot_id}, @-mention name in this group={bot_mention}\n"
            "- Lines in history prefixed with `[nickname|user_id]` are observed group context "
            "and are not necessarily addressed to you.\n"
            "- Treat only the current new message as a request explicitly directed at you, "
            "and answer it directly."
        )

    # Owner-only group commands (no @Bot required for the creator).
    _OWNER_ONLY_GROUP_COMMANDS = frozenset({
        "/new",         # Start new session
        "/reset",       # Alias for /new
        "/retry",       # Retry last message
        "/undo",        # Delete last user/assistant turn
        "/stop",        # Kill all running background processes
        "/approve",     # Approve pending dangerous command
        "/deny",        # Deny pending dangerous command
        "/background",  # Run a prompt in background
        "/bg",          # Alias for /background
        "/btw",         # Temporary side question based on current session context
        "/queue",       # Queue a prompt for next turn
        "/q",           # Alias for /queue
    })

    # Commands any participant can fire with @Bot (e.g. approve/deny pending tool calls).
    _GROUP_AT_BOT_COMMANDS = frozenset({
        "/approve",
        "/deny",
    })

    def _detect_group_slash_command(
        self,
        *,
        push: dict,
        msg_body: list,
        chat_type: str,
        chat_id: str,
        from_account: str,
    ) -> Tuple[Optional[str], Optional[str], Optional[str], bool]:
        """Identify an in-group slash command. Returns ``(cmd, cmd_line, scope, is_owner)``.

        scope: ``"owner"`` (skip @Bot), ``"at_bot"`` (require @Bot), ``"owner_only_reject"``, or ``None``.
        """
        if chat_type != "group":
            return None, None, None, False

        text_elems = [
            e for e in (msg_body or [])
            if e.get("msg_type") == "TIMTextElem"
        ]
        if len(text_elems) != 1:
            return None, None, None, False

        text = (text_elems[0].get("msg_content") or {}).get("text", "")
        cmd_line = self._rewrite_slash_command(text)
        if not cmd_line.startswith("/"):
            return None, None, None, False
        cmd = cmd_line.split(maxsplit=1)[0].lower()

        in_owner_list = cmd in self._OWNER_ONLY_GROUP_COMMANDS
        in_at_bot_list = cmd in self._GROUP_AT_BOT_COMMANDS
        if not in_owner_list and not in_at_bot_list:
            return None, None, None, False

        owner_id = (push or {}).get("bot_owner_id") or ""
        is_owner = bool(owner_id) and owner_id == from_account

        if in_owner_list and is_owner:
            scope = "owner"
            logger.info(
                "[%s] Owner slash command: chat=%s from=%s cmd=%s",
                self.name, chat_id, from_account, cmd,
            )
        elif in_at_bot_list:
            scope = "at_bot"
            logger.info(
                "[%s] At-bot slash command candidate: chat=%s from=%s cmd=%s "
                "(admission still gated on @Bot detection)",
                self.name, chat_id, from_account, cmd,
            )
        else:
            scope = "owner_only_reject"
            logger.info(
                "[%s] Reject non-owner slash command: chat=%s from=%s cmd=%s owner=%s",
                self.name, chat_id, from_account, cmd, owner_id or "(empty)",
            )

        return cmd, cmd_line, scope, is_owner

    # ------------------------------------------------------------------
    # Group chat: observe messages into session transcript
    # ------------------------------------------------------------------

    def _observe_group_message(
        self, source: SessionSource, sender_display: str, text: str,
    ) -> None:
        """Write a non-@bot group message into the transcript for context."""
        store = getattr(self, "_session_store", None)
        if not store:
            return
        try:
            session_entry = store.get_or_create_session(source)
            user_id = source.user_id or "unknown"
            attributed = f"[{sender_display}|{user_id}]\n{text}"
            store.append_to_transcript(
                session_entry.session_id,
                {
                    "role": "user",
                    "content": attributed,
                    "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                    "observed": True,
                },
            )
        except Exception as exc:
            logger.warning("[%s] Failed to observe group message: %s", self.name, exc)

    # ------------------------------------------------------------------
    # Observed-media hydration: download image/file anchors from recent
    # transcript and cache locally for the current @bot turn.
    # ------------------------------------------------------------------

    async def _collect_observed_media(
        self, source: SessionSource,
    ) -> Tuple[List[str], List[str]]:
        """Resolve recent observed image/file anchors into ``(local_paths, mimes)``."""
        store = getattr(self, "_session_store", None)
        if not store:
            return [], []
        try:
            session_entry = store.get_or_create_session(source)
            history = store.load_transcript(session_entry.session_id)
        except Exception as exc:
            logger.warning(
                "[%s] Observed-media hydration setup failed: %s",
                self.name, exc,
            )
            return [], []
        if not history:
            return [], []

        start = max(0, len(history) - OBSERVED_MEDIA_BACKFILL_LOOKBACK)
        order: List[Tuple[str, str, str]] = []  # (rid, kind, filename)
        seen: set[str] = set()
        for msg in history[start:]:
            content = msg.get("content")
            if not isinstance(content, str) or "|ybres:" not in content:
                continue
            for m in _YB_RES_REF_RE.finditer(content):
                head = m.group(1)  # "image" | "file:<name>" | "voice" | "video"
                rid = m.group(2)
                kind, _, filename = head.partition(":")
                kind = kind.strip()
                if kind not in ("image", "file"):
                    continue
                if rid in seen:
                    continue
                seen.add(rid)
                order.append((rid, kind, filename.strip()))
                if len(order) >= OBSERVED_MEDIA_BACKFILL_MAX_RESOLVE_PER_TURN:
                    break
            if len(order) >= OBSERVED_MEDIA_BACKFILL_MAX_RESOLVE_PER_TURN:
                break

        if not order:
            return [], []

        media_paths: List[str] = []
        mimes: List[str] = []
        for rid, kind, filename in order:
            try:
                fresh_url = await self._download_url_by_resource_id(rid)
            except Exception as exc:
                logger.warning(
                    "[%s] observed-media resolve failed: rid=%s kind=%s err=%s",
                    self.name, rid, kind, exc,
                )
                continue
            cached = await self._download_and_cache_yuanbao_resource(
                fetch_url=fresh_url,
                kind=kind,
                file_name=filename or None,
                log_tag=f"rid={rid}",
            )
            if cached is None:
                continue
            path, mime = cached
            media_paths.append(path)
            mimes.append(mime)
        return media_paths, mimes

    # ------------------------------------------------------------------
    # Reply Heartbeat state machine
    # ------------------------------------------------------------------

    async def send_typing(self, chat_id: str, metadata: Optional[dict] = None) -> None:
        """
        Send "typing" status heartbeat (RUNNING), best effort, no exception raised.
        Called periodically by the base class _keep_typing loop, delegated to the Reply Heartbeat state machine.
        """
        try:
            await self._start_reply_heartbeat(chat_id)
        except Exception:
            pass

    async def stop_typing(self, chat_id: str) -> None:
        """
        Stop the RUNNING heartbeat loop without sending FINISH immediately.

        FINISH is sent by send() after actual message delivery to ensure correct ordering:
        RUNNING... → message arrives → FINISH.

        gateway/run.py's _message_handler calls stop_typing() after getting the agent result
        but before returning the response text; at this point the message hasn't been sent via WS yet,
        so we only stop the RUNNING loop here; FINISH is deferred until send() completes.
        """
        try:
            await self._stop_reply_heartbeat(chat_id, send_finish=False)
        except Exception:
            pass

    async def _process_message_background(self, event, session_key: str) -> None:
        """Wrap base class processing with a slow-response notifier."""
        chat_id = event.source.chat_id
        await self._start_slow_response_notifier(chat_id)
        try:
            await super()._process_message_background(event, session_key)
        finally:
            self._cancel_slow_response_notifier(chat_id)

    async def _start_reply_heartbeat(self, chat_id: str) -> None:
        """
        Start or renew the Reply Heartbeat periodic sender (RUNNING, every 2s).

        If the chat_id already has an active heartbeat task, only refresh the timestamp (no duplicate start).
        Auto-stop (send FINISH) after 30 seconds without renewal.
        """
        if self._ws is None or not self._bot_id:
            return

        existing = self._reply_heartbeat_tasks.get(chat_id)
        if existing and not existing.done():
            self._reply_hb_last_active[chat_id] = time.time()
            return

        self._reply_hb_last_active[chat_id] = time.time()

        task = asyncio.create_task(
            self._reply_heartbeat_worker(chat_id),
            name=f"yuanbao-reply-hb-{chat_id}",
        )
        self._reply_heartbeat_tasks[chat_id] = task

    async def _reply_heartbeat_worker(self, chat_id: str) -> None:
        """
        Background coroutine: send RUNNING heartbeat every 2 seconds.
        30 seconds without renewal → send FINISH and exit.
        """
        try:
            # Send RUNNING immediately
            await self._send_heartbeat_once(chat_id, WS_HEARTBEAT_RUNNING)

            while True:
                await asyncio.sleep(REPLY_HEARTBEAT_INTERVAL_S)

                last_active = self._reply_hb_last_active.get(chat_id, 0)
                if time.time() - last_active > REPLY_HEARTBEAT_TIMEOUT_S:
                    break

                if self._ws is None:
                    break

                await self._send_heartbeat_once(chat_id, WS_HEARTBEAT_RUNNING)

        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            cancelled = False
        else:
            cancelled = False
        finally:
            if not cancelled:
                try:
                    await self._send_heartbeat_once(chat_id, WS_HEARTBEAT_FINISH)
                except Exception:
                    pass
            self._reply_heartbeat_tasks.pop(chat_id, None)
            self._reply_hb_last_active.pop(chat_id, None)

    async def _stop_reply_heartbeat(self, chat_id: str, send_finish: bool = True) -> None:
        """
        Stop Reply Heartbeat and optionally send FINISH.

        Args:
            send_finish: When True, send FINISH immediately after cancelling the worker;
                         When False, only cancel the worker; FINISH is the caller's responsibility.
        """
        task = self._reply_heartbeat_tasks.pop(chat_id, None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if send_finish:
            try:
                await self._send_heartbeat_once(chat_id, WS_HEARTBEAT_FINISH)
            except Exception:
                pass

    async def _send_heartbeat_once(self, chat_id: str, heartbeat_val: int) -> None:
        """Send a single heartbeat (RUNNING or FINISH), best effort."""
        if self._ws is None or not self._bot_id:
            return
        try:
            if chat_id.startswith("group:"):
                group_code = chat_id[len("group:"):]
                encoded = encode_send_group_heartbeat(
                    from_account=self._bot_id,
                    group_code=group_code,
                    heartbeat=heartbeat_val,
                )
            else:
                to_account = chat_id.removeprefix("direct:")
                encoded = encode_send_private_heartbeat(
                    from_account=self._bot_id,
                    to_account=to_account,
                    heartbeat=heartbeat_val,
                )
            await self._ws.send(encoded)
            status_name = "RUNNING" if heartbeat_val == WS_HEARTBEAT_RUNNING else "FINISH"
            logger.debug(
                "[%s] Reply heartbeat %s sent: chat=%s",
                self.name, status_name, chat_id,
            )
        except Exception as exc:
            logger.debug("[%s] _send_heartbeat_once failed: %s", self.name, exc)

    # ------------------------------------------------------------------
    # Slow-response notifier (timeout without reply hint)
    # ------------------------------------------------------------------

    async def _start_slow_response_notifier(self, chat_id: str) -> None:
        """Start a delayed task that notifies the user when the agent is slow."""
        self._cancel_slow_response_notifier(chat_id)
        task = asyncio.create_task(
            self._slow_response_notifier(chat_id),
            name=f"yuanbao-slow-resp-{chat_id}",
        )
        self._slow_response_tasks[chat_id] = task

    async def _slow_response_notifier(self, chat_id: str) -> None:
        """Wait SLOW_RESPONSE_TIMEOUT_S, then push a 'please wait' message."""
        try:
            await asyncio.sleep(SLOW_RESPONSE_TIMEOUT_S)
            logger.info(
                "[%s] Agent response exceeded %ds for %s, sending wait notice",
                self.name, int(SLOW_RESPONSE_TIMEOUT_S), chat_id,
            )
            await self._send_text_chunk(chat_id, SLOW_RESPONSE_MESSAGE)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.debug("[%s] Slow-response notifier failed: %s", self.name, exc)

    def _cancel_slow_response_notifier(self, chat_id: str) -> None:
        """Cancel the pending slow-response notifier for *chat_id*, if any."""
        task = self._slow_response_tasks.pop(chat_id, None)
        if task and not task.done():
            task.cancel()

    # ------------------------------------------------------------------
    # Group query methods
    # ------------------------------------------------------------------

    async def query_group_info(self, group_code: str) -> Optional[dict]:
        """
        Query group info (group name, owner, member count, etc.).

        Args:
            group_code: Group code

        Returns:
            {
              "group_code": str,
              "group_name": str,
              "owner_id": str,
              "member_count": int,
              "max_member": int,
            }
            or None (query failed)
        """
        if self._ws is None:
            return None
        encoded = encode_query_group_info(group_code)
        from gateway.platforms.yuanbao_proto import decode_conn_msg as _decode
        decoded = _decode(encoded)
        req_id = decoded["head"]["msg_id"]
        try:
            response = await self._send_biz_request(encoded, req_id=req_id)
            # response comes from _handle_frame; for RPC Response it returns {"head": head}
            # but for Push responses, it returns the decoded push
            # For group query RPCs, we need to decode from conn_data
            # Actually _send_biz_request returns {"head": head} for Response cmd_type
            # Group query response data needs to be obtained from the raw frame
            # Since _handle_frame currently only returns head for Response, group query needs improvement
            # Temporarily return response head to indicate success
            head = response.get("head", {})
            status = head.get("status", 0)
            if status != 0:
                logger.warning("[%s] query_group_info failed: status=%d", self.name, status)
                return None
            # If response contains body/data (from biz layer), decode it
            biz_data = response.get("data", b"") or response.get("body", b"")
            if biz_data and isinstance(biz_data, bytes):
                return decode_query_group_info_rsp(biz_data)
            return {"group_code": group_code}
        except asyncio.TimeoutError:
            logger.warning("[%s] query_group_info timeout: group=%s", self.name, group_code)
            return None
        except Exception as exc:
            logger.warning("[%s] query_group_info failed: %s", self.name, exc)
            return None

    async def get_group_member_list(
        self, group_code: str, offset: int = 0, limit: int = 200
    ) -> Optional[dict]:
        """
        Query group member list.

        Args:
            group_code: Group code
            offset: Pagination offset
            limit: Page size

        Returns:
            {
              "members": [{"user_id": str, "nickname": str, "role": int, ...}, ...],
              "next_offset": int,
              "is_complete": bool,
            }
            or None (query failed)
        """
        if self._ws is None:
            return None
        encoded = encode_get_group_member_list(group_code, offset=offset, limit=limit)
        from gateway.platforms.yuanbao_proto import decode_conn_msg as _decode
        decoded = _decode(encoded)
        req_id = decoded["head"]["msg_id"]
        try:
            response = await self._send_biz_request(encoded, req_id=req_id)
            head = response.get("head", {})
            status = head.get("status", 0)
            if status != 0:
                logger.warning("[%s] get_group_member_list failed: status=%d", self.name, status)
                return None
            biz_data = response.get("data", b"") or response.get("body", b"")
            if biz_data and isinstance(biz_data, bytes):
                result = decode_get_group_member_list_rsp(biz_data)
            else:
                result = {"members": [], "next_offset": 0, "is_complete": True}
            if result and result.get("members"):
                self._member_cache[group_code] = result["members"]
            return result
        except asyncio.TimeoutError:
            logger.warning("[%s] get_group_member_list timeout: group=%s", self.name, group_code)
            return None
        except Exception as exc:
            logger.warning("[%s] get_group_member_list failed: %s", self.name, exc)
            return None

    def _push_to_inbound(self, conn_data: bytes) -> None:
        """
        Convert push frame to MessageEvent and dispatch to AI for processing.

        conn_data in Yuanbao scenario has only two forms:
          - JSON string: callback_command format inbound message (mainstream)
          - Protobuf binary: InboundMessagePush

        Try JSON first, fall back to protobuf on failure. JSON-first avoids feeding JSON ASCII
        bytes to the protobuf parser which may produce "seemingly valid" garbage fields.

        Note: Yuanbao's inbound message callback_command is usually "C2C.CallbackAfterSendMsg" /
        "Group.CallbackAfterSendMsg" — "AfterSendMsg" is misleading; it's actually an inbound message
        (not a bot send echo); do not filter it.
        """
        push = None
        decoded_via = ""  # "json" | "protobuf"

        if conn_data:
            try:
                conn_json = json.loads(conn_data.decode("utf-8"))
            except Exception:
                conn_json = None
            if isinstance(conn_json, dict):
                logger.info(
                    "[%s] conn_data is JSON: fields=%s len=%d",
                    self.name, list(conn_json.keys()), len(conn_data),
                )
                push = _parse_json_push(conn_json) or None
                if push:
                    decoded_via = "json"
            else:
                try:
                    push = decode_inbound_push(conn_data) or None
                except Exception:
                    push = None
                if push:
                    decoded_via = "protobuf"
                    logger.info(
                        "[%s] conn_data is Protobuf (InboundMessagePush): len=%d",
                        self.name, len(conn_data),
                    )

        if not push:
            logger.info("[%s] Push decoded but no valid message. conn_data hex(first64)=%s",
                self.name, conn_data.hex()[:128] if conn_data else "(empty)")
            return

        logger.info(
            "[%s] Push decoded (via=%s): from=%s group=%s msg_id=%s msg_types=%s",
            self.name, decoded_via,
            push.get("from_account", ""),
            push.get("group_code", ""),
            push.get("msg_id", ""),
            [e.get("msg_type", "") for e in push.get("msg_body", [])],
        )
        logger.debug("[%s] Push payload: %s", self.name, push)
        from_account: str = push.get("from_account", "")
        group_code: str = push.get("group_code", "")
        group_name: str = push.get("group_name", "")
        sender_nickname: str = push.get("sender_nickname", "")
        msg_body: list = push.get("msg_body", [])
        msg_id_field: str = push.get("msg_id", "")
        cloud_custom_data: str = push.get("cloud_custom_data", "")

        # ---- Inbound dedup ----
        if msg_id_field and self._dedup.is_duplicate(msg_id_field):
            logger.debug("[%s] Duplicate message ignored: msg_id=%s", self.name, msg_id_field)
            return

        # ---- Self-reference filter ----
        if self._is_self_reference(from_account, self._bot_id):
            logger.debug("[%s] Ignoring self-sent message from %s", self.name, from_account)
            return

        # Determine chat type and build chat_id
        if group_code:
            chat_id = f"group:{group_code}"
            chat_type = "group"
            chat_name = group_name or group_code
        else:
            chat_id = f"direct:{from_account}"
            chat_type = "dm"
            chat_name = sender_nickname or from_account

        # ---- Platform-level access control filter ----
        if chat_type == "dm":
            if not self._is_dm_allowed(from_account):
                logger.debug(
                    "[%s] DM from %s blocked by dm_policy=%s",
                    self.name, from_account, self._dm_policy,
                )
                return
        elif chat_type == "group":
            if not self._is_group_allowed(group_code):
                logger.debug(
                    "[%s] Group %s blocked by group_policy=%s",
                    self.name, group_code, self._group_policy,
                )
                return

        # ------------------------------------------------------------------
        # Auto-sethome: on the very first inbound message, if no home
        # channel has been configured yet, automatically designate this
        # conversation as the Yuanbao home channel.
        # When the existing home is a group chat and a DM arrives,
        # upgrade to DM (direct:xxx overrides group:xxx).
        # ------------------------------------------------------------------
        if not self._auto_sethome_done:
            _cur_home = os.getenv("YUANBAO_HOME_CHANNEL", "")
            # Trigger when: (a) no home at all, or (b) home is group and this is DM
            _should_set = (
                not _cur_home
                or (_cur_home.startswith("group:") and chat_type == "dm")
            )
            if chat_type == "dm":
                self._auto_sethome_done = True  # DM seen — no further upgrades needed
            if _should_set:
                try:
                    from hermes_constants import get_hermes_home
                    from utils import atomic_yaml_write
                    import yaml

                    _home = get_hermes_home()
                    config_path = _home / "config.yaml"
                    user_config: dict = {}
                    if config_path.exists():
                        with open(config_path, encoding="utf-8") as f:
                            user_config = yaml.safe_load(f) or {}
                    user_config["YUANBAO_HOME_CHANNEL"] = chat_id
                    atomic_yaml_write(config_path, user_config)
                    os.environ["YUANBAO_HOME_CHANNEL"] = str(chat_id)
                    logger.info(
                        "[%s] Auto-sethome: designated %s (%s) as Yuanbao home channel",
                        self.name, chat_id, chat_name,
                    )
                    # Silent auto-sethome: no user-facing message, only log
                except Exception as e:
                    logger.warning("[%s] Auto-sethome failed: %s", self.name, e)

        # Extract raw text first (before any history enrichment)
        raw_text = self._rewrite_slash_command(self._extract_text(msg_body))
        media_refs = self._extract_inbound_media_refs(msg_body)

        # ---- Placeholder filter ----
        if self._is_skippable_placeholder(raw_text, len(media_refs)):
            logger.debug("[%s] Skipping placeholder message: %r", self.name, raw_text)
            return

        matched_cmd, cmd_line, cmd_scope, is_owner = self._detect_group_slash_command(
            push=push,
            msg_body=msg_body,
            chat_type=chat_type,
            chat_id=chat_id,
            from_account=from_account,
        )
        if cmd_scope == "owner_only_reject":
            if self._is_at_bot(msg_body):
                self._track_task(asyncio.create_task(
                    self.send(chat_id, f"⚠️ {matched_cmd} is only available to the creator in private chat mode"),
                    name=f"yuanbao-owner-cmd-denial-{matched_cmd}",
                ))
            return

        if cmd_scope == "at_bot" and not self._is_at_bot(msg_body):
            return

        admitted_command: Optional[str] = None
        if cmd_scope == "owner" and cmd_line:
            admitted_command = matched_cmd
            raw_text = cmd_line
        elif cmd_scope == "at_bot" and cmd_line:
            admitted_command = matched_cmd
            raw_text = cmd_line

        source = self.build_source(
            chat_id=chat_id,
            chat_type=chat_type,
            chat_name=chat_name,
            user_id=from_account or None,
            user_name=sender_nickname or from_account,
            # For group chats, set a synthetic thread_id so
            # build_session_key treats it like a shared thread —
            # user_id is kept for auth but excluded from the session
            # key, matching how Telegram/Feishu handle shared threads.
            thread_id="main" if chat_type == "group" else None,
        )

        if chat_type == "group" and not admitted_command and not self._is_at_bot(msg_body):
            self._observe_group_message(source, sender_nickname or from_account, raw_text)
            logger.info(
                "[%s] Group message observed (no @bot): chat=%s from=%s",
                self.name, chat_id, from_account,
            )
            return

        msg_type = self._classify_message_type(raw_text, msg_body)
        event_channel_prompt = None
        event_text = raw_text
        dispatch_source = source
        if chat_type == "group" and not admitted_command:
            event_channel_prompt = self._build_group_channel_prompt(msg_body)
            user_id_label = from_account or "unknown"
            nickname_label = sender_nickname or from_account or "unknown"
            event_text = f"[{nickname_label}|{user_id_label}]\n{raw_text}"
            # Suppress runner's default ``[user_name]`` shared-thread prefix so
            # the text the model sees matches the observed-history format.
            dispatch_source = dataclasses.replace(source, user_name=None)

        async def _dispatch_inbound_event() -> None:
            media_urls, media_types = await self._resolve_inbound_media_urls(media_refs)
            if self._is_skippable_placeholder(raw_text, len(media_urls)):
                logger.debug("[%s] Skip placeholder after media download: %r", self.name, raw_text)
                return

            extra_img_urls: List[str] = []
            extra_img_mimes: List[str] = []
            try:
                extra_img_urls, extra_img_mimes = await self._collect_observed_media(
                    dispatch_source,
                )
            except Exception as exc:
                logger.warning(
                    "[%s] observed-image hydration raised, continuing anyway: %s",
                    self.name, exc,
                )
            if extra_img_urls:
                current = set(media_urls)
                for u, m in zip(extra_img_urls, extra_img_mimes):
                    if u in current:
                        continue
                    media_urls.append(u)
                    media_types.append(m)
                    current.add(u)

            # Replace [kind|ybres:xxx] anchors with local cache paths so
            # the transcript records usable paths for the model.
            _patched_event_text = event_text
            for u, m in zip(media_urls, media_types):
                if not u.startswith("/"):
                    continue
                anchor_match = _YB_RES_REF_RE.search(_patched_event_text)
                if not anchor_match:
                    continue
                head = anchor_match.group(1)
                kind, _, filename = head.partition(":")
                kind = kind.strip()
                if kind == "image" and m.startswith("image/"):
                    replacement = f"[image: {u}]"
                elif kind == "file":
                    label = filename.strip() or os.path.basename(u)
                    replacement = f"[file: {label} → {u}]"
                else:
                    continue
                _patched_event_text = (
                    _patched_event_text[:anchor_match.start()]
                    + replacement
                    + _patched_event_text[anchor_match.end():]
                )

            reply_to_message_id, reply_to_text = self._extract_quote_context(cloud_custom_data)

            logger.info(
                "[%s] Dispatching MessageEvent:\n"
                "  chat_id=%s chat_type=%s user_id=%s msg_id=%s msg_type=%s\n"
                "  reply_to_message_id=%s reply_to_text=%r\n"
                "  media_urls (count=%d, from_history=%d)=%s\n"
                "  media_types=%s\n"
                "  text (len=%d):\n%s\n"
                "  channel_prompt (len=%s):\n%s",
                self.name,
                dispatch_source.chat_id,
                dispatch_source.chat_type,
                dispatch_source.user_id,
                msg_id_field or "",
                getattr(msg_type, "value", msg_type),
                reply_to_message_id,
                (reply_to_text or "")[:200],
                len(media_urls),
                len(extra_img_urls),
                media_urls,
                media_types,
                len(_patched_event_text or ""),
                _patched_event_text,
                len(event_channel_prompt) if event_channel_prompt else "None",
                event_channel_prompt if event_channel_prompt else "<None>",
            )

            event = MessageEvent(
                text=_patched_event_text,
                message_type=msg_type,
                source=dispatch_source,
                message_id=msg_id_field or None,
                raw_message=push,
                media_urls=media_urls,
                media_types=media_types,
                reply_to_message_id=reply_to_message_id,
                reply_to_text=reply_to_text,
                channel_prompt=event_channel_prompt,
            )
            await self.handle_message(event)

        task = asyncio.create_task(
            _dispatch_inbound_event(),
            name=f"yuanbao-inbound-{msg_id_field or 'unknown'}",
        )
        self._inbound_tasks.add(task)
        task.add_done_callback(self._inbound_tasks.discard)

    def _extract_text(self, msg_body: list) -> str:
        """Extract plain text from MsgBody. Media elems embed ``[kind|ybres:<id>]`` anchors."""
        parts: list[str] = []
        for elem in msg_body:
            elem_type: str = elem.get("msg_type", "")
            content: dict = elem.get("msg_content", {})

            if elem_type == "TIMTextElem":
                text = content.get("text", "")
                if text:
                    parts.append(text)
            elif elem_type == "TIMImageElem":
                image_info_array = content.get("image_info_array")
                image_url = ""
                if isinstance(image_info_array, list):
                    if len(image_info_array) > 1 and isinstance(image_info_array[1], dict):
                        image_url = str(image_info_array[1].get("url") or "").strip()
                    if not image_url and image_info_array and isinstance(image_info_array[0], dict):
                        image_url = str(image_info_array[0].get("url") or "").strip()
                rid = self._parse_resource_id(image_url)
                parts.append(f"[image|ybres:{rid}]" if rid else "[image]")
            elif elem_type == "TIMFileElem":
                filename = content.get("file_name", content.get("fileName", content.get("filename", "")))
                rid = self._parse_resource_id(str(content.get("url") or ""))
                if filename and rid:
                    parts.append(f"[file:{filename}|ybres:{rid}]")
                elif rid:
                    parts.append(f"[file|ybres:{rid}]")
                elif filename:
                    parts.append(f"[file: {filename}]")
                else:
                    parts.append("[file]")
            elif elem_type == "TIMSoundElem":
                rid = self._parse_resource_id(str(content.get("url") or ""))
                parts.append(f"[voice|ybres:{rid}]" if rid else "[voice]")
            elif elem_type == "TIMVideoFileElem":
                rid = self._parse_resource_id(str(content.get("url") or ""))
                parts.append(f"[video|ybres:{rid}]" if rid else "[video]")
            elif elem_type == "TIMCustomElem":
                data_val = content.get("data", "")
                if data_val:
                    try:
                        custom = json.loads(data_val)
                        if isinstance(custom, dict) and custom.get("elem_type") == 1002:
                            parts.append(custom.get("text", "[mention]"))
                        else:
                            parts.append("[unsupported message type]")
                    except (json.JSONDecodeError, TypeError):
                        parts.append(data_val)
                else:
                    parts.append("[unsupported message type]")
            elif elem_type == "TIMFaceElem":
                # Sticker/emoji: extract name from data JSON
                raw_data = content.get("data", "")
                face_name = ""
                if raw_data:
                    try:
                        face_data = json.loads(raw_data)
                        face_name = (face_data.get("name") or "").strip()
                    except (json.JSONDecodeError, TypeError, AttributeError):
                        pass
                parts.append(f"[emoji: {face_name}]" if face_name else "[emoji]")
            elif elem_type:
                # Unknown element type — include type as placeholder
                parts.append(f"[{elem_type}]")

        return " ".join(parts) if parts else ""

    @staticmethod
    def _rewrite_slash_command(text: str) -> str:
        """
        Normalize input text: strip whitespace and convert full-width slash (Chinese
        input method) to ASCII slash so commands are recognized correctly.
        """
        text = text.strip()
        if text.startswith('\uff0f'):  # Full-width slash
            text = '/' + text[1:]
        return text

    @staticmethod
    def _classify_message_type(text: str, msg_body: list) -> MessageType:
        """Determine MessageType from text content and msg_body elements."""
        if text.startswith("/"):
            return MessageType.COMMAND
        for elem in msg_body:
            etype = elem.get("msg_type", "")
            if etype == "TIMImageElem":
                return MessageType.PHOTO
            if etype == "TIMSoundElem":
                return MessageType.VOICE
            if etype == "TIMVideoFileElem":
                return MessageType.VIDEO
            if etype == "TIMFileElem":
                return MessageType.DOCUMENT
        return MessageType.TEXT

    # ------------------------------------------------------------------
    # DM active private chat + access control
    # ------------------------------------------------------------------

    DM_MAX_CHARS = 10000  # DM text limit

    async def send_dm(self, user_id: str, text: str) -> SendResult:
        """
        Actively send C2C private chat message.

        Args:
            user_id: Target user ID
            text: Message text (limit 10000 characters)

        Returns:
            SendResult
        """
        if not self._resolve_dm_access(user_id):
            return SendResult(success=False, error="DM access denied for this user")
        if len(text) > self.DM_MAX_CHARS:
            text = text[:self.DM_MAX_CHARS] + "\n...(truncated)"
        chat_id = f"direct:{user_id}"
        return await self.send(chat_id, text)

    def _is_dm_allowed(self, sender_id: str) -> bool:
        """Platform-level DM inbound filter (open / allowlist / disabled)."""
        if self._dm_policy == "disabled":
            return False
        if self._dm_policy == "allowlist":
            return sender_id.strip() in self._dm_allow_from
        return True

    def _is_group_allowed(self, group_code: str) -> bool:
        """Platform-level group chat inbound filter (open / allowlist / disabled)."""
        if self._group_policy == "disabled":
            return False
        if self._group_policy == "allowlist":
            return group_code.strip() in self._group_allow_from
        return True

    def _resolve_dm_access(self, user_id: str) -> bool:
        """Active DM send authorization, reusing _is_dm_allowed()."""
        return self._is_dm_allowed(user_id)

    # ------------------------------------------------------------------
    # Agent tool methods (can be registered as AI toolset)
    # ------------------------------------------------------------------

    async def tool_query_group_info(self, chat_id: str) -> dict:
        """
        AI tool: Query current group info.

        No parameters needed (group_code extracted from session context).
        Returns group name, owner, member count, etc.
        """
        if not chat_id.startswith("group:"):
            return {"error": "This command is only available in group chats"}
        group_code = chat_id[len("group:"):]
        result = await self.query_group_info(group_code)
        if result is None:
            return {"error": "Failed to query group info"}
        return result

    async def tool_query_session_members(
        self,
        chat_id: str,
        action: str = "list_all",
        name: Optional[str] = None,
    ) -> dict:
        """
        AI tool: Query group member list.

        Args:
            chat_id: Chat ID (extracted from session context)
            action: 'find' (search by name) | 'list_bots' (list bots) | 'list_all' (list all)
            name: Search keyword when action='find'

        Returns:
            {"members": [...], "total": int, "mentionHint": str}
        """
        if not chat_id.startswith("group:"):
            return {"error": "This command is only available in group chats"}
        group_code = chat_id[len("group:"):]
        result = await self.get_group_member_list(group_code)
        if result is None:
            return {"error": "Failed to query group members"}

        members = result.get("members", [])

        if action == "find" and name:
            query = name.lower()
            members = [
                m for m in members
                if query in (m.get("nickname", "") or "").lower()
                or query in (m.get("name_card", "") or "").lower()
                or query in (m.get("user_id", "") or "").lower()
            ]
        elif action == "list_bots":
            # Bots typically have role=0 and user_id looks like a bot ID
            # Simple filter here; real scenarios may need more precise detection
            members = [m for m in members if "bot" in (m.get("nickname", "") or "").lower()]

        # Construct mentionHint
        mention_hint = ""
        if members and len(members) <= 10:
            names = [m.get("name_card") or m.get("nickname") or m.get("user_id", "") for m in members]
            mention_hint = "Mention with @name: " + ", ".join(names)

        return {
            "members": members[:50],  # Limit return count
            "total": len(members),
            "mentionHint": mention_hint,
        }

    # ------------------------------------------------------------------
    # Trace Context + Markdown Hint
    # ------------------------------------------------------------------

    @staticmethod
    def _generate_trace_id() -> str:
        """Generate distributed trace ID (32-char hex)."""
        return uuid.uuid4().hex

    @staticmethod
    def _build_traceparent(trace_id: str, span_id: Optional[str] = None) -> str:
        """
        Build W3C traceparent header (simplified).
        Format: 00-{trace_id}-{span_id}-01
        """
        if not span_id:
            span_id = uuid.uuid4().hex[:16]
        return f"00-{trace_id}-{span_id}-01"

    @staticmethod
    def _markdown_hint_system_prompt() -> str:
        """
        Markdown rendering hint (appended to system prompt).

        Tell AI that Yuanbao platform supports Markdown rendering, including:
        - Code blocks (```lang)
        - Tables (| col | col |)
        - Bold/italic
        """
        return (
            "The current platform supports Markdown rendering. You can use the following formats:\n"
            "- Code blocks: ```language\\ncode\\n```\n"
            "- Tables: | col1 | col2 |\\n|---|---|\\n| val1 | val2 |\n"
            "- Bold: **text** / Italic: *text*\n"
            "Please use Markdown formatting when appropriate to improve readability."
        )

    @staticmethod
    def _msg_body_desensitize(msg_body: list) -> list:
        """
        Sanitize message body (for logging).

        Truncate long text in TIMTextElem, replace URLs in TIMImageElem.
        """
        result = []
        for elem in msg_body:
            msg_type = elem.get("msg_type", "")
            content = elem.get("msg_content", {})
            if msg_type == "TIMTextElem":
                text = content.get("text", "")
                if len(text) > 100:
                    text = text[:100] + f"...({len(text)} chars)"
                result.append({"msg_type": msg_type, "msg_content": {"text": text}})
            elif msg_type == "TIMImageElem":
                result.append({"msg_type": msg_type, "msg_content": {"url": "[IMAGE_URL]"}})
            elif msg_type == "TIMFileElem":
                fname = content.get("file_name", content.get("fileName", ""))
                result.append({"msg_type": msg_type, "msg_content": {"file_name": fname}})
            else:
                result.append({"msg_type": msg_type, "msg_content": "[REDACTED]"})
        return result

    # ------------------------------------------------------------------
    # Media validation
    # ------------------------------------------------------------------

    MEDIA_MAX_SIZE_MB: int = 20  # aligned with openclaw plugin default

    @staticmethod
    def _validate_media_before_queue(
        file_bytes: Optional[bytes], filename: str, max_size_mb: int = MEDIA_MAX_SIZE_MB
    ) -> Optional[str]:
        """
        Media pre-validation: check file validity before sending/uploading.

        Returns:
            Error description (str) if validation fails, otherwise None.
        """
        if file_bytes is None or len(file_bytes) == 0:
            return f"Empty file: {filename}"
        max_bytes = max_size_mb * 1024 * 1024
        if len(file_bytes) > max_bytes:
            size_mb = len(file_bytes) / 1024 / 1024
            return f"File too large: {filename} ({size_mb:.1f}MB > {max_size_mb}MB)"
        return None

    # ------------------------------------------------------------------
    # Media send methods
    # ------------------------------------------------------------------

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict] = None,
        **kwargs: Any,
    ) -> SendResult:
        """
        Send image message.

        Flow:
          1. Download image URL content (httpx)
          2. Call genUploadInfo to get COS temporary credentials
          3. PUT upload to COS
          4. Build TIMImageElem message body, send via WS

        If image_url is already a COS public URL (cos.*myqcloud.com or file.myqcloud.com),
        skip re-uploading and use it directly to build the message body.
        """
        if self._ws is None:
            return SendResult(success=False, error="Not connected", retryable=True)

        self._cancel_slow_response_notifier(chat_id)

        try:
            # 1. Download image
            logger.info("[%s] send_image: downloading %s", self.name, image_url)
            file_bytes, content_type = await media_download_url(image_url, max_size_mb=self.MEDIA_MAX_SIZE_MB)

            if not content_type or content_type == "application/octet-stream":
                path_part = image_url.split("?")[0]
                content_type = guess_mime_type(path_part) or "image/jpeg"

            filename = os.path.basename(image_url.split("?")[0]) or "image.jpg"

            validation_err = self._validate_media_before_queue(file_bytes, filename, self.MEDIA_MAX_SIZE_MB)
            if validation_err:
                return SendResult(success=False, error=validation_err)

            file_uuid = md5_hex(file_bytes)

            # 2. Get COS upload credentials
            token_data = await self._get_cached_token()
            token: str = token_data.get("token", "")
            bot_id: str = token_data.get("bot_id", "") or self._bot_id or ""

            credentials = await get_cos_credentials(
                app_key=self._app_key,
                api_domain=self._api_domain,
                token=token,
                filename=filename,
                bot_id=bot_id,
                route_env=self._route_env,
            )

            # 3. Upload to COS
            upload_result = await upload_to_cos(
                file_bytes=file_bytes,
                filename=filename,
                content_type=content_type,
                credentials=credentials,
                bucket=credentials["bucketName"],
                region=credentials["region"],
            )

            # 4. Build TIMImageElem message body
            msg_body = build_image_msg_body(
                url=upload_result["url"],
                uuid=file_uuid,
                filename=filename,
                size=upload_result["size"],
                width=upload_result.get("width", 0),
                height=upload_result.get("height", 0),
                mime_type=content_type,
            )

            # 5. If caption exists, append text message body
            if caption:
                msg_body.append(
                    {"msg_type": "TIMTextElem", "msg_content": {"text": caption}}
                )

            # 6. Send
            async with self._get_chat_lock(chat_id):
                if chat_id.startswith("group:"):
                    group_code = chat_id[len("group:"):]
                    result = await self._send_group_msg_body(group_code, msg_body, reply_to)
                else:
                    to_account = chat_id.removeprefix("direct:")
                    result = await self._send_c2c_msg_body(to_account, msg_body)

            if result.get("success"):
                return SendResult(success=True, message_id=result.get("msg_key"))
            return SendResult(success=False, error=result.get("error", "Unknown error"))

        except Exception as exc:
            logger.error("[%s] send_image() failed: %s", self.name, exc, exc_info=True)
            return SendResult(success=False, error=str(exc))

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict] = None,
        **kwargs: Any,
    ) -> SendResult:
        """
        Send local image file.

        Similar to send_image(), but accepts a local path instead of URL, reads file bytes directly,
        skipping the HTTP download step, going directly to COS upload → TIMImageElem flow.
        Refer to send_document()'s local file reading logic.
        """
        if self._ws is None:
            return SendResult(success=False, error="Not connected", retryable=True)

        self._cancel_slow_response_notifier(chat_id)

        try:
            # 1. Read local file
            if not os.path.isfile(image_path):
                return SendResult(success=False, error=f"File not found: {image_path}")

            logger.info("[%s] send_image_file: reading local file %s", self.name, image_path)
            with open(image_path, "rb") as f:
                file_bytes = f.read()

            filename = os.path.basename(image_path) or "image.jpg"

            validation_err = self._validate_media_before_queue(file_bytes, filename, self.MEDIA_MAX_SIZE_MB)
            if validation_err:
                return SendResult(success=False, error=validation_err)

            content_type = guess_mime_type(filename) or "image/jpeg"
            file_uuid = md5_hex(file_bytes)

            # 2. Get COS upload credentials
            token_data = await self._get_cached_token()
            token: str = token_data.get("token", "")
            bot_id: str = token_data.get("bot_id", "") or self._bot_id or ""

            credentials = await get_cos_credentials(
                app_key=self._app_key,
                api_domain=self._api_domain,
                token=token,
                filename=filename,
                bot_id=bot_id,
                route_env=self._route_env,
            )

            # 3. Upload to COS
            upload_result = await upload_to_cos(
                file_bytes=file_bytes,
                filename=filename,
                content_type=content_type,
                credentials=credentials,
                bucket=credentials["bucketName"],
                region=credentials["region"],
            )

            # 4. Build TIMImageElem message body
            msg_body = build_image_msg_body(
                url=upload_result["url"],
                uuid=file_uuid,
                filename=filename,
                size=upload_result["size"],
                width=upload_result.get("width", 0),
                height=upload_result.get("height", 0),
                mime_type=content_type,
            )

            # 5. If caption exists, append text message body
            if caption:
                msg_body.append(
                    {"msg_type": "TIMTextElem", "msg_content": {"text": caption}}
                )

            # 6. Send
            async with self._get_chat_lock(chat_id):
                if chat_id.startswith("group:"):
                    group_code = chat_id[len("group:"):]
                    result = await self._send_group_msg_body(group_code, msg_body, reply_to)
                else:
                    to_account = chat_id.removeprefix("direct:")
                    result = await self._send_c2c_msg_body(to_account, msg_body)

            if result.get("success"):
                return SendResult(success=True, message_id=result.get("msg_key"))
            return SendResult(success=False, error=result.get("error", "Unknown error"))

        except Exception as exc:
            logger.error("[%s] send_image_file() failed: %s", self.name, exc, exc_info=True)
            return SendResult(success=False, error=str(exc))

    async def send_file(
        self,
        chat_id: str,
        file_url: str,
        filename: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict] = None,
        **kwargs: Any,
    ) -> SendResult:
        """
        Send file message.

        Flow:
          1. Download file URL content (httpx)
          2. Call genUploadInfo to get COS temporary credentials
          3. PUT upload to COS
          4. Build TIMFileElem message body, send via WS
        """
        if self._ws is None:
            return SendResult(success=False, error="Not connected", retryable=True)

        self._cancel_slow_response_notifier(chat_id)

        try:
            # 1. Download file
            logger.info("[%s] send_file: downloading %s", self.name, file_url)
            file_bytes, content_type = await media_download_url(file_url, max_size_mb=self.MEDIA_MAX_SIZE_MB)

            if not filename:
                path_part = file_url.split("?")[0]
                filename = os.path.basename(path_part) or "file"

            validation_err = self._validate_media_before_queue(file_bytes, filename, self.MEDIA_MAX_SIZE_MB)
            if validation_err:
                return SendResult(success=False, error=validation_err)

            if not content_type or content_type == "application/octet-stream":
                content_type = guess_mime_type(filename) or "application/octet-stream"

            file_uuid = md5_hex(file_bytes)

            # 2. Get COS upload credentials
            token_data = await self._get_cached_token()
            token: str = token_data.get("token", "")
            bot_id: str = token_data.get("bot_id", "") or self._bot_id or ""

            credentials = await get_cos_credentials(
                app_key=self._app_key,
                api_domain=self._api_domain,
                token=token,
                filename=filename,
                bot_id=bot_id,
                route_env=self._route_env,
            )

            # 3. Upload to COS
            upload_result = await upload_to_cos(
                file_bytes=file_bytes,
                filename=filename,
                content_type=content_type,
                credentials=credentials,
                bucket=credentials["bucketName"],
                region=credentials["region"],
            )

            # 4. Build TIMFileElem message body
            msg_body = build_file_msg_body(
                url=upload_result["url"],
                filename=filename,
                uuid=file_uuid,
                size=upload_result["size"],
            )

            # 5. Send
            async with self._get_chat_lock(chat_id):
                if chat_id.startswith("group:"):
                    group_code = chat_id[len("group:"):]
                    result = await self._send_group_msg_body(group_code, msg_body, reply_to)
                else:
                    to_account = chat_id.removeprefix("direct:")
                    result = await self._send_c2c_msg_body(to_account, msg_body)

            if result.get("success"):
                return SendResult(success=True, message_id=result.get("msg_key"))
            return SendResult(success=False, error=result.get("error", "Unknown error"))

        except Exception as exc:
            logger.error("[%s] send_file() failed: %s", self.name, exc, exc_info=True)
            return SendResult(success=False, error=str(exc))

    async def send_sticker(
        self,
        chat_id: str,
        sticker_name: Optional[str] = None,
        face_index: Optional[int] = None,
        reply_to: Optional[str] = None,
        **kwargs: Any,
    ) -> SendResult:
        """
        Send sticker/emoji (TIMFaceElem).

        Parsing priority:
          1. sticker_name not empty → fuzzy search in STICKER_MAP, return error if not found
          2. face_index not empty   → directly use that index to build TIMFaceElem (no data)
          3. Both empty          → randomly send a built-in sticker

        chat_id format same as send():
          - C2C:  "direct:{account_id}" or "{account_id}"
          - Group: "group:{group_code}"
        """
        from gateway.platforms.yuanbao_sticker import (
            get_sticker_by_name,
            get_random_sticker,
            build_face_msg_body,
            build_sticker_msg_body,
        )

        if self._ws is None:
            return SendResult(success=False, error="Not connected", retryable=True)

        self._cancel_slow_response_notifier(chat_id)

        try:
            if sticker_name is not None:
                sticker = get_sticker_by_name(sticker_name)
                if sticker is None:
                    return SendResult(
                        success=False,
                        error=f"Sticker not found: {sticker_name!r}",
                    )
                msg_body = build_sticker_msg_body(sticker)
            elif face_index is not None:
                msg_body = build_face_msg_body(face_index=face_index)
            else:
                sticker = get_random_sticker()
                msg_body = build_sticker_msg_body(sticker)

            async with self._get_chat_lock(chat_id):
                if chat_id.startswith("group:"):
                    group_code = chat_id[len("group:"):]
                    result = await self._send_group_msg_body(group_code, msg_body, reply_to)
                else:
                    to_account = chat_id.removeprefix("direct:")
                    result = await self._send_c2c_msg_body(to_account, msg_body)

            if result.get("success"):
                return SendResult(success=True, message_id=result.get("msg_key"))
            return SendResult(success=False, error=result.get("error", "Unknown error"))

        except Exception as exc:
            logger.error("[%s] send_sticker() failed: %s", self.name, exc, exc_info=True)
            return SendResult(success=False, error=str(exc))

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        filename: Optional[str] = None,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict] = None,
        **kwargs: Any,
    ) -> SendResult:
        """
        Send local file (document) message.

        Similar to send_file(), but accepts a local path instead of URL.
        Flow: Read local file → COS upload → Build TIMFileElem → WS send
        """
        if self._ws is None:
            return SendResult(success=False, error="Not connected", retryable=True)

        self._cancel_slow_response_notifier(chat_id)

        try:
            # 1. Read local file
            if not os.path.isfile(file_path):
                return SendResult(success=False, error=f"File not found: {file_path}")

            logger.info("[%s] send_document: reading local file %s", self.name, file_path)
            with open(file_path, "rb") as f:
                file_bytes = f.read()

            if not filename:
                filename = os.path.basename(file_path) or "document"

            validation_err = self._validate_media_before_queue(file_bytes, filename, self.MEDIA_MAX_SIZE_MB)
            if validation_err:
                return SendResult(success=False, error=validation_err)

            content_type = guess_mime_type(filename) or "application/octet-stream"
            file_uuid = md5_hex(file_bytes)

            # 2. Get COS upload credentials
            token_data = await self._get_cached_token()
            token: str = token_data.get("token", "")
            bot_id: str = token_data.get("bot_id", "") or self._bot_id or ""

            credentials = await get_cos_credentials(
                app_key=self._app_key,
                api_domain=self._api_domain,
                token=token,
                filename=filename,
                bot_id=bot_id,
                route_env=self._route_env,
            )

            # 3. Upload to COS
            upload_result = await upload_to_cos(
                file_bytes=file_bytes,
                filename=filename,
                content_type=content_type,
                credentials=credentials,
                bucket=credentials["bucketName"],
                region=credentials["region"],
            )

            # 4. Build TIMFileElem message body
            msg_body = build_file_msg_body(
                url=upload_result["url"],
                filename=filename,
                uuid=file_uuid,
                size=upload_result["size"],
            )

            # 5. If caption exists, append text message body
            if caption:
                msg_body.append(
                    {"msg_type": "TIMTextElem", "msg_content": {"text": caption}}
                )

            # 6. Send
            async with self._get_chat_lock(chat_id):
                if chat_id.startswith("group:"):
                    group_code = chat_id[len("group:"):]
                    result = await self._send_group_msg_body(group_code, msg_body, reply_to)
                else:
                    to_account = chat_id.removeprefix("direct:")
                    result = await self._send_c2c_msg_body(to_account, msg_body)

            if result.get("success"):
                return SendResult(success=True, message_id=result.get("msg_key"))
            return SendResult(success=False, error=result.get("error", "Unknown error"))

        except Exception as exc:
            logger.error("[%s] send_document() failed: %s", self.name, exc, exc_info=True)
            return SendResult(success=False, error=str(exc))

    async def _send_c2c_msg_body(self, to_account: str, msg_body: list) -> dict:
        """Send C2C message with arbitrary MsgBody."""
        req_id = f"c2c_{next_seq_no()}"
        encoded = encode_send_c2c_message(
            to_account=to_account,
            msg_body=msg_body,
            from_account=self._bot_id or "",
            msg_id=req_id,
        )
        try:
            response = await self._send_biz_request(encoded, req_id=req_id)
            return {"success": True, "msg_key": response.get("msg_id", "")}
        except asyncio.TimeoutError:
            return {"success": False, "error": "Request timeout after 30.0s"}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    async def _send_group_msg_body(
        self,
        group_code: str,
        msg_body: list,
        reply_to: Optional[str] = None,
    ) -> dict:
        """Send group message with arbitrary MsgBody."""
        req_id = f"grp_{next_seq_no()}"
        encoded = encode_send_group_message(
            group_code=group_code,
            msg_body=msg_body,
            from_account=self._bot_id or "",
            msg_id=req_id,
            ref_msg_id=reply_to or "",
        )
        try:
            response = await self._send_biz_request(encoded, req_id=req_id)
            return {"success": True, "msg_key": response.get("msg_id", "")}
        except asyncio.TimeoutError:
            return {"success": False, "error": "Request timeout after 30.0s"}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    async def _get_cached_token(self) -> dict:
        """
        Get the current valid sign token (using module-level cache).
        """
        return await get_sign_token(
            self._app_key, self._app_secret, self._api_domain,
            route_env=self._route_env,
        )

    def _schedule_reconnect(self) -> None:
        """Schedule a reconnect only if running and not already reconnecting."""
        if self._running and not self._reconnecting:
            asyncio.create_task(self._reconnect_with_backoff())

    async def _reconnect_with_backoff(self) -> bool:
        """
        Reconnect with exponential backoff.

        Waits 1s, 2s, 4s, … up to 60s between attempts.
        On each attempt, force-refreshes the sign token (old token may be expired).
        Returns True on successful reconnect, False after max attempts.
        """
        if self._reconnecting:
            logger.debug("[%s] Reconnect already in progress, skipping", self.name)
            return False
        self._reconnecting = True
        try:
            return await self._do_reconnect()
        finally:
            self._reconnecting = False

    async def _do_reconnect(self) -> bool:
        """Internal reconnect loop, called under the _reconnecting guard."""
        for attempt in range(MAX_RECONNECT_ATTEMPTS):
            self._reconnect_attempts = attempt + 1
            wait = min(2 ** attempt, 60)
            logger.info(
                "[%s] Reconnect attempt %d/%d in %ds",
                self.name, attempt + 1, MAX_RECONNECT_ATTEMPTS, wait,
            )
            await asyncio.sleep(wait)

            # Clean up existing connection before re-trying
            await self._cleanup_ws()

            try:
                # Force-refresh token to avoid using a stale one
                token_data = await force_refresh_sign_token(
                    self._app_key, self._app_secret, self._api_domain,
                    route_env=self._route_env,
                )
                if token_data.get("bot_id"):
                    self._bot_id = str(token_data["bot_id"])

                self._ws = await asyncio.wait_for(
                    websockets.connect(  # type: ignore[attr-defined]
                        self._ws_url,
                        ping_interval=None,
                        ping_timeout=None,
                        close_timeout=5,
                    ),
                    timeout=CONNECT_TIMEOUT_SECONDS,
                )

                authed = await self._authenticate(token_data)
                if not authed:
                    logger.warning("[%s] Re-auth failed on attempt %d", self.name, attempt + 1)
                    await self._cleanup_ws()
                    continue

                # Restart background tasks
                self._reconnect_attempts = 0
                self._consecutive_hb_timeouts = 0
                self._mark_connected()

                if self._heartbeat_task and not self._heartbeat_task.done():
                    self._heartbeat_task.cancel()
                self._heartbeat_task = asyncio.create_task(
                    self._heartbeat_loop(),
                    name=f"yuanbao-heartbeat-{self._connect_id}",
                )

                if self._recv_task and not self._recv_task.done():
                    self._recv_task.cancel()
                self._recv_task = asyncio.create_task(
                    self._receive_loop(),
                    name=f"yuanbao-recv-{self._connect_id}",
                )

                logger.info(
                    "[%s] Reconnected on attempt %d. connectId=%s",
                    self.name, attempt + 1, self._connect_id,
                )
                return True

            except asyncio.TimeoutError:
                logger.warning("[%s] Reconnect attempt %d timed out", self.name, attempt + 1)
            except Exception as exc:
                logger.warning(
                    "[%s] Reconnect attempt %d failed: %s", self.name, attempt + 1, exc
                )

        logger.error(
            "[%s] Giving up after %d reconnect attempts", self.name, MAX_RECONNECT_ATTEMPTS
        )
        self._mark_disconnected()
        return False

    async def _cleanup_ws(self) -> None:
        """Close and clear the WebSocket connection."""
        ws = self._ws
        self._ws = None
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass

    def get_status(self) -> dict:
        """Return a snapshot of the current connection status."""
        ws = self._ws
        connected = False
        if ws is not None:
            # websockets >= 10 exposes .open as a bool property
            open_attr = getattr(ws, "open", None)
            if open_attr is True:
                connected = True
            elif callable(open_attr):
                try:
                    connected = bool(open_attr())
                except Exception:
                    connected = False

        return {
            "connected": connected,
            "bot_id": self._bot_id,
            "connect_id": self._connect_id,
            "reconnect_attempts": self._reconnect_attempts,
            "ws_url": self._ws_url,
        }

    def _next_seq(self) -> int:
        """Return a monotonically increasing local sequence number."""
        self._seq += 1
        return self._seq


# ============================================================
# Module-level helper functions (JSON push parsing, aligned with TS decodeFromContent)
# ============================================================

def _parse_json_push(raw_json: dict) -> dict | None:
    """
    Convert JSON-format push (from rawData or DirectedPush.content) to
    a dict with the same structure as decode_inbound_push.

    Supports standard callback format (callback_command + from_account + msg_body)
    and legacy format fields (GroupId, MsgSeq, MsgKey, MsgBody, etc.).
    """
    if not raw_json:
        return None

    # Tencent IM callback format uses PascalCase (From_Account, To_Account, MsgBody).
    # Internal format uses snake_case (from_account, to_account, msg_body).
    # Support both.
    from_account = (
        raw_json.get("from_account", "")
        or raw_json.get("From_Account", "")
    )
    group_code = (
        raw_json.get("group_code", "")
        or raw_json.get("GroupId", "")
        or raw_json.get("group_id", "")
    )
    msg_body_raw = (
        raw_json.get("msg_body", [])
        or raw_json.get("MsgBody", [])
    )
    msg_body = _convert_json_msg_body(msg_body_raw)

    # Allow a push with from_account even if msg_body is empty (e.g. callback notifications)
    if not from_account and not msg_body:
        return None

    return {
        "callback_command": raw_json.get("callback_command", ""),
        "from_account": from_account,
        "to_account": raw_json.get("to_account", "") or raw_json.get("To_Account", ""),
        "sender_nickname": raw_json.get("sender_nickname", "") or raw_json.get("nick_name", ""),
        "group_code": group_code,
        "group_name": raw_json.get("group_name", ""),
        "msg_seq": raw_json.get("msg_seq", 0) or raw_json.get("MsgSeq", 0),
        "msg_id": raw_json.get("msg_id", "") or raw_json.get("msg_key", "") or raw_json.get("MsgKey", ""),
        "msg_body": msg_body,
        "cloud_custom_data": raw_json.get("cloud_custom_data", "") or raw_json.get("CloudCustomData", ""),
        "bot_owner_id": raw_json.get("bot_owner_id", "") or raw_json.get("botOwnerId", ""),
        "trace_id": (raw_json.get("log_ext") or {}).get("trace_id", "") if isinstance(raw_json.get("log_ext"), dict) else "",
    }


def _convert_json_msg_body(raw_body: list) -> list:
    """
    Normalize raw JSON msg_body array to [{"msg_type": str, "msg_content": dict}] format.
    Compatible with both PascalCase (MsgType/MsgContent) and snake_case (msg_type/msg_content) naming.
    """
    result = []
    for item in raw_body or []:
        if not isinstance(item, dict):
            continue
        msg_type = item.get("msg_type") or item.get("MsgType", "")
        msg_content = item.get("msg_content") or item.get("MsgContent", {})
        if isinstance(msg_content, str):
            try:
                msg_content = json.loads(msg_content)
            except Exception:
                msg_content = {"text": msg_content}
        result.append({"msg_type": msg_type, "msg_content": msg_content or {}})
    return result


# ============================================================
# Markdown chunking utility functions (originally yuanbao_markdown.py)
# ============================================================

def has_unclosed_fence(text: str) -> bool:
    """
    Detect whether the text has unclosed code block fences.

    Scan line by line, toggling in/out state when encountering a line starting with ```.
    An odd number of toggles indicates an unclosed fence.

    Args:
        text: Markdown text to check

    Returns:
        Returns True if the text ends with an unclosed fence, otherwise False
    """
    in_fence = False
    for line in text.split('\n'):
        if line.startswith('```'):
            in_fence = not in_fence
    return in_fence


def ends_with_table_row(text: str) -> bool:
    """
    Detect whether the text ends with a table row (last non-empty line starts and ends with |).

    Args:
        text: Text to check

    Returns:
        Returns True if the last non-empty line is a table row
    """
    trimmed = text.rstrip()
    if not trimmed:
        return False
    last_line = trimmed.split('\n')[-1].strip()
    return last_line.startswith('|') and last_line.endswith('|')


def split_at_paragraph_boundary(text: str, max_chars: int) -> tuple[str, str]:
    """
    Find the nearest paragraph boundary split point within max_chars, return (head, tail).

    Split priority:
    1. Blank line (paragraph boundary)
    2. Newline after period/question mark/exclamation mark (Chinese and English)
    3. Last newline
    4. Force split at max_chars

    Args:
        text: Text to split
        max_chars: Maximum character count limit

    Returns:
        (head, tail) tuple, head is the front part, tail is the back part, satisfying head + tail == text
    """
    if len(text) <= max_chars:
        return text, ''

    window = text[:max_chars]

    # 1. Prefer the last blank line (\n\n) as paragraph boundary
    pos = window.rfind('\n\n')
    if pos > 0:
        return text[:pos + 2], text[pos + 2:]

    # 2. Then find the last newline after a sentence-ending punctuation
    sentence_end_re = re.compile(r'[。！？.!?]\n')
    best_pos = -1
    for m in sentence_end_re.finditer(window):
        best_pos = m.end()
    if best_pos > 0:
        return text[:best_pos], text[best_pos:]

    # 3. Fallback: find the last newline
    pos = window.rfind('\n')
    if pos > 0:
        return text[:pos + 1], text[pos + 1:]

    # 4. No valid split point found, force split at max_chars
    return text[:max_chars], text[max_chars:]


def _is_fence_atom(text: str) -> bool:
    """Determine whether an atomic block is a code block (starts with ```)."""
    return text.lstrip().startswith('```')


def _is_table_atom(text: str) -> bool:
    """Determine whether an atomic block is a table (first line starts with |)."""
    first_line = text.split('\n')[0].strip()
    return first_line.startswith('|') and first_line.endswith('|')


def _split_into_atoms(text: str) -> list[str]:
    """
    Split text into a list of "atomic blocks", each being an indivisible logical unit:

    - Code block (fence): from opening ``` to closing ``` (including fence lines)
    - Table: consecutive |...| lines forming a whole segment
    - Normal paragraph: plain text segments separated by blank lines

    Blank lines serve as separators and are not included in any atomic block.

    Args:
        text: Markdown text to split

    Returns:
        List of atomic block strings (all non-empty)
    """
    lines = text.split('\n')
    atoms: list[str] = []

    current_lines: list[str] = []
    in_fence = False

    def _is_table_line(line: str) -> bool:
        stripped = line.strip()
        return stripped.startswith('|') and stripped.endswith('|')

    def _flush_current() -> None:
        if current_lines:
            atom = '\n'.join(current_lines)
            if atom.strip():
                atoms.append(atom)
            current_lines.clear()

    for line in lines:
        if in_fence:
            current_lines.append(line)
            if line.startswith('```') and len(current_lines) > 1:
                in_fence = False
                _flush_current()
        elif line.startswith('```'):
            _flush_current()
            in_fence = True
            current_lines.append(line)
        elif _is_table_line(line):
            if current_lines and not _is_table_line(current_lines[-1]):
                _flush_current()
            current_lines.append(line)
        elif line.strip() == '':
            _flush_current()
        else:
            if current_lines and _is_table_line(current_lines[-1]):
                _flush_current()
            current_lines.append(line)

    _flush_current()

    return atoms


def chunk_markdown_text(text: str, max_chars: int = 4000) -> list[str]:
    """
    Split Markdown text into multiple chunks by max_chars.

    Guarantees:
    - Each chunk <= max_chars characters (unless a single code block/table itself exceeds the limit)
    - Code blocks (```...```) are not split in the middle
    - Table rows are not split in the middle (tables output as atomic blocks)
    - Split at paragraph boundaries (blank lines, after periods, etc.)

    Args:
        text: Markdown text to split
        max_chars: Max characters per chunk, default 4000

    Returns:
        List of text chunks after splitting (non-empty)
    """
    if not text:
        return []

    if len(text) <= max_chars:
        return [text]

    # Phase 1: Extract atomic blocks
    atoms = _split_into_atoms(text)

    # Phase 2: Greedy merge
    chunks: list[str] = []
    indivisible_set: set[int] = set()
    current_parts: list[str] = []
    current_len = 0

    def _flush_parts() -> None:
        if current_parts:
            chunks.append('\n\n'.join(current_parts))

    for atom in atoms:
        atom_len = len(atom)
        sep_len = 2 * len(current_parts)
        projected_len = current_len + sep_len + atom_len

        if projected_len > max_chars and current_parts:
            _flush_parts()
            current_parts = []
            current_len = 0

        if (not current_parts
                and atom_len > max_chars
                and (_is_fence_atom(atom) or _is_table_atom(atom))):
            indivisible_set.add(len(chunks))
            chunks.append(atom)
            continue

        current_parts.append(atom)
        current_len += atom_len

    _flush_parts()

    # Phase 3: Post-processing — split still-oversized chunks at paragraph boundaries
    result: list[str] = []
    for idx, chunk in enumerate(chunks):
        if len(chunk) <= max_chars:
            result.append(chunk)
            continue

        if idx in indivisible_set:
            result.append(chunk)
            continue

        if has_unclosed_fence(chunk):
            result.append(chunk)
            continue

        remaining = chunk
        while len(remaining) > max_chars:
            head, remaining = split_at_paragraph_boundary(remaining, max_chars)
            if not head:
                head, remaining = remaining[:max_chars], remaining[max_chars:]
            if head:
                result.append(head)
        if remaining:
            result.append(remaining)

    return [c for c in result if c]


def _infer_block_separator(prev_chunk: str, next_chunk: str) -> str:
    """
    Infer the separator to use between two split chunks.

    Rules (aligned with TS markdown-stream.ts):
    - Previous chunk ends with code fence or next chunk starts with fence → single newline '\\n'
    - Previous chunk ends with table row and next chunk starts with table row → single newline '\\n' (continued table)
    - Otherwise → double newline '\\n\\n' (paragraph separator)

    Args:
        prev_chunk: Previous chunk
        next_chunk: Next chunk

    Returns:
        '\\n' or '\\n\\n'
    """
    prev_trimmed = prev_chunk.rstrip()
    next_trimmed = next_chunk.lstrip()

    # Previous chunk ends with fence or next chunk starts with fence
    if prev_trimmed.endswith('```') or next_trimmed.startswith('```'):
        return '\n'

    # Table continuation
    if ends_with_table_row(prev_chunk):
        first_line = next_trimmed.split('\n')[0].strip() if next_trimmed else ''
        if first_line.startswith('|') and first_line.endswith('|'):
            return '\n'

    return '\n\n'


def _merge_block_streaming_fences(chunks: list[str]) -> list[str]:
    """
    Stream-aware fence-conscious chunk merging.

    When streaming output produces multiple chunks truncated in the middle of a fence,
    attempt to merge adjacent chunks to complete the fence.

    Rules:
    - If chunk i has an unclosed fence and chunk i+1 starts with ```,
        merge i+1 into i (until the fence is closed or no more chunks).
    - Use _infer_block_separator to infer the separator during merging.

    Args:
        chunks: Original chunk list

    Returns:
        Merged chunk list (length <= original length)
    """
    if not chunks:
        return []

    result: list[str] = []
    i = 0
    while i < len(chunks):
        current = chunks[i]
        # If current chunk has unclosed fence, try merging subsequent chunks
        while has_unclosed_fence(current) and i + 1 < len(chunks):
            sep = _infer_block_separator(current, chunks[i + 1])
            current = current + sep + chunks[i + 1]
            i += 1
        result.append(current)
        i += 1

    return result


def _strip_outer_markdown_fence(text: str) -> str:
    """
    Strip outer Markdown fence.

    When AI reply is entirely wrapped in ```markdown\\n...\\n```, remove the outer fence,
    keeping the content. Only strip when the first line is ```markdown (case-insensitive) and the last line is ```.

    Args:
        text: Text to process

    Returns:
        Text with outer fence stripped (returns original if no match)
    """
    if not text:
        return text

    lines = text.split('\n')
    if len(lines) < 3:
        return text

    first_line = lines[0].strip()
    last_line = lines[-1].strip()

    # First line must be ```markdown (optional language tag md/markdown)
    if not re.match(r'^```(?:markdown|md)?\s*$', first_line, re.IGNORECASE):
        return text

    # Last line must be plain ```
    if last_line != '```':
        return text

    # Strip first and last lines
    inner = '\n'.join(lines[1:-1])
    return inner


def _sanitize_markdown_table(text: str) -> str:
    """
    Table output sanitization.

    Handle common formatting issues in AI-generated Markdown tables:
    1. Remove extra whitespace before/after table rows
    2. Ensure separator rows (|---|---|) are correctly formatted
    3. Remove empty table rows

    Args:
        text: Markdown text containing tables

    Returns:
        Sanitized text
    """
    if '|' not in text:
        return text

    lines = text.split('\n')
    result_lines: list[str] = []

    for line in lines:
        stripped = line.strip()

        # Table row processing
        if stripped.startswith('|') and stripped.endswith('|'):
            # Separator row normalization: | --- | --- | → |---|---|
            if re.match(r'^\|[\s\-:]+(\|[\s\-:]+)+\|$', stripped):
                cells = stripped.split('|')
                normalized = '|'.join(
                    cell.strip() if cell.strip() else cell
                    for cell in cells
                )
                result_lines.append(normalized)
            elif stripped == '||' or stripped.replace('|', '').strip() == '':
                # Empty table row → skip
                continue
            else:
                result_lines.append(stripped)
        else:
            result_lines.append(line)

    return '\n'.join(result_lines)


# ============================================================
# Sign-ticket API (originally yuanbao_api.py)
# ============================================================

SIGN_TOKEN_PATH = "/api/v5/robotLogic/sign-token"

#: Retryable business error codes
RETRYABLE_SIGN_CODE = 10099
SIGN_MAX_RETRIES = 3
SIGN_RETRY_DELAY_S = 1.0

#: Early refresh margin (seconds), treat as expiring 60 seconds before actual expiry
CACHE_REFRESH_MARGIN_S = 60

#: HTTP timeout (seconds)
HTTP_TIMEOUT_S = 10.0

# Module-level cache
# key: app_key
# value: {"token": str, "bot_id": str, "expire_ts": float (unix timestamp)}
_token_cache: dict[str, dict[str, Any]] = {}

# Per-app_key refresh locks — prevents concurrent duplicate sign-token requests.
# Safety note: Locks are created lazily inside _get_refresh_lock(), which is only
# called from async functions (running event loop), so the Lock is always bound to
# the correct loop.  disconnect() clears this dict to prevent stale locks from a
# previous event loop persisting across reconnects.
_refresh_locks: dict[str, asyncio.Lock] = {}


def _get_refresh_lock(app_key: str) -> asyncio.Lock:
    """Return (creating if needed) the per-app_key refresh lock.

    Must only be called from within a running event loop (async context).
    """
    if app_key not in _refresh_locks:
        _refresh_locks[app_key] = asyncio.Lock()
    return _refresh_locks[app_key]


def _compute_signature(nonce: str, timestamp: str, app_key: str, app_secret: str) -> str:
    """
    Compute signature (aligned with TypeScript original).
    plain     = nonce + timestamp + app_key + app_secret
    signature = HMAC-SHA256(key=app_secret, msg=plain).hexdigest()
    """
    plain = nonce + timestamp + app_key + app_secret
    return hmac.new(app_secret.encode(), plain.encode(), hashlib.sha256).hexdigest()


def _build_timestamp() -> str:
    """
    Build Beijing time ISO-8601 timestamp (aligned with TypeScript original).
    Format: 2006-01-02T15:04:05+08:00 (no milliseconds)
    """
    bjtime = datetime.now(tz=timezone(timedelta(hours=8)))
    return bjtime.strftime("%Y-%m-%dT%H:%M:%S+08:00")


def _is_cache_valid(entry: dict[str, Any]) -> bool:
    """Determine whether the cache is valid (not expired and has margin)."""
    return entry["expire_ts"] - time.time() > CACHE_REFRESH_MARGIN_S


async def _do_fetch_sign_token(
    app_key: str,
    app_secret: str,
    api_domain: str,
    route_env: str = "",
) -> dict[str, Any]:
    """
    Send sign-ticket HTTP request with auto-retry (up to SIGN_MAX_RETRIES times).
    """
    url = f"{api_domain.rstrip('/')}{SIGN_TOKEN_PATH}"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_S) as client:
        for attempt in range(SIGN_MAX_RETRIES + 1):
            nonce = secrets.token_hex(16)
            timestamp = _build_timestamp()
            signature = _compute_signature(nonce, timestamp, app_key, app_secret)

            payload = {
                "app_key": app_key,
                "nonce": nonce,
                "signature": signature,
                "timestamp": timestamp,
            }

            headers = {
                "Content-Type": "application/json",
                "X-AppVersion": _APP_VERSION,
                "X-OperationSystem": _OPERATION_SYSTEM,
                "X-Instance-Id": _YUANBAO_INSTANCE_ID,
                "X-Bot-Version": _BOT_VERSION,
            }
            if route_env:
                headers["X-Route-Env"] = route_env

            logger.info(
                "Sign token request: url=%s%s",
                url,
                f" (retry {attempt}/{SIGN_MAX_RETRIES})" if attempt > 0 else "",
            )

            response = await client.post(url, json=payload, headers=headers)

            if response.status_code != 200:
                body = response.text
                raise RuntimeError(f"Sign token API returned {response.status_code}: {body[:200]}")

            try:
                result_data: dict[str, Any] = response.json()
            except Exception as exc:
                raise ValueError(f"Sign token response parse error: {exc}") from exc

            code = result_data.get("code")
            if code == 0:
                data = result_data.get("data")
                if not isinstance(data, dict):
                    raise ValueError(f"Sign token response missing 'data' field: {result_data}")
                logger.info("Sign token success: bot_id=%s", data.get("bot_id"))
                return data

            if code == RETRYABLE_SIGN_CODE and attempt < SIGN_MAX_RETRIES:
                logger.warning(
                    "Sign token retryable: code=%s, retrying in %ss (attempt=%d/%d)",
                    code,
                    SIGN_RETRY_DELAY_S,
                    attempt + 1,
                    SIGN_MAX_RETRIES,
                )
                await asyncio.sleep(SIGN_RETRY_DELAY_S)
                continue

            msg = result_data.get("msg", "")
            raise RuntimeError(f"Sign token error: code={code}, msg={msg}")

    raise RuntimeError("Sign token failed: max retries exceeded")


async def get_sign_token(
    app_key: str,
    app_secret: str,
    api_domain: str,
    route_env: str = "",
) -> dict[str, Any]:
    """
    Get WS auth token (with cache).

    Return directly on cache hit without re-requesting; treat as expiring 60 seconds before actual expiry, triggering refresh.
    """
    cached = _token_cache.get(app_key)
    if cached and _is_cache_valid(cached):
        remain = int(cached["expire_ts"] - time.time())
        logger.info("Using cached token (%ds remaining)", remain)
        return dict(cached)

    async with _get_refresh_lock(app_key):
        cached = _token_cache.get(app_key)
        if cached and _is_cache_valid(cached):
            return dict(cached)

        data = await _do_fetch_sign_token(app_key, app_secret, api_domain, route_env)

        duration: int = data.get("duration", 0)
        expire_ts = time.time() + duration if duration > 0 else time.time() + 3600

        _token_cache[app_key] = {
            "token": data.get("token", ""),
            "bot_id": data.get("bot_id", ""),
            "duration": duration,
            "product": data.get("product", ""),
            "source": data.get("source", ""),
            "expire_ts": expire_ts,
        }

    return dict(_token_cache[app_key])


async def force_refresh_sign_token(
    app_key: str,
    app_secret: str,
    api_domain: str,
    route_env: str = "",
) -> dict[str, Any]:
    """
    Force refresh token (clear cache and re-sign).
    """
    logger.warning("[force-refresh] Clearing cache and re-signing token: app_key=****%s", app_key[-4:])
    async with _get_refresh_lock(app_key):
        _token_cache.pop(app_key, None)
        data = await _do_fetch_sign_token(app_key, app_secret, api_domain, route_env)

        duration: int = data.get("duration", 0)
        expire_ts = time.time() + duration if duration > 0 else time.time() + 3600

        _token_cache[app_key] = {
            "token": data.get("token", ""),
            "bot_id": data.get("bot_id", ""),
            "duration": duration,
            "product": data.get("product", ""),
            "source": data.get("source", ""),
            "expire_ts": expire_ts,
        }

    return dict(_token_cache[app_key])


# ---------------------------------------------------------------------------
# Module-level send helper (used by send_message tool)
# ---------------------------------------------------------------------------

_YUANBAO_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}


async def send_yuanbao_direct(
    adapter: "YuanbaoAdapter",
    chat_id: str,
    message: str,
    media_files: Optional[List[Tuple[str, bool]]] = None,
) -> Dict[str, Any]:
    """
    Send helper for the ``send_message`` tool — text + media via Yuanbao.

    Unlike Weixin which creates a fresh adapter per call, Yuanbao reuses the
    running gateway adapter (persistent WebSocket). Logic mirrors
    send_weixin_direct: send text first, then iterate media_files by extension.
    """
    last_result: Optional[SendResult] = None

    # 1. Send text
    if message.strip():
        last_result = await adapter.send(chat_id, message)
        if not last_result.success:
            return {"error": f"Yuanbao send failed: {last_result.error}"}

    # 2. Iterate media_files, dispatch by file extension
    for media_path, _is_voice in media_files or []:
        ext = Path(media_path).suffix.lower()
        if ext in _YUANBAO_IMAGE_EXTS:
            last_result = await adapter.send_image_file(chat_id, media_path)
        else:
            last_result = await adapter.send_document(chat_id, media_path)

        if not last_result.success:
            return {"error": f"Yuanbao media send failed: {last_result.error}"}

    if last_result is None:
        return {"error": "No deliverable text or media remained after processing"}

    return {
        "success": True,
        "platform": "yuanbao",
        "chat_id": chat_id,
        "message_id": last_result.message_id if last_result else None,
    }
