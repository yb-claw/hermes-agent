"""
Yuanbao (Tencent IM Bot) platform adapter.

Connects to the Yuanbao WebSocket Gateway for inbound messages and uses the
REST API for outbound messages. Communication uses protobuf-encoded frames
over WebSocket (ConnMsg protocol).

Configuration in config.yaml:
    platforms:
      yuanbao:
        enabled: true
        extra:
          app_key: "your-app-key"          # or YUANBAO_APP_KEY env var
          app_secret: "your-app-secret"    # or YUANBAO_APP_SECRET env var
          api_domain: "api.example.com"    # or YUANBAO_API_DOMAIN env var
          ws_url: "wss://ws.example.com"   # or YUANBAO_WS_URL env var
          token: ""                        # pre-signed static token (optional)
          dm_policy: "open"                # open | allowlist | disabled
          allow_from: ["user_id_1"]
          require_mention: true            # group chat requires @mention

Reference: openclaw/extensions/yuanbao
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import random
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    import aiohttp

    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    aiohttp = None  # type: ignore[assignment]

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    httpx = None  # type: ignore[assignment]

try:
    from google.protobuf import descriptor_pb2, descriptor_pool, symbol_database
    from google.protobuf import json_format
    from google.protobuf.internal import decoder as _pb_decoder
    from google.protobuf.internal import encoder as _pb_encoder

    PROTOBUF_AVAILABLE = True
except ImportError:
    PROTOBUF_AVAILABLE = False

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.platforms.helpers import strip_markdown

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_API_DOMAIN = "bot.hunyuan.tencent.com"
DEFAULT_WS_GATEWAY_URL = "wss://ws-bot.hunyuan.tencent.com"
SIGN_TOKEN_PATH = "/api/v5/robotLogic/sign-token"

DEFAULT_API_TIMEOUT = 30.0
CONNECT_TIMEOUT_SECONDS = 20.0

RECONNECT_BACKOFF = [1, 2, 5, 10, 30, 60]
MAX_RECONNECT_ATTEMPTS = 100

MAX_MESSAGE_LENGTH = 4000
DEDUP_WINDOW_SECONDS = 300
DEDUP_MAX_SIZE = 1000

# Heartbeat
DEFAULT_HEARTBEAT_INTERVAL_S = 5
HEARTBEAT_TIMEOUT_THRESHOLD = 2

# Close codes that should not trigger reconnect
NO_RECONNECT_CLOSE_CODES = {4012, 4013, 4014, 4018, 4019, 4021}

# Auth failure codes that require token refresh
AUTH_FAILED_CODES = {41103, 41104, 41108}
AUTH_ALREADY_CODE = 41101
AUTH_RETRYABLE_CODES = {50400, 50503, 90001, 90003}

# Cache refresh margin: refresh token 5 minutes before expiry
CACHE_REFRESH_MARGIN_S = 5 * 60


def check_yuanbao_requirements() -> bool:
    """Check if Yuanbao runtime dependencies are available."""
    return AIOHTTP_AVAILABLE and HTTPX_AVAILABLE


def _coerce_list(value: Any) -> List[str]:
    """Coerce config values into a trimmed string list."""
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


# ---------------------------------------------------------------------------
# Lightweight protobuf codec for ConnMsg protocol
# ---------------------------------------------------------------------------
# The Yuanbao WebSocket protocol uses protobuf-encoded ConnMsg frames.
# We implement a minimal codec using raw protobuf wire format to avoid
# depending on compiled .proto files or the heavy google.protobuf library.
# ---------------------------------------------------------------------------

def _encode_varint(value: int) -> bytes:
    """Encode an integer as a protobuf varint."""
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)


def _decode_varint(data: bytes, pos: int) -> tuple:
    """Decode a varint from data at position. Returns (value, new_pos)."""
    result = 0
    shift = 0
    while pos < len(data):
        b = data[pos]
        result |= (b & 0x7F) << shift
        pos += 1
        if not (b & 0x80):
            return result, pos
        shift += 7
    raise ValueError("Truncated varint")


def _encode_length_delimited(field_number: int, data: bytes) -> bytes:
    """Encode a length-delimited protobuf field."""
    tag = _encode_varint((field_number << 3) | 2)
    length = _encode_varint(len(data))
    return tag + length + data


def _encode_varint_field(field_number: int, value: int) -> bytes:
    """Encode a varint protobuf field."""
    tag = _encode_varint((field_number << 3) | 0)
    return tag + _encode_varint(value)


def _encode_string_field(field_number: int, value: str) -> bytes:
    """Encode a string protobuf field."""
    return _encode_length_delimited(field_number, value.encode("utf-8"))


def _decode_fields(data: bytes) -> Dict[int, Any]:
    """Decode protobuf fields from binary data. Returns {field_number: value}."""
    fields: Dict[int, Any] = {}
    pos = 0
    while pos < len(data):
        try:
            tag, pos = _decode_varint(data, pos)
        except ValueError:
            break
        field_number = tag >> 3
        wire_type = tag & 0x07

        if wire_type == 0:  # varint
            value, pos = _decode_varint(data, pos)
            fields[field_number] = value
        elif wire_type == 2:  # length-delimited
            length, pos = _decode_varint(data, pos)
            fields[field_number] = data[pos : pos + length]
            pos += length
        elif wire_type == 1:  # 64-bit
            fields[field_number] = data[pos : pos + 8]
            pos += 8
        elif wire_type == 5:  # 32-bit
            fields[field_number] = data[pos : pos + 4]
            pos += 4
        else:
            break  # Unknown wire type
    return fields


class _ConnMsgCodec:
    """Minimal codec for the Yuanbao ConnMsg protobuf protocol.

    ConnMsg {
        Head head = 1;
        bytes data = 2;
    }

    Head {
        uint32 cmd_type = 1;
        string cmd = 2;
        uint32 seq_no = 3;
        string msg_id = 4;
        string module = 5;
        bool need_ack = 6;
        int32 status = 10;
    }
    """

    CMD_TYPE_REQUEST = 0
    CMD_TYPE_RESPONSE = 1
    CMD_TYPE_PUSH = 2
    CMD_TYPE_PUSH_ACK = 3

    CMD_AUTH_BIND = "auth-bind"
    CMD_PING = "ping"
    CMD_KICKOUT = "kickout"

    MODULE_CONN_ACCESS = "conn_access"
    BIZ_MODULE = "yuanbao_openclaw_proxy"

    _seq_counter = 0

    @classmethod
    def _next_seq(cls) -> int:
        cls._seq_counter += 1
        return cls._seq_counter

    @classmethod
    def encode_head(
        cls,
        cmd_type: int,
        cmd: str,
        msg_id: str,
        module: str = "",
        need_ack: bool = False,
        status: int = 0,
    ) -> bytes:
        """Encode a Head message."""
        result = bytearray()
        if cmd_type:
            result += _encode_varint_field(1, cmd_type)
        if cmd:
            result += _encode_string_field(2, cmd)
        result += _encode_varint_field(3, cls._next_seq())
        if msg_id:
            result += _encode_string_field(4, msg_id)
        if module:
            result += _encode_string_field(5, module)
        if need_ack:
            result += _encode_varint_field(6, 1)
        if status:
            result += _encode_varint_field(10, status)
        return bytes(result)

    @classmethod
    def decode_head(cls, data: bytes) -> Dict[str, Any]:
        """Decode a Head message."""
        fields = _decode_fields(data)
        return {
            "cmd_type": fields.get(1, 0),
            "cmd": fields.get(2, b"").decode("utf-8") if isinstance(fields.get(2), bytes) else "",
            "seq_no": fields.get(3, 0),
            "msg_id": fields.get(4, b"").decode("utf-8") if isinstance(fields.get(4), bytes) else "",
            "module": fields.get(5, b"").decode("utf-8") if isinstance(fields.get(5), bytes) else "",
            "need_ack": bool(fields.get(6, 0)),
            "status": fields.get(10, 0),
        }

    @classmethod
    def encode_conn_msg(cls, head: bytes, body: bytes) -> bytes:
        """Encode a ConnMsg (head + data)."""
        result = _encode_length_delimited(1, head)
        if body:
            result += _encode_length_delimited(2, body)
        return result

    @classmethod
    def decode_conn_msg(cls, data: bytes) -> Optional[Dict[str, Any]]:
        """Decode a ConnMsg. Returns {'head': dict, 'data': bytes}."""
        try:
            fields = _decode_fields(data)
            head_bytes = fields.get(1, b"")
            body_bytes = fields.get(2, b"")
            if not head_bytes:
                return None
            head = cls.decode_head(head_bytes)
            return {"head": head, "data": body_bytes}
        except Exception:
            return None

    @classmethod
    def build_auth_bind(
        cls,
        biz_id: str,
        uid: str,
        source: str,
        token: str,
        msg_id: str,
        route_env: str = "",
    ) -> bytes:
        """Build an auth-bind request frame."""
        # AuthInfo: uid=1, source=2, token=3
        auth_info = (
            _encode_string_field(1, uid)
            + _encode_string_field(2, source)
            + _encode_string_field(3, token)
        )
        # DeviceInfo: app_version=1, app_operation_system=2, instance_id=10
        device_info = (
            _encode_string_field(1, "hermes-agent")
            + _encode_string_field(2, "linux")
            + _encode_string_field(10, "16")
        )
        # AuthBindReq: biz_id=1, auth_info=2, device_info=3, env_name=5
        auth_bind_req = (
            _encode_string_field(1, biz_id)
            + _encode_length_delimited(2, auth_info)
            + _encode_length_delimited(3, device_info)
        )
        if route_env:
            auth_bind_req += _encode_string_field(5, route_env)

        head = cls.encode_head(
            cls.CMD_TYPE_REQUEST,
            cls.CMD_AUTH_BIND,
            msg_id,
            cls.MODULE_CONN_ACCESS,
        )
        return cls.encode_conn_msg(head, auth_bind_req)

    @classmethod
    def build_ping(cls, msg_id: str) -> bytes:
        """Build a ping request frame."""
        head = cls.encode_head(
            cls.CMD_TYPE_REQUEST,
            cls.CMD_PING,
            msg_id,
            cls.MODULE_CONN_ACCESS,
        )
        # PingReq is empty
        return cls.encode_conn_msg(head, b"")

    @classmethod
    def build_push_ack(cls, original_head: Dict[str, Any]) -> bytes:
        """Build a push ACK frame."""
        head = cls.encode_head(
            cls.CMD_TYPE_PUSH_ACK,
            original_head.get("cmd", ""),
            original_head.get("msg_id", ""),
            original_head.get("module", ""),
        )
        return cls.encode_conn_msg(head, b"")

    @classmethod
    def build_biz_request(
        cls, cmd: str, module: str, biz_data: bytes, msg_id: str
    ) -> bytes:
        """Build a business request ConnMsg."""
        head = cls.encode_head(cls.CMD_TYPE_REQUEST, cmd, msg_id, module)
        return cls.encode_conn_msg(head, biz_data)

    @classmethod
    def decode_auth_bind_rsp(cls, data: bytes) -> Dict[str, Any]:
        """Decode an AuthBindRsp."""
        fields = _decode_fields(data)
        return {
            "code": fields.get(1, 0),
            "message": fields.get(2, b"").decode("utf-8") if isinstance(fields.get(2), bytes) else "",
            "connect_id": fields.get(3, b"").decode("utf-8") if isinstance(fields.get(3), bytes) else "",
            "timestamp": fields.get(4, 0),
            "client_ip": fields.get(5, b"").decode("utf-8") if isinstance(fields.get(5), bytes) else "",
        }

    @classmethod
    def decode_ping_rsp(cls, data: bytes) -> Dict[str, Any]:
        """Decode a PingRsp."""
        fields = _decode_fields(data)
        return {
            "heart_interval": fields.get(1, 0),
            "timestamp": fields.get(2, 0),
        }

    @classmethod
    def decode_push_msg(cls, data: bytes) -> Optional[Dict[str, Any]]:
        """Decode a PushMsg (cmd, module, msg_id, data)."""
        fields = _decode_fields(data)
        cmd = fields.get(1, b"")
        module = fields.get(2, b"")
        if not cmd and not module:
            return None
        return {
            "cmd": cmd.decode("utf-8") if isinstance(cmd, bytes) else "",
            "module": module.decode("utf-8") if isinstance(module, bytes) else "",
            "msg_id": fields.get(3, b"").decode("utf-8") if isinstance(fields.get(3), bytes) else "",
            "data": fields.get(4, b""),
        }

    @classmethod
    def decode_directed_push(cls, data: bytes) -> Optional[Dict[str, Any]]:
        """Decode a DirectedPush (type, content)."""
        fields = _decode_fields(data)
        push_type = fields.get(1, None)
        content = fields.get(2, b"")
        if push_type is None and not content:
            return None
        return {
            "type": push_type,
            "content": content.decode("utf-8") if isinstance(content, bytes) else "",
        }

    @classmethod
    def decode_biz_rsp(cls, data: bytes) -> Dict[str, Any]:
        """Decode a generic business response (code + message)."""
        fields = _decode_fields(data)
        return {
            "code": fields.get(1, 0),
            "message": fields.get(2, b"").decode("utf-8") if isinstance(fields.get(2), bytes) else "",
        }


# Biz command constants
BIZ_CMD_SEND_C2C = "send_c2c_message"
BIZ_CMD_SEND_GROUP = "send_group_message"
BIZ_CMD_SEND_PRIVATE_HEARTBEAT = "send_private_heartbeat"
BIZ_CMD_SEND_GROUP_HEARTBEAT = "send_group_heartbeat"
BIZ_CMD_QUERY_GROUP_INFO = "query_group_info"


def _encode_msg_body_element(elem: Dict[str, Any]) -> bytes:
    """Encode a single MsgBodyElement to protobuf bytes.

    InboundMessagePush.MsgBodyElement {
        string msg_type = 1;
        string msg_content_json = 2;  // JSON-serialized content
    }
    """
    result = bytearray()
    msg_type = elem.get("msg_type", "")
    if msg_type:
        result += _encode_string_field(1, msg_type)
    msg_content = elem.get("msg_content")
    if msg_content is not None:
        result += _encode_string_field(2, json.dumps(msg_content, ensure_ascii=False))
    return bytes(result)


def _encode_send_c2c_msg(
    to_account: str,
    msg_body: List[Dict[str, Any]],
    from_account: str = "",
    msg_random: int = 0,
    group_code: str = "",
    trace_id: str = "",
) -> bytes:
    """Encode a SendC2CMessageReq."""
    result = bytearray()
    result += _encode_string_field(1, to_account)
    for elem in msg_body:
        result += _encode_length_delimited(2, _encode_msg_body_element(elem))
    if from_account:
        result += _encode_string_field(3, from_account)
    if msg_random:
        result += _encode_varint_field(5, msg_random)
    if group_code:
        result += _encode_string_field(6, group_code)
    if trace_id:
        result += _encode_string_field(8, trace_id)
    return bytes(result)


def _encode_send_group_msg(
    group_code: str,
    msg_body: List[Dict[str, Any]],
    from_account: str = "",
    random_val: int = 0,
    ref_msg_id: str = "",
    trace_id: str = "",
) -> bytes:
    """Encode a SendGroupMessageReq."""
    result = bytearray()
    result += _encode_string_field(1, group_code)
    for elem in msg_body:
        result += _encode_length_delimited(2, _encode_msg_body_element(elem))
    if from_account:
        result += _encode_string_field(3, from_account)
    if random_val:
        result += _encode_string_field(5, str(random_val))
    if ref_msg_id:
        result += _encode_string_field(6, ref_msg_id)
    if trace_id:
        result += _encode_string_field(8, trace_id)
    return bytes(result)


def _decode_inbound_message(data: bytes) -> Optional[Dict[str, Any]]:
    """Decode an InboundMessagePush from protobuf bytes.

    Tries protobuf decoding first, falls back to JSON text decoding.
    """
    # Try JSON text decoding first (some push events are JSON strings)
    try:
        text = data.decode("utf-8")
        parsed = json.loads(text)
        if isinstance(parsed, dict) and (
            parsed.get("callback_command")
            or parsed.get("from_account")
            or parsed.get("msg_body")
        ):
            return parsed
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        pass

    # Try protobuf field decoding
    try:
        fields = _decode_fields(data)
        msg: Dict[str, Any] = {}

        # Map protobuf fields to InboundMessage fields
        # InboundMessagePush proto field mapping:
        # 1=callback_command, 2=from_account, 3=to_account, 4=sender_nickname,
        # 5=group_id, 6=group_code, 7=group_name, 8=msg_seq, 9=msg_random,
        # 10=msg_time, 11=msg_key, 12=msg_id, 15=msg_body (repeated),
        # 17=cloud_custom_data, 20=event_time, 21=bot_owner_id,
        # 30=claw_msg_type, 31=private_from_group_code, 40=log_ext
        _str_fields = {
            1: "callback_command", 2: "from_account", 3: "to_account",
            4: "sender_nickname", 5: "group_id", 6: "group_code",
            7: "group_name", 11: "msg_key", 12: "msg_id",
            17: "cloud_custom_data", 21: "bot_owner_id",
            31: "private_from_group_code",
        }
        _int_fields = {
            8: "msg_seq", 9: "msg_random", 10: "msg_time",
            20: "event_time", 30: "claw_msg_type",
        }

        for field_num, field_name in _str_fields.items():
            val = fields.get(field_num)
            if val is not None:
                msg[field_name] = val.decode("utf-8") if isinstance(val, bytes) else str(val)

        for field_num, field_name in _int_fields.items():
            val = fields.get(field_num)
            if val is not None:
                msg[field_name] = int(val) if not isinstance(val, int) else val

        # Try to decode msg_body (field 15) - it's repeated, but our simple
        # decoder only captures the last value. For now, try JSON parsing.
        body_raw = fields.get(15)
        if body_raw and isinstance(body_raw, bytes):
            try:
                body_fields = _decode_fields(body_raw)
                msg_type = body_fields.get(1, b"").decode("utf-8") if isinstance(body_fields.get(1), bytes) else ""
                content_json = body_fields.get(2, b"").decode("utf-8") if isinstance(body_fields.get(2), bytes) else ""
                if msg_type:
                    content = json.loads(content_json) if content_json else {}
                    msg["msg_body"] = [{"msg_type": msg_type, "msg_content": content}]
            except (json.JSONDecodeError, ValueError):
                pass

        # log_ext (field 40) - nested message with trace_id
        log_ext_raw = fields.get(40)
        if log_ext_raw and isinstance(log_ext_raw, bytes):
            try:
                ext_fields = _decode_fields(log_ext_raw)
                trace_id = ext_fields.get(1, b"")
                if trace_id:
                    msg["trace_id"] = trace_id.decode("utf-8") if isinstance(trace_id, bytes) else ""
            except Exception:
                pass

        if msg.get("callback_command") or msg.get("from_account") or msg.get("msg_body"):
            return msg
    except Exception:
        pass

    return None


# ---------------------------------------------------------------------------
# YuanbaoAdapter
# ---------------------------------------------------------------------------


class YuanbaoAdapter(BasePlatformAdapter):
    """Yuanbao (Tencent IM Bot) adapter using WebSocket + REST API."""

    SUPPORTS_MESSAGE_EDITING = False
    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.YUANBAO)

        extra = config.extra or {}
        self._app_key = str(extra.get("app_key") or os.getenv("YUANBAO_APP_KEY", "")).strip()
        self._app_secret = str(extra.get("app_secret") or os.getenv("YUANBAO_APP_SECRET", "")).strip()
        self._api_domain = str(extra.get("api_domain") or os.getenv("YUANBAO_API_DOMAIN", DEFAULT_API_DOMAIN)).strip()
        self._ws_url = str(extra.get("ws_url") or os.getenv("YUANBAO_WS_URL", DEFAULT_WS_GATEWAY_URL)).strip()
        self._static_token = str(extra.get("token") or os.getenv("YUANBAO_TOKEN", "")).strip()
        self._route_env = str(extra.get("route_env", "")).strip()
        self._require_mention = bool(extra.get("require_mention", True))

        # Auth/ACL policies
        self._dm_policy = str(extra.get("dm_policy", "open")).strip().lower()
        self._allow_from = _coerce_list(extra.get("allow_from") or extra.get("allowFrom"))

        # Connection state
        self._ws_session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._http_client: Optional[httpx.AsyncClient] = None
        self._listen_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

        # Token cache
        self._sign_token: Optional[str] = None
        self._bot_id: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._token_lock = asyncio.Lock()

        # Heartbeat
        self._heartbeat_interval_s: float = DEFAULT_HEARTBEAT_INTERVAL_S
        self._heartbeat_ack_received: bool = True
        self._heartbeat_timeout_count: int = 0
        self._last_heartbeat_at: float = 0.0

        # Connection
        self._connect_id: Optional[str] = None
        self._chat_type_map: Dict[str, str] = {}  # chat_id → "c2c"|"group"

        # Request/response correlation
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._seen_messages: Dict[str, float] = {}

        # Reconnect state
        self._reconnect_attempts: int = 0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return "Yuanbao"

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        """Sign token, connect WebSocket, and authenticate."""
        if not AIOHTTP_AVAILABLE:
            message = "Yuanbao startup failed: aiohttp not installed"
            self._set_fatal_error("yuanbao_missing_dependency", message, retryable=True)
            logger.warning("[%s] %s. Run: pip install aiohttp", self.name, message)
            return False
        if not HTTPX_AVAILABLE:
            message = "Yuanbao startup failed: httpx not installed"
            self._set_fatal_error("yuanbao_missing_dependency", message, retryable=True)
            logger.warning("[%s] %s. Run: pip install httpx", self.name, message)
            return False
        if not self._static_token and (not self._app_key or not self._app_secret):
            message = "Yuanbao startup failed: YUANBAO_APP_KEY and YUANBAO_APP_SECRET are required (or provide YUANBAO_TOKEN)"
            self._set_fatal_error("yuanbao_missing_credentials", message, retryable=True)
            logger.warning("[%s] %s", self.name, message)
            return False

        # Prevent duplicate connections
        if not self._acquire_platform_lock("yuanbao-appkey", self._app_key or self._static_token, "Yuanbao app key"):
            return False

        try:
            self._http_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)

            # Get sign token
            await self._ensure_token()

            # Open WebSocket
            await self._open_ws()

            # Start listeners
            self._listen_task = asyncio.create_task(self._listen_loop())
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            self._mark_connected()
            logger.info("[%s] Connected (bot_id=%s)", self.name, self._bot_id or "unknown")
            return True
        except Exception as exc:
            message = f"Yuanbao startup failed: {exc}"
            self._set_fatal_error("yuanbao_connect_error", message, retryable=True)
            logger.error("[%s] %s", self.name, message, exc_info=True)
            await self._cleanup()
            self._release_platform_lock()
            return False

    async def disconnect(self) -> None:
        """Close all connections and stop listeners."""
        self._running = False
        self._mark_disconnected()

        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        await self._cleanup()
        self._release_platform_lock()
        logger.info("[%s] Disconnected", self.name)

    async def _cleanup(self) -> None:
        """Close WebSocket and HTTP sessions."""
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = None

        if self._ws_session and not self._ws_session.closed:
            await self._ws_session.close()
        self._ws_session = None

        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

        # Fail pending requests
        for fut in self._pending_requests.values():
            if not fut.done():
                fut.set_exception(RuntimeError("Disconnected"))
        self._pending_requests.clear()

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    async def _ensure_token(self) -> str:
        """Return a valid sign token, refreshing if needed."""
        if self._static_token:
            self._sign_token = self._static_token
            return self._static_token

        if self._sign_token and time.time() < self._token_expires_at - CACHE_REFRESH_MARGIN_S:
            return self._sign_token

        async with self._token_lock:
            if self._sign_token and time.time() < self._token_expires_at - CACHE_REFRESH_MARGIN_S:
                return self._sign_token

            token_data = await self._fetch_sign_token()
            self._sign_token = token_data["token"]
            self._bot_id = token_data.get("bot_id") or self._bot_id
            duration = token_data.get("duration", 0)
            if duration > 0:
                self._token_expires_at = time.time() + duration
            logger.info("[%s] Sign token refreshed, bot_id=%s, duration=%ds",
                        self.name, self._bot_id, duration)
            return self._sign_token

    async def _fetch_sign_token(self) -> Dict[str, Any]:
        """Call the sign-token API to get a new token."""
        url = f"https://{self._api_domain}{SIGN_TOKEN_PATH}"

        nonce = uuid.uuid4().hex
        # Generate Beijing time ISO 8601 timestamp
        bj_offset = 8 * 3600
        bj_time = datetime.fromtimestamp(time.time() + bj_offset, tz=timezone.utc)
        timestamp = bj_time.strftime("%Y-%m-%dT%H:%M:%S+08:00")

        # Compute HMAC-SHA256 signature
        plain = nonce + timestamp + self._app_key + self._app_secret
        signature = hmac.new(
            self._app_secret.encode("utf-8"),
            plain.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        body = {
            "app_key": self._app_key,
            "nonce": nonce,
            "signature": signature,
            "timestamp": timestamp,
        }

        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self._route_env:
            headers["x-route-env"] = self._route_env

        for attempt in range(3):
            try:
                resp = await self._http_client.post(
                    url, json=body, headers=headers, timeout=DEFAULT_API_TIMEOUT,
                )
                resp.raise_for_status()
                result = resp.json()
            except Exception as exc:
                if attempt < 2:
                    await asyncio.sleep(1.0 * (attempt + 1))
                    continue
                raise RuntimeError(f"sign-token failed: {exc}") from exc

            code = result.get("code", -1)
            if code == 0:
                return result.get("data", result)
            if code == 10099 and attempt < 2:  # retryable
                await asyncio.sleep(1.0)
                continue
            raise RuntimeError(f"sign-token error: code={code}, msg={result.get('msg', '')}")

        raise RuntimeError("sign-token failed: max retries exceeded")

    def _get_auth_headers(self) -> Dict[str, str]:
        """Return auth headers for REST API calls."""
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "X-ID": self._bot_id or "",
            "X-Token": self._sign_token or "",
            "X-Source": "bot",
        }
        if self._route_env:
            headers["X-Route-Env"] = self._route_env
        return headers

    # ------------------------------------------------------------------
    # WebSocket lifecycle
    # ------------------------------------------------------------------

    async def _open_ws(self) -> None:
        """Open a WebSocket connection to the Yuanbao gateway."""
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = None
        if self._ws_session and not self._ws_session.closed:
            await self._ws_session.close()
        self._ws_session = None

        self._ws_session = aiohttp.ClientSession()
        self._ws = await self._ws_session.ws_connect(
            self._ws_url,
            timeout=CONNECT_TIMEOUT_SECONDS,
        )
        logger.info("[%s] WebSocket connected to %s", self.name, self._ws_url)

        # Send auth-bind immediately
        await self._send_auth_bind()

    async def _send_auth_bind(self) -> None:
        """Send auth-bind request after WebSocket connects."""
        token = await self._ensure_token()
        uid = self._bot_id or ""
        msg_id = uuid.uuid4().hex

        frame = _ConnMsgCodec.build_auth_bind(
            biz_id="ybBot",
            uid=uid,
            source="bot",
            token=token,
            msg_id=msg_id,
            route_env=self._route_env,
        )
        if self._ws and not self._ws.closed:
            await self._ws.send_bytes(frame)
            logger.info("[%s] Auth-bind sent (uid=%s)", self.name, uid)

    async def _listen_loop(self) -> None:
        """Read WebSocket events and reconnect on errors."""
        backoff_idx = 0

        while self._running:
            try:
                await self._read_events()
                backoff_idx = 0
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if not self._running:
                    return
                logger.warning("[%s] WebSocket error: %s", self.name, exc)
                self._mark_disconnected()
                self._fail_pending("Connection interrupted")

                if backoff_idx >= MAX_RECONNECT_ATTEMPTS:
                    logger.error("[%s] Max reconnect attempts reached", self.name)
                    return

                if await self._reconnect(backoff_idx):
                    backoff_idx = 0
                else:
                    backoff_idx += 1

    async def _read_events(self) -> None:
        """Read WebSocket binary frames until connection closes."""
        if not self._ws:
            raise RuntimeError("WebSocket not connected")

        while self._running and self._ws and not self._ws.closed:
            msg = await self._ws.receive()
            if msg.type == aiohttp.WSMsgType.BINARY:
                conn_msg = _ConnMsgCodec.decode_conn_msg(msg.data)
                if conn_msg:
                    await self._dispatch_conn_msg(conn_msg)
            elif msg.type == aiohttp.WSMsgType.TEXT:
                # Some push events may arrive as text
                try:
                    data = msg.data.encode("utf-8") if isinstance(msg.data, str) else msg.data
                    conn_msg = _ConnMsgCodec.decode_conn_msg(data)
                    if conn_msg:
                        await self._dispatch_conn_msg(conn_msg)
                except Exception:
                    logger.debug("[%s] Unparseable text frame", self.name)
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                raise RuntimeError(f"WebSocket closed: {msg.type}")

    async def _reconnect(self, backoff_idx: int) -> bool:
        """Attempt to reconnect. Returns True on success."""
        delay = RECONNECT_BACKOFF[min(backoff_idx, len(RECONNECT_BACKOFF) - 1)]
        logger.info("[%s] Reconnecting in %ds (attempt %d)...",
                     self.name, delay, backoff_idx + 1)
        await asyncio.sleep(delay)

        try:
            # Force token refresh on reconnect
            self._sign_token = None
            self._token_expires_at = 0.0
            await self._ensure_token()
            await self._open_ws()
            self._mark_connected()
            logger.info("[%s] Reconnected", self.name)
            return True
        except Exception as exc:
            logger.warning("[%s] Reconnect failed: %s", self.name, exc)
            return False

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeat pings."""
        try:
            # Initial delay before first ping
            await asyncio.sleep(5.0)

            while self._running:
                if not self._heartbeat_ack_received:
                    self._heartbeat_timeout_count += 1
                    if self._heartbeat_timeout_count >= HEARTBEAT_TIMEOUT_THRESHOLD:
                        logger.warning("[%s] Heartbeat timeout (%d consecutive), triggering reconnect",
                                       self.name, self._heartbeat_timeout_count)
                        self._heartbeat_timeout_count = 0
                        # Close WS to trigger reconnect in listen_loop
                        if self._ws and not self._ws.closed:
                            await self._ws.close()
                        break
                else:
                    self._heartbeat_timeout_count = 0

                # Send ping
                msg_id = uuid.uuid4().hex
                frame = _ConnMsgCodec.build_ping(msg_id)
                if self._ws and not self._ws.closed:
                    self._heartbeat_ack_received = False
                    self._last_heartbeat_at = time.monotonic()
                    await self._ws.send_bytes(frame)

                await asyncio.sleep(max(self._heartbeat_interval_s - 1, 1))
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # ConnMsg dispatch
    # ------------------------------------------------------------------

    async def _dispatch_conn_msg(self, conn_msg: Dict[str, Any]) -> None:
        """Route inbound ConnMsg payloads."""
        head = conn_msg.get("head", {})
        data = conn_msg.get("data", b"")
        cmd_type = head.get("cmd_type", 0)
        cmd = head.get("cmd", "")

        # Response (cmd_type=1)
        if cmd_type == _ConnMsgCodec.CMD_TYPE_RESPONSE:
            if cmd == _ConnMsgCodec.CMD_AUTH_BIND:
                self._on_auth_bind_response(head, data)
            elif cmd == _ConnMsgCodec.CMD_PING:
                self._on_ping_response(head, data)
            else:
                self._on_biz_response(head, data)
            return

        # Push (cmd_type=2)
        if cmd_type == _ConnMsgCodec.CMD_TYPE_PUSH:
            await self._on_push(head, data)
            return

    def _on_auth_bind_response(self, head: Dict[str, Any], data: bytes) -> None:
        """Handle auth-bind response."""
        rsp = _ConnMsgCodec.decode_auth_bind_rsp(data) if data else {}
        code = rsp.get("code", 0)
        status = head.get("status", 0)

        if status and status != 0 and code != AUTH_ALREADY_CODE:
            logger.error("[%s] Auth-bind failed: status=%s, code=%s, message=%s",
                         self.name, status, code, rsp.get("message", ""))
            return

        if code != 0 and code != AUTH_ALREADY_CODE:
            logger.error("[%s] Auth-bind response error: code=%s, message=%s",
                         self.name, code, rsp.get("message", ""))
            return

        self._connect_id = rsp.get("connect_id", "")
        self._reconnect_attempts = 0
        self._heartbeat_ack_received = True
        self._heartbeat_timeout_count = 0
        logger.info("[%s] Auth success: connect_id=%s", self.name, self._connect_id)

    def _on_ping_response(self, head: Dict[str, Any], data: bytes) -> None:
        """Handle ping response (heartbeat ACK)."""
        self._heartbeat_ack_received = True
        self._heartbeat_timeout_count = 0
        rsp = _ConnMsgCodec.decode_ping_rsp(data) if data else {}
        interval = rsp.get("heart_interval", 0)
        if interval and interval > 1:
            self._heartbeat_interval_s = float(interval)

    def _on_biz_response(self, head: Dict[str, Any], data: bytes) -> None:
        """Handle business response — match by msg_id."""
        msg_id = head.get("msg_id", "")
        if not msg_id:
            return

        fut = self._pending_requests.pop(msg_id, None)
        if not fut or fut.done():
            return

        rsp = _ConnMsgCodec.decode_biz_rsp(data) if data else {"code": head.get("status", 0)}
        rsp["msg_id"] = msg_id
        if head.get("status", 0) != 0:
            rsp["code"] = head["status"]
        fut.set_result(rsp)

    async def _on_push(self, head: Dict[str, Any], data: bytes) -> None:
        """Handle push messages."""
        # Send ACK if needed
        if head.get("need_ack"):
            ack = _ConnMsgCodec.build_push_ack(head)
            if self._ws and not self._ws.closed:
                await self._ws.send_bytes(ack)

        # Handle kickout
        if head.get("cmd") == _ConnMsgCodec.CMD_KICKOUT:
            logger.warning("[%s] Kicked out", self.name)
            return

        # Try to decode as PushMsg first
        push_msg = _ConnMsgCodec.decode_push_msg(data)
        if push_msg and (push_msg.get("cmd") or push_msg.get("module")):
            raw_data = push_msg.get("data", b"")
            await self._handle_push_data(raw_data, data)
            return

        # Try DirectedPush
        directed = _ConnMsgCodec.decode_directed_push(data)
        if directed and directed.get("content"):
            await self._handle_push_content(directed["content"])
            return

        # Fallback: try the raw data
        if data:
            await self._handle_push_data(data, data)

    async def _handle_push_data(self, raw_data: bytes, conn_data: bytes) -> None:
        """Decode push data and forward as inbound message."""
        # Try decoding from conn_data first, then raw_data
        for data_bytes in (conn_data, raw_data):
            if not data_bytes:
                continue
            msg = _decode_inbound_message(data_bytes)
            if msg:
                await self._process_inbound(msg)
                return

    async def _handle_push_content(self, content: str) -> None:
        """Handle DirectedPush content (JSON string)."""
        try:
            parsed = json.loads(content)
        except (json.JSONDecodeError, ValueError):
            parsed = None

        if parsed and isinstance(parsed, dict):
            if parsed.get("msg_body") or parsed.get("callback_command") or parsed.get("from_account"):
                await self._process_inbound(parsed)
                return

        # If not a full message, wrap text content
        if content.strip():
            msg = {
                "callback_command": "C2C.CallbackAfterSendMsg",
                "msg_body": [{"msg_type": "TIMTextElem", "msg_content": {"text": content}}],
            }
            await self._process_inbound(msg)

    # ------------------------------------------------------------------
    # Inbound message processing
    # ------------------------------------------------------------------

    async def _process_inbound(self, msg: Dict[str, Any]) -> None:
        """Convert a Yuanbao inbound message to a MessageEvent and forward."""
        callback_cmd = msg.get("callback_command", "")
        from_account = msg.get("from_account", "")
        group_code = msg.get("group_code", "")

        # Skip recall/system events
        if "Recall" in callback_cmd or "WithDraw" in callback_cmd:
            logger.debug("[%s] Skipping recall event: %s", self.name, callback_cmd)
            return

        # Determine chat type
        is_group = bool(group_code) or "Group." in callback_cmd

        # Extract text from msg_body
        text = ""
        msg_body = msg.get("msg_body", [])
        if isinstance(msg_body, list):
            for elem in msg_body:
                if not isinstance(elem, dict):
                    continue
                msg_type = elem.get("msg_type", "")
                content = elem.get("msg_content", {})
                if not isinstance(content, dict):
                    try:
                        content = json.loads(content) if isinstance(content, str) else {}
                    except (json.JSONDecodeError, ValueError):
                        content = {}

                if msg_type == "TIMTextElem":
                    text += content.get("text", "")

        text = text.strip()
        if not text:
            return

        # Dedup
        msg_key = msg.get("msg_key") or msg.get("msg_id") or ""
        if msg_key and self._is_duplicate(msg_key):
            return

        # Build chat_id and user_id
        chat_id = group_code if is_group else from_account
        user_id = from_account
        if not chat_id:
            return

        self._chat_type_map[chat_id] = "group" if is_group else "c2c"

        # Build source and event
        source = self.build_source(
            chat_id=chat_id,
            user_id=user_id,
            user_name=msg.get("sender_nickname") or None,
            chat_type="group" if is_group else "dm",
            chat_name=msg.get("group_name") or None,
        )

        event = MessageEvent(
            source=source,
            text=text,
            message_type=MessageType.TEXT,
            raw_message=msg,
            message_id=msg_key or uuid.uuid4().hex[:12],
            timestamp=self._parse_yuanbao_timestamp(msg.get("msg_time")),
        )
        await self.handle_message(event)

    # ------------------------------------------------------------------
    # Outbound messaging via WebSocket
    # ------------------------------------------------------------------

    async def _ws_send_and_wait(
        self, cmd: str, data: bytes, timeout: float = 30.0
    ) -> Dict[str, Any]:
        """Send a business request via WebSocket and wait for response."""
        msg_id = uuid.uuid4().hex
        frame = _ConnMsgCodec.build_biz_request(
            cmd, _ConnMsgCodec.BIZ_MODULE, data, msg_id
        )

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_requests[msg_id] = fut

        try:
            if not self._ws or self._ws.closed:
                raise RuntimeError("WebSocket not connected")
            await self._ws.send_bytes(frame)
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_requests.pop(msg_id, None)
            raise RuntimeError(f"WS request timeout ({timeout}s) for msg_id={msg_id}")
        except Exception:
            self._pending_requests.pop(msg_id, None)
            raise

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a text message to a Yuanbao user or group."""
        del metadata

        if not self.is_connected:
            return SendResult(success=False, error="Not connected")

        if not content or not content.strip():
            return SendResult(success=True)

        formatted = self.format_message(content)
        chunks = self.truncate_message(formatted, self.MAX_MESSAGE_LENGTH)

        last_result = SendResult(success=False, error="No chunks")
        for chunk in chunks:
            last_result = await self._send_chunk(chat_id, chunk, reply_to)
            if not last_result.success:
                return last_result
            reply_to = None
        return last_result

    async def _send_chunk(
        self, chat_id: str, content: str, reply_to: Optional[str] = None,
    ) -> SendResult:
        """Send a single text chunk via WebSocket."""
        chat_type = self._guess_chat_type(chat_id)
        msg_body = [{"msg_type": "TIMTextElem", "msg_content": {"text": content}}]
        msg_random = random.randint(0, 4294967295)

        try:
            if chat_type == "group":
                data = _encode_send_group_msg(
                    group_code=chat_id,
                    msg_body=msg_body,
                    from_account=self._bot_id or "",
                    random_val=msg_random,
                    ref_msg_id=reply_to or "",
                )
                rsp = await self._ws_send_and_wait(BIZ_CMD_SEND_GROUP, data)
            else:
                data = _encode_send_c2c_msg(
                    to_account=chat_id,
                    msg_body=msg_body,
                    from_account=self._bot_id or "",
                    msg_random=msg_random,
                )
                rsp = await self._ws_send_and_wait(BIZ_CMD_SEND_C2C, data)

            code = rsp.get("code", 0)
            if code == 0:
                return SendResult(success=True, message_id=rsp.get("msg_id", ""))
            else:
                error_msg = rsp.get("message", f"code={code}")
                logger.error("[%s] Send failed: %s", self.name, error_msg)
                return SendResult(success=False, error=error_msg)
        except Exception as exc:
            logger.error("[%s] Send error: %s", self.name, exc)
            return SendResult(success=False, error=str(exc))

    # ------------------------------------------------------------------
    # Typing indicator
    # ------------------------------------------------------------------

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Send a typing heartbeat (reply status heartbeat)."""
        del metadata
        if not self.is_connected or not self._bot_id:
            return

        chat_type = self._guess_chat_type(chat_id)
        try:
            if chat_type == "group":
                # SendGroupHeartbeat: from_account=1, to_account=2, group_code=3, send_time=4, heartbeat=5
                data = (
                    _encode_string_field(1, self._bot_id)
                    + _encode_string_field(2, chat_id)
                    + _encode_string_field(3, chat_id)
                    + _encode_varint_field(4, int(time.time()))
                    + _encode_varint_field(5, 1)  # RUNNING
                )
                await self._ws_send_and_wait(BIZ_CMD_SEND_GROUP_HEARTBEAT, data, timeout=10.0)
            else:
                # SendPrivateHeartbeat: from_account=1, to_account=2, heartbeat=3
                data = (
                    _encode_string_field(1, self._bot_id)
                    + _encode_string_field(2, chat_id)
                    + _encode_varint_field(3, 1)  # RUNNING
                )
                await self._ws_send_and_wait(BIZ_CMD_SEND_PRIVATE_HEARTBEAT, data, timeout=10.0)
        except Exception as exc:
            logger.debug("[%s] send_typing failed: %s", self.name, exc)

    # ------------------------------------------------------------------
    # Format
    # ------------------------------------------------------------------

    def format_message(self, content: str) -> str:
        """Format message for Yuanbao. Strip markdown since Yuanbao uses
        Tencent IM which has limited markdown support."""
        return strip_markdown(content)

    # ------------------------------------------------------------------
    # Chat info
    # ------------------------------------------------------------------

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return chat metadata."""
        chat_type = self._guess_chat_type(chat_id)
        return {
            "name": chat_id,
            "type": "group" if chat_type == "group" else "dm",
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _guess_chat_type(self, chat_id: str) -> str:
        """Determine chat type from stored inbound metadata."""
        return self._chat_type_map.get(chat_id, "c2c")

    def _fail_pending(self, reason: str) -> None:
        """Fail all pending response futures."""
        for fut in self._pending_requests.values():
            if not fut.done():
                fut.set_exception(RuntimeError(reason))
        self._pending_requests.clear()

    @staticmethod
    def _parse_yuanbao_timestamp(raw: Any) -> datetime:
        """Parse Yuanbao timestamp (Unix seconds or None)."""
        if raw is None:
            return datetime.now(tz=timezone.utc)
        try:
            return datetime.fromtimestamp(int(raw), tz=timezone.utc)
        except (ValueError, TypeError, OSError):
            return datetime.now(tz=timezone.utc)

    def _is_duplicate(self, msg_id: str) -> bool:
        """Check if a message ID has been seen recently."""
        now = time.time()
        if len(self._seen_messages) > DEDUP_MAX_SIZE:
            cutoff = now - DEDUP_WINDOW_SECONDS
            self._seen_messages = {
                key: ts for key, ts in self._seen_messages.items() if ts > cutoff
            }
        if msg_id in self._seen_messages:
            return True
        self._seen_messages[msg_id] = now
        return False
