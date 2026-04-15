"""
Yuanbao platform adapter.

Connects to the Yuanbao WebSocket gateway, handles authentication (AUTH_BIND),
heartbeat, and reconnection.  Message receive (T05) and send (T06) logic
will be added in follow-up tasks.

Configuration in config.yaml (or via env vars):
    platforms:
      yuanbao:
        yuanbao_app_key: "..."        # or YUANBAO_APP_KEY
        yuanbao_app_secret: "..."     # or YUANBAO_APP_SECRET
        yuanbao_bot_id: "..."         # or YUANBAO_BOT_ID  (optional, returned by sign-token)
        yuanbao_ws_gateway_url: "wss://..." # or YUANBAO_WS_GATEWAY_URL
        yuanbao_sign_token_url: "https://..." # or YUANBAO_SIGN_TOKEN_URL
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import time
import uuid
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Optional

import httpx

try:
    import websockets
    import websockets.exceptions
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
    websockets = None  # type: ignore[assignment]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult
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
    decode_conn_msg,
    decode_inbound_push,
    decode_push_msg,
    decode_directed_push,
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
from gateway.session import SessionSource

logger = logging.getLogger(__name__)

DEFAULT_WS_GATEWAY_URL = "wss://yuanbao.tencent.com/ws"
DEFAULT_SIGN_TOKEN_URL = "https://yuanbao.tencent.com/api/sign-token"

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

# Maximum number of distinct chat IDs tracked in _chat_locks / _group_history.
# Oldest entry is evicted when the limit is reached to prevent unbounded growth.
_CHAT_DICT_MAX_SIZE = 1000

# Reply Heartbeat 配置
REPLY_HEARTBEAT_INTERVAL_S = 2.0   # 每 2 秒续发 RUNNING
REPLY_HEARTBEAT_TIMEOUT_S = 30.0   # 30 秒无活动自动停止

# Outbound Queue 配置
OUTBOUND_MIN_CHARS = 200           # 最小缓冲门槛（字符数）才触发 flush
OUTBOUND_FLUSH_DELAY_S = 0.5      # flush 延迟（秒），收集更多文本后再发

# Reply-to 引用配置
REPLY_REF_TTL_S = 300.0            # 引用去重 TTL（5 分钟）
_REPLY_REF_MAX_ENTRIES = 500       # 引用去重字典最大容量

# 占位符消息过滤（当无实际媒体内容时跳过这些纯占位符）
_SKIPPABLE_PLACEHOLDERS = frozenset({
    "[image]", "[图片]", "[file]", "[文件]",
    "[video]", "[视频]", "[voice]", "[语音]",
})


class _OutboundQueue:
    """
    出站文本缓冲队列（per chat_id）。

    参考 feishu.py 的 FeishuBatchState 内联模式。
    实现 merge-text 策略：fence-aware 文本缓冲 + 达到 minChars 门槛时发送。

    用法：
        q = _OutboundQueue(adapter, chat_id, reply_to)
        q.push(text1)
        q.push(text2)
        await q.flush()     # fence-aware 合并后发送
        await q.drain_now() # tool_call 前提前投递
        q.abort()           # 丢弃缓冲
    """

    def __init__(
        self,
        adapter: "YuanbaoAdapter",
        chat_id: str,
        reply_to: Optional[str] = None,
    ) -> None:
        self._adapter = adapter
        self._chat_id = chat_id
        self._reply_to = reply_to
        self._buffer: list[str] = []
        self._total_chars: int = 0
        self._first_send: bool = True  # 只有第一次 send 带 reply_to
        self._flushed: bool = False

    def push(self, text: str) -> None:
        """将文本加入缓冲。"""
        if not text:
            return
        self._buffer.append(text)
        self._total_chars += len(text)

    async def flush(self) -> SendResult:
        """
        将缓冲中的文本 fence-aware 合并后发送。

        合并逻辑：
        1. 拼接所有缓冲文本（使用 _infer_block_separator 推断分隔符）
        2. 调用 _strip_outer_markdown_fence 剥除外层围栏
        3. 调用 _sanitize_markdown_table 净化表格
        4. 调用 truncate_message 分片（保留代码块围栏完整性）
        5. 调用 _merge_block_streaming_fences 合并未闭合围栏
        6. 逐片发送
        """
        if not self._buffer:
            return SendResult(success=True)

        # 1. 合并缓冲
        merged = self._buffer[0]
        for i in range(1, len(self._buffer)):
            sep = _infer_block_separator(merged, self._buffer[i])
            merged = merged + sep + self._buffer[i]
        self._buffer.clear()
        self._total_chars = 0
        self._flushed = True

        # 2. 预处理
        merged = _strip_outer_markdown_fence(merged)
        merged = _sanitize_markdown_table(merged)

        # 3. 分片（使用 base class truncate_message 保留代码块围栏完整性）
        chunks = self._adapter.truncate_message(merged, self._adapter.MAX_TEXT_CHUNK)

        # 4. 合并未闭合围栏（处理流式输出产生的断裂围栏）
        chunks = _merge_block_streaming_fences(chunks)

        # 5. 发送
        lock = self._adapter._get_chat_lock(self._chat_id)
        async with lock:
            for chunk in chunks:
                r_to = self._reply_to if self._first_send else None
                self._first_send = False
                result = await self._adapter._send_text_chunk(self._chat_id, chunk, r_to)
                if not result.success:
                    return result
        return SendResult(success=True)

    async def drain_now(self) -> SendResult:
        """
        立即投递缓冲内容（tool_call 前调用，保证文本在媒体前发出）。
        """
        return await self.flush()

    def abort(self) -> None:
        """丢弃缓冲。"""
        self._buffer.clear()
        self._total_chars = 0

    @property
    def is_empty(self) -> bool:
        return self._total_chars == 0

    @property
    def chars_buffered(self) -> int:
        return self._total_chars


class YuanbaoAdapter(BasePlatformAdapter):
    """Yuanbao AI Bot adapter backed by a persistent WebSocket connection."""

    PLATFORM = Platform.YUANBAO
    MAX_TEXT_CHUNK: int = 5000  # 元宝单条消息字符上限

    def __init__(self, config: PlatformConfig, **kwargs: Any) -> None:
        super().__init__(config, Platform.YUANBAO)

        # Credentials / endpoints from config or environment
        self._app_key: str = (
            config.yuanbao_app_key or os.getenv("YUANBAO_APP_KEY", "")
        ).strip()
        self._app_secret: str = (
            config.yuanbao_app_secret or os.getenv("YUANBAO_APP_SECRET", "")
        ).strip()
        self._bot_id: Optional[str] = (
            config.yuanbao_bot_id or os.getenv("YUANBAO_BOT_ID", "") or None
        )
        self._ws_gateway_url: str = (
            config.yuanbao_ws_gateway_url
            or os.getenv("YUANBAO_WS_GATEWAY_URL", DEFAULT_WS_GATEWAY_URL)
        ).strip()
        self._sign_token_url: str = (
            config.yuanbao_sign_token_url
            or os.getenv("YUANBAO_SIGN_TOKEN_URL", DEFAULT_SIGN_TOKEN_URL)
        ).strip()

        # Runtime state
        self._ws = None                          # websockets connection
        self._connect_id: Optional[str] = None  # received from BIND_ACK
        self._pending_acks: Dict[str, asyncio.Future] = {}  # req_id (str) -> Future
        self._chat_locks: Dict[str, asyncio.Lock] = {}  # per-chat-id 锁（懒初始化，上限 _CHAT_DICT_MAX_SIZE）
        self._send_lock: asyncio.Lock = asyncio.Lock()  # guards concurrent WS sends

        # Background tasks
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._recv_task: Optional[asyncio.Task] = None

        # Reconnection state
        self._reconnect_attempts: int = 0
        self._consecutive_hb_timeouts: int = 0  # heartbeat timeout counter
        self._pending_pong: Optional[asyncio.Future] = None  # current in-flight ping future

        # Internal sequence counter (separate from proto-level next_seq_no)
        self._seq: int = 0

        # Outbound queues: chat_id -> _OutboundQueue (session-level lifecycle)
        self._outbound_queues: Dict[str, _OutboundQueue] = {}

        # Group chat history: chat_id -> deque of {sender_id, text, ts}
        # Upper-bounded to _CHAT_DICT_MAX_SIZE entries; oldest evicted first.
        self._group_history: Dict[str, deque] = {}

        # Reply Heartbeat state: chat_id -> asyncio.Task (the periodic sender)
        self._reply_heartbeat_tasks: Dict[str, asyncio.Task] = {}

        # Member cache: group_code -> [{"user_id":..., "nickname":..., ...}, ...]
        # Populated by get_group_member_list(), used by @mention resolution.
        self._member_cache: Dict[str, list] = {}

        # Reply-to dedup: inbound_msg_id -> expire_ts
        # Only the first outbound message carries refMsgId for a given inbound msg.
        self._reply_ref_used: Dict[str, float] = {}

        # replyToMode config: 'off' | 'first' | 'all' (default: 'first')
        self._reply_to_mode: str = (
            getattr(config, 'yuanbao_reply_to_mode', None)
            or os.getenv("YUANBAO_REPLY_TO_MODE", "first")
        ).strip().lower()

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
                "YUANBAO_APP_KEY and YUANBAO_APP_SECRET are required"
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

        try:
            # Step 1: Get sign token
            logger.info("[%s] Fetching sign token from %s", self.name, self._sign_token_url)
            token_data = await get_sign_token(
                self._app_key, self._app_secret, self._sign_token_url
            )

            # Update bot_id if returned by sign-token API
            if token_data.get("bot_id"):
                self._bot_id = str(token_data["bot_id"])

            # Step 2: Open WebSocket connection (disable built-in ping/pong)
            logger.info("[%s] Connecting to %s", self.name, self._ws_gateway_url)
            self._ws = await asyncio.wait_for(
                websockets.connect(  # type: ignore[attr-defined]
                    self._ws_gateway_url,
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

            try:
                from tools.yuanbao_tools import set_adapter
                set_adapter(self)
            except Exception as exc:
                logger.warning("[%s] Failed to inject yuanbao_tools adapter: %s", self.name, exc)

            return True

        except asyncio.TimeoutError:
            logger.error("[%s] Connection timed out", self.name)
            await self._cleanup_ws()
            return False
        except Exception as exc:
            logger.error("[%s] connect() failed: %s", self.name, exc, exc_info=True)
            await self._cleanup_ws()
            return False

    async def disconnect(self) -> None:
        """Cancel background tasks and close the WebSocket connection."""
        self._running = False
        self._mark_disconnected()

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

        await self._cleanup_ws()
        logger.info("[%s] Disconnected", self.name)

    def _get_chat_lock(self, chat_id: str) -> asyncio.Lock:
        """获取或创建指定 chat_id 的锁（懒初始化，上限 _CHAT_DICT_MAX_SIZE）。"""
        if chat_id not in self._chat_locks:
            if len(self._chat_locks) >= _CHAT_DICT_MAX_SIZE:
                # Evict the oldest entry (insertion-order guaranteed in Python 3.7+)
                self._chat_locks.pop(next(iter(self._chat_locks)))
            self._chat_locks[chat_id] = asyncio.Lock()
        return self._chat_locks[chat_id]

    def _should_attach_reply_ref(self, inbound_msg_id: str) -> bool:
        """
        判断是否应该为本次出站消息附加 refMsgId。

        规则：
        - replyToMode='off' → 永不附加
        - replyToMode='all' → 始终附加
        - replyToMode='first'（默认）→ 同一入站消息只有首次回复附加

        带 TTL 清理，防止内存泄漏。
        """
        if self._reply_to_mode == "off":
            return False
        if self._reply_to_mode == "all":
            return True
        if not inbound_msg_id:
            return False

        now = time.time()

        # 清理过期条目
        if len(self._reply_ref_used) > _REPLY_REF_MAX_ENTRIES:
            expired = [k for k, ts in self._reply_ref_used.items() if now - ts > REPLY_REF_TTL_S]
            for k in expired:
                self._reply_ref_used.pop(k, None)

        # 检查是否已使用
        if inbound_msg_id in self._reply_ref_used:
            return False

        self._reply_ref_used[inbound_msg_id] = now
        return True

    @staticmethod
    def _is_self_reference(from_account: str, bot_id: Optional[str]) -> bool:
        """检测是否是 bot 自己的消息（自引用）。"""
        if not from_account or not bot_id:
            return False
        return from_account == bot_id

    @staticmethod
    def _is_skippable_placeholder(text: str) -> bool:
        """检测是否为纯占位符消息（无实际内容时应跳过）。"""
        stripped = text.strip()
        return stripped in _SKIPPABLE_PLACEHOLDERS

    @staticmethod
    def truncate_message(
        content: str,
        max_length: int = 5000,
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
        current_len = 0

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

            if not current_parts and atom_len > max_length:
                if _is_table_atom(atom):
                    chunks.append(atom)
                    continue
                chunks.extend(_base_split_no_indicator(atom))
                continue

            current_parts.append(atom)
            current_len += atom_len

        _flush()

        return chunks if chunks else [content]

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[dict] = None,
        **kwargs: Any,
    ) -> SendResult:
        """
        发送文本消息，支持自动分片、per-chat-id 顺序保障。

        chat_id 格式：
          - C2C:  "direct:{account_id}" 或直接 "{account_id}"（无前缀）
          - 群聊: "group:{group_code}"

        增强逻辑：
          1. 获取或创建该 chat_id 的锁，保证同 chat_id 消息串行发送
          2. 调用 truncate_message() 分片（保留代码块围栏完整性）
          3. 逐片调用 _send_text_chunk() 发送（含重试），任一片失败立即停止
        """
        if self._ws is None:
            return SendResult(success=False, error="Not connected", retryable=True)

        lock = self._get_chat_lock(chat_id)
        async with lock:
            chunks = self.truncate_message(content, self.MAX_TEXT_CHUNK)
            for i, chunk in enumerate(chunks):
                r_to = reply_to if i == 0 else None
                result = await self._send_text_chunk(chat_id, chunk, r_to)
                if not result.success:
                    return result

        # 消息投递成功后发送 FINISH 心跳，保证时序：RUNNING → 消息到达 → FINISH
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
        发送单个文本片段，带重试（指数退避：1s, 2s, 4s）。

        - 最多重试 retry 次
        - 每次失败后等待 2^attempt 秒（0→1s, 1→2s, 2→4s）
        - 返回最终结果
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
        """发送 C2C 文本消息，返回 {success: bool, msg_key: str}。"""
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
        """发送群聊文本消息，自动将 @nickname 转换为 TIMCustomElem。"""
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
        发送业务层请求并等待响应。

        1. 注册 Future 到 self._pending_acks[req_id]
        2. 发送 encoded_conn_msg（bytes）到 WS
        3. asyncio.wait_for(future, timeout)
        4. 超时/异常时清理 pending_acks
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
        route_env = token_data.get("route_env", "") or ""

        msg_id = str(uuid.uuid4())

        # Build and send AUTH_BIND
        auth_bytes = encode_auth_bind(
            biz_id="ybBot",
            uid=uid,
            source=source,
            token=token,
            msg_id=msg_id,
            app_version="hermes-agent/0.8.0",
            operation_system="linux",
            bot_version="hermes-agent",
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
                            if self._running:
                                asyncio.create_task(self._reconnect_with_backoff())
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
        接收循环：
        1. 读取 WS 帧（raw bytes）
        2. 调用 decode_conn_msg() 解码
        3. 根据 cmd_type 分发：
           - Response + "ping"      : HEARTBEAT_ACK，只记录 log
           - Response (其他)       : RPC 响应，resolve pending_acks
           - Push (cmd_type=2)     : 服务器推送，判断是入站消息还是 RPC 响应
           - 其他                  : 忽略或 log
        4. 每条 need_ack=True 的 Push 都回 PushAck
        5. WS 断开时（ConnectionClosed）觧发 _reconnect_with_backoff()
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
            elif self._running:
                asyncio.create_task(self._reconnect_with_backoff())
        except Exception as exc:
            logger.warning("[%s] receive_loop exited: %s", self.name, exc)
            if self._running:
                logger.info("[%s] Attempting reconnect after receive_loop error", self.name)
                asyncio.create_task(self._reconnect_with_backoff())

    async def _handle_frame(self, raw: bytes) -> None:
        """处理单个 WebSocket 帧。"""
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
                    "[%s] WS 收到入站消息推送, 开始解码分发: cmd=%s, data长度=%d",
                    self.name, cmd, len(data),
                )
                self._push_to_inbound(head, data)
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
        检测消息是否 @Bot。

        AT 元素格式：TIMCustomElem，msg_content.data 是 JSON 字符串：
            {"elem_type": 1002, "text": "@xxx", "user_id": "<botId>"}
        elem_type == 1002 且 user_id == self._bot_id 时视为 @Bot。
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

    def _build_group_context(self, chat_id: str, current_sender: str, current_msg: str) -> str:
        """
        将该 chat_id 的群聊历史 + 当前消息拼成完整 context（Markdown 格式）。

        格式：
            **[历史消息]**
            用户A: 消息1
            用户B: 消息2
            ...
            **[当前消息]**
            用户C: 当前消息内容
        """
        history = self._group_history.get(chat_id)
        lines: list[str] = []
        if history:
            lines.append("**[历史消息]**")
            for entry in history:
                lines.append(f"{entry['sender_id']}: {entry['text']}")
            lines.append("")
        lines.append("**[当前消息]**")
        lines.append(f"{current_sender}: {current_msg}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Reply Heartbeat 状态机
    # ------------------------------------------------------------------

    async def send_typing(self, chat_id: str, metadata: Optional[dict] = None) -> None:
        """
        发送"正在输入"状态心跳（RUNNING），best effort，不抛错。
        由 base 类的 _keep_typing 循环周期调用，委托给 Reply Heartbeat 状态机。
        """
        try:
            await self._start_reply_heartbeat(chat_id)
        except Exception:
            pass

    async def stop_typing(self, chat_id: str) -> None:
        """
        停止 RUNNING 心跳循环，但不立即发送 FINISH。

        FINISH 由 send() 在消息实际投递后发送，以确保时序正确：
        RUNNING... → 消息到达 → FINISH。

        gateway/run.py 的 _message_handler 在拿到 agent 结果后、返回 response
        文本之前就会调用 stop_typing()，此时消息尚未通过 WS 发出，所以这里
        只停止 RUNNING 循环，FINISH 延迟到 send() 完成后发送。
        """
        try:
            await self._stop_reply_heartbeat(chat_id, send_finish=False)
        except Exception:
            pass

    async def _start_reply_heartbeat(self, chat_id: str) -> None:
        """
        启动或续期 Reply Heartbeat 定时续发（RUNNING，每 2s 一次）。

        如果该 chat_id 已有活跃的 heartbeat task，则仅刷新时间戳（不重复启动）。
        30 秒无续期调用时自动停止（发送 FINISH）。
        """
        if self._ws is None or not self._bot_id:
            return

        # 如果已有任务在跑，只刷新时间戳即可
        existing = self._reply_heartbeat_tasks.get(chat_id)
        if existing and not existing.done():
            if not hasattr(self, '_reply_hb_last_active'):
                self._reply_hb_last_active: Dict[str, float] = {}
            self._reply_hb_last_active[chat_id] = time.time()
            return

        # 启动新的 heartbeat task
        if not hasattr(self, '_reply_hb_last_active'):
            self._reply_hb_last_active = {}
        self._reply_hb_last_active[chat_id] = time.time()

        task = asyncio.create_task(
            self._reply_heartbeat_worker(chat_id),
            name=f"yuanbao-reply-hb-{chat_id}",
        )
        self._reply_heartbeat_tasks[chat_id] = task

    async def _reply_heartbeat_worker(self, chat_id: str) -> None:
        """
        后台协程：每 2 秒发送一次 RUNNING 心跳。
        30 秒无续期 → 发送 FINISH 并退出。
        """
        try:
            # 立即发一次 RUNNING
            await self._send_heartbeat_once(chat_id, WS_HEARTBEAT_RUNNING)

            while True:
                await asyncio.sleep(REPLY_HEARTBEAT_INTERVAL_S)

                # 检查超时
                last_active = getattr(self, '_reply_hb_last_active', {}).get(chat_id, 0)
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
            if hasattr(self, '_reply_hb_last_active'):
                self._reply_hb_last_active.pop(chat_id, None)

    async def _stop_reply_heartbeat(self, chat_id: str, send_finish: bool = True) -> None:
        """
        停止 Reply Heartbeat 并可选发送 FINISH。

        Args:
            send_finish: True 时取消 worker 后立即发 FINISH；
                         False 时仅取消 worker，FINISH 由调用方负责。
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
        """发送单次心跳（RUNNING 或 FINISH），best effort。"""
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
    # 群查询方法
    # ------------------------------------------------------------------

    async def query_group_info(self, group_code: str) -> Optional[dict]:
        """
        查询群信息（群名、群主、成员数等）。

        Args:
            group_code: 群号

        Returns:
            {
              "group_code": str,
              "group_name": str,
              "owner_id": str,
              "member_count": int,
              "max_member": int,
            }
            或 None（查询失败）
        """
        if self._ws is None:
            return None
        encoded = encode_query_group_info(group_code)
        from gateway.platforms.yuanbao_proto import decode_conn_msg as _decode
        decoded = _decode(encoded)
        req_id = decoded["head"]["msg_id"]
        try:
            response = await self._send_biz_request(encoded, req_id=req_id)
            # response 来自 _handle_frame，对于 RPC Response 返回的是 {"head": head}
            # 但对于 Push 响应，返回的是 decoded push
            # 对于群查询这种 RPC，我们需要从 conn_data 中解码
            # 实际上 _send_biz_request 返回的 response 对于 Response cmd_type 是 {"head": head}
            # 群查询的响应 data 需要从原始帧中获取
            # 由于当前 _handle_frame 对 Response 只返回 head，群查询需要改进
            # 暂时返回 response head 表示成功
            head = response.get("head", {})
            status = head.get("status", 0)
            if status != 0:
                logger.warning("[%s] query_group_info failed: status=%d", self.name, status)
                return None
            # 如果 response 中有 body/data（来自 biz 层），解码它
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
        查询群成员列表。

        Args:
            group_code: 群号
            offset: 分页偏移
            limit: 分页大小

        Returns:
            {
              "members": [{"user_id": str, "nickname": str, "role": int, ...}, ...],
              "next_offset": int,
              "is_complete": bool,
            }
            或 None（查询失败）
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

    def _push_to_inbound(self, head: dict, conn_data: bytes) -> None:
        """
        将推送帧转换为 MessageEvent 并分发给 AI 处理。

        解码优先级（对齐 TS gateway.ts 的 wsPushToInboundMessage）:
          1. connData protobuf: decode_inbound_push(conn_data) 直接解码
          2. PushMsg 外层解包: decode_push_msg(conn_data) 拿 rawData,
             再 decode_inbound_push(rawData)
          3. rawData JSON 回退: raw_data 当 JSON 解析
          4. DirectedPush.content: decode_directed_push(conn_data) 拿 content JSON
          5. conn_data JSON 兜底（含 callback_command 过滤，最后才尝试）
        """
        push = None
        raw_data = None  # PushMsg.data

        # Step 1: connData protobuf 解码（优先，完整 ConnMsg.data）
        # P0: 仅当 push 非 None 且非空 dict 才停止尝试
        if conn_data:
            try:
                push = decode_inbound_push(conn_data)
                if not push:  # None 或空 dict {} 都视为失败，继续后续步骤
                    push = None
            except Exception:
                push = None

        # Step 2: PushMsg 外层解包 + rawData protobuf
        if not push:
            push_msg = decode_push_msg(conn_data)
            if push_msg is not None:
                raw_data = push_msg.get("data") or b""
                logger.debug(
                    "[%s] PushMsg decoded: cmd=%r module=%r raw_data_len=%d",
                    self.name, push_msg.get("cmd"), push_msg.get("module"), len(raw_data),
                )
                if raw_data:
                    try:
                        push = decode_inbound_push(raw_data)
                        if not push:
                            push = None
                    except Exception:
                        push = None

        # Step 3: rawData JSON 回退
        if not push and raw_data:
            try:
                raw_json = json.loads(raw_data.decode("utf-8"))
                logger.info("[%s] rawData JSON push: fields=%s len=%d",
                    self.name, list(raw_json.keys()), len(json.dumps(raw_json)))
                push = _parse_json_push(raw_json)
            except Exception:
                pass

        # Step 4: DirectedPush.content（content 是 JSON 字符串）
        if not push:
            directed = decode_directed_push(conn_data)
            if directed and directed.get("content"):
                content_str = directed["content"]
                logger.info("[%s] DirectedPush: type=%d content_len=%d",
                    self.name, directed.get("type", 0), len(content_str))
                try:
                    content_json = json.loads(content_str)
                    push = _parse_json_push(content_json)
                    if push and not push.get("msg_body"):
                        msg_body = _parse_push_content_to_msg_body(content_str)
                        if msg_body:
                            push["msg_body"] = msg_body
                except Exception as _content_exc:
                    # P2: 记录 JSON 解析失败，方便诊断
                    logger.warning(
                        "[%s] DirectedPush content JSON parse failed: %s",
                        self.name, _content_exc,
                    )
                    if content_str.strip():
                        push = {
                            "from_account": "",
                            "msg_body": [{"msg_type": "TIMTextElem",
                                          "msg_content": {"text": content_str}}],
                        }
                        logger.debug(
                            "[%s] DirectedPush fallback to plain text: %s",
                            self.name, content_str[:200],
                        )

        # Step 5: conn_data itself is JSON (callback_command style push) — 最后兜底
        # Yuanbao delivers inbound user messages directly as JSON in ConnMsg.data, e.g.:
        #   {"callback_command":"C2C.CallbackAfterSendMsg", "from_account":..., "msg_body":[...]}
        # NOTE: Despite the name "AfterSendMsg", this is the format for INBOUND user messages
        # (not a bot send-echo). The TS original code (gateway.ts decodeFromContent) assigns
        # C2C.CallbackAfterSendMsg as the callback_command for user messages received via
        # DirectedPush.content — this is exactly what Yuanbao server sends as the push payload.
        # Do NOT filter these out.
        if not push and conn_data:
            try:
                conn_json = json.loads(conn_data.decode("utf-8"))
                if not isinstance(conn_json, dict):
                    pass
                else:
                    logger.info("[%s] conn_data JSON push: fields=%s len=%d",
                        self.name, list(conn_json.keys()), len(json.dumps(conn_json)))
                    push = _parse_json_push(conn_json)
            except Exception:
                pass

        if not push:
            logger.info("[%s] Push decoded but no valid message. conn_data hex(first64)=%s",
                self.name, conn_data.hex()[:128] if conn_data else "(empty)")
            return

        logger.info("[%s] Push decoded: from=%s group=%s msg_body_len=%d", self.name, push.get("from_account",""), push.get("group_code",""), len(push.get("msg_body",[])))
        from_account: str = push.get("from_account", "")
        group_code: str = push.get("group_code", "")
        group_name: str = push.get("group_name", "")
        sender_nickname: str = push.get("sender_nickname", "")
        msg_body: list = push.get("msg_body", [])
        msg_id_field: str = push.get("msg_id", "")

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

        # ---- Group chat: history recording + @Bot filter ----
        if chat_type == "group":
            text_for_history = self._extract_text(msg_body)
            # Record every group message into history
            if chat_id not in self._group_history:
                if len(self._group_history) >= _CHAT_DICT_MAX_SIZE:
                    self._group_history.pop(next(iter(self._group_history)))
                self._group_history[chat_id] = deque(maxlen=50)
            self._group_history[chat_id].append({
                "sender_id": from_account,
                "text": text_for_history,
                "ts": int(time.time() * 1000),
            })

            # Only trigger AI when @Bot
            if not self._is_at_bot(msg_body):
                logger.info(
                    "[%s] Group message recorded (no @bot): chat=%s from=%s",
                    self.name, chat_id, from_account,
                )
                return

            # Build context-enriched content
            text = self._build_group_context(chat_id, from_account, text_for_history)
        else:
            text = self._extract_text(msg_body)

        # ---- Placeholder filter ----
        if self._is_skippable_placeholder(text):
            logger.debug("[%s] Skipping placeholder message: %r", self.name, text)
            return

        source = SessionSource(
            platform=self.PLATFORM,
            chat_id=chat_id,
            chat_type=chat_type,
            chat_name=chat_name,
            user_id=from_account or None,
            user_name=sender_nickname or None,
        )

        # ---- Command handling ----
        # Check synchronously if text looks like a command, then dispatch async
        rewritten = self._rewrite_slash_command(text)
        if rewritten.startswith('/'):
            asyncio.create_task(
                self._dispatch_command(rewritten, chat_id, source, msg_id_field)
            )
            return

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=msg_id_field or None,
            raw_message=push,
        )

        # Dispatch to base class handler (spawns background task internally).
        # The base class _keep_typing loop will call our send_typing() to
        # start/refresh the reply heartbeat, and stop_typing() will be called
        # when processing finishes to send FINISH.
        asyncio.create_task(self.handle_message(event))

    def _extract_text(self, msg_body: list) -> str:
        """
        从 MsgBody 提取纯文本内容。
        - TIMTextElem      → 提取 text 字段
        - TIMImageElem     → "[图片]"
        - TIMFileElem      → "[文件: {filename}]"
        - TIMSoundElem     → "[语音]"
        - TIMVideoFileElem → "[视频]"
        - TIMCustomElem    → 尝试提取 data 字段，否则 "[自定义消息]"
        - 多个 elem 用空格拼接
        """
        parts: list[str] = []
        for elem in msg_body:
            elem_type: str = elem.get("msg_type", "")
            content: dict = elem.get("msg_content", {})

            if elem_type == "TIMTextElem":
                text = content.get("text", "")
                if text:
                    parts.append(text)
            elif elem_type == "TIMImageElem":
                parts.append("[图片]")
            elif elem_type == "TIMFileElem":
                filename = content.get("fileName", content.get("filename", ""))
                parts.append(f"[文件: {filename}]" if filename else "[文件]")
            elif elem_type == "TIMSoundElem":
                parts.append("[语音]")
            elif elem_type == "TIMVideoFileElem":
                parts.append("[视频]")
            elif elem_type == "TIMCustomElem":
                data_val = content.get("data", "")
                parts.append(data_val if data_val else "[自定义消息]")
            elif elem_type:
                # Unknown element type — include type as placeholder
                parts.append(f"[{elem_type}]")

        return " ".join(parts) if parts else ""

    # ------------------------------------------------------------------
    # 命令框架
    # ------------------------------------------------------------------

    # 群聊中允许执行的公开命令（不需要 Owner 身份）
    GROUP_PUBLIC_COMMANDS = frozenset({"/status", "/help", "/ping"})

    async def _dispatch_command(
        self, text: str, chat_id: str, source: Any, reply_to_msg_id: str
    ) -> None:
        """Dispatch command and send reply. Called as a task from _push_to_inbound."""
        try:
            reply = await self._handle_command(text, chat_id, source)
            if reply is not None:
                await self.send(chat_id, reply, reply_to=reply_to_msg_id or None)
        except Exception as exc:
            logger.error("[%s] _dispatch_command failed: %s", self.name, exc)

    async def _handle_command(self, text: str, chat_id: str, source: Any) -> Optional[str]:
        """
        命令路由入口。如果 text 是斜杠命令则处理并返回回复文本，否则返回 None。

        Args:
            text: 用户输入文本
            chat_id: 聊天 ID
            source: SessionSource

        Returns:
            命令回复文本（str），或 None（非命令）
        """
        text = self._rewrite_slash_command(text)
        if not text.startswith('/'):
            return None

        parts = text.strip().split(None, 1)
        cmd = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ""

        # 群聊中非公开命令 → 引导私聊
        chat_type = "group" if chat_id.startswith("group:") else "dm"
        if chat_type == "group" and cmd not in self.GROUP_PUBLIC_COMMANDS:
            return f"该命令请私聊我执行: {cmd}"

        if cmd == "/status":
            return self._cmd_status()
        elif cmd == "/help":
            return self._cmd_help()
        elif cmd == "/ping":
            return "pong"
        elif cmd == "/upgrade":
            if not self._resolve_command_auth(source):
                return "仅 Bot Owner 可以执行 /upgrade 命令"
            return await self._cmd_upgrade(args)
        elif cmd == "/issue-log":
            if not self._resolve_command_auth(source):
                return "仅 Bot Owner 可以执行 /issue-log 命令"
            return await self._cmd_issue_log(args)
        else:
            return None  # 未知命令，交给 AI 处理

    def _resolve_command_auth(self, source: Any) -> bool:
        """
        检查是否为 Bot Owner（命令鉴权）。

        支持配置 bot_owner_id 或 allow_from 列表。
        """
        user_id = getattr(source, 'user_id', None) or ""
        if not user_id:
            return False

        # Check bot_owner_id from config
        owner_id = getattr(self._config, 'yuanbao_bot_owner_id', None) or os.getenv("YUANBAO_BOT_OWNER_ID", "")
        if owner_id and user_id == owner_id.strip():
            return True

        # Check allow_from list
        allow_from = getattr(self._config, 'allow_from', None) or []
        if user_id in allow_from:
            return True

        return False

    @staticmethod
    def _rewrite_slash_command(text: str) -> str:
        """
        斜杠命令预处理：标准化命令格式。

        - 去除前后空白
        - 中文全角斜杠 → 半角
        """
        text = text.strip()
        if text.startswith('\uff0f'):  # 全角斜杠
            text = '/' + text[1:]
        return text

    def _cmd_status(self) -> str:
        """执行 /status 命令，返回 bot 状态信息。"""
        try:
            from hermes_cli import __version__ as hermes_version
        except ImportError:
            hermes_version = "unknown"

        status = self.get_status()
        connected = "已连接" if status.get("connected") else "未连接"
        bot_id = status.get("bot_id", "N/A")
        connect_id = status.get("connect_id", "N/A")
        reconnects = status.get("reconnect_attempts", 0)

        return (
            f"hermes-agent({hermes_version})\n"
            f"状态: {connected}\n"
            f"Bot ID: {bot_id}\n"
            f"Connect ID: {connect_id}\n"
            f"重连次数: {reconnects}"
        )

    @staticmethod
    def _cmd_help() -> str:
        """执行 /help 命令。"""
        return (
            "可用命令:\n"
            "/status - 查看 Bot 状态\n"
            "/help - 显示帮助\n"
            "/ping - 连通性测试\n"
            "/upgrade [version] - 升级 Bot（仅 Owner）\n"
            "/issue-log - 导出诊断日志（仅 Owner）"
        )

    async def _cmd_upgrade(self, args: str) -> str:
        """
        执行 /upgrade 命令（Owner 限制）。
        触发 hermes-agent 自身升级流程。
        """
        version = args.strip() if args else "latest"
        logger.info("[%s] /upgrade requested: version=%s", self.name, version)
        # TODO: Implement actual upgrade trigger via hermes CLI
        return f"升级请求已收到，目标版本: {version}\n请通过服务器终端执行: hermes update {version}"

    async def _cmd_issue_log(self, args: str) -> str:
        """
        执行 /issue-log 命令（Owner 限制）。

        收集最近日志信息，方便诊断问题。
        """
        import io
        lines = []
        lines.append("=== Issue Log ===")
        lines.append(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Connection status
        status = self.get_status()
        lines.append(f"Connected: {status.get('connected')}")
        lines.append(f"Bot ID: {status.get('bot_id', 'N/A')}")
        lines.append(f"Connect ID: {status.get('connect_id', 'N/A')}")
        lines.append(f"Reconnect Attempts: {status.get('reconnect_attempts', 0)}")

        # Heartbeat state
        lines.append(f"Active Reply Heartbeats: {len(self._reply_heartbeat_tasks)}")
        lines.append(f"Consecutive HB Timeouts: {self._consecutive_hb_timeouts}")

        # Queue state
        lines.append(f"Outbound Queues: {len(self._outbound_queues)}")
        lines.append(f"Pending ACKs: {len(self._pending_acks)}")

        # Group history
        lines.append(f"Group History Chats: {len(self._group_history)}")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # DM 主动私聊 + 访问控制
    # ------------------------------------------------------------------

    DM_MAX_CHARS = 10000  # DM 文本上限

    async def send_dm(self, user_id: str, text: str) -> SendResult:
        """
        主动发起 C2C 私聊消息。

        Args:
            user_id: 目标用户 ID
            text: 消息文本（上限 10000 字符）

        Returns:
            SendResult
        """
        if not self._resolve_dm_access(user_id):
            return SendResult(success=False, error="DM access denied for this user")
        if len(text) > self.DM_MAX_CHARS:
            text = text[:self.DM_MAX_CHARS] + "\n...(已截断)"
        chat_id = f"direct:{user_id}"
        return await self.send(chat_id, text)

    def _resolve_dm_access(self, user_id: str) -> bool:
        """
        DM 访问控制。

        策略（从配置/环境变量读取）：
        - 'open': 允许所有用户
        - 'allowlist': 仅白名单用户
        - 'disabled': 禁止所有 DM

        配置项：
        - yuanbao_dm_policy / YUANBAO_DM_POLICY（默认 'open'）
        - yuanbao_dm_allow_from / YUANBAO_DM_ALLOW_FROM（逗号分隔的用户 ID 列表）
        """
        policy = (
            getattr(self._config, 'yuanbao_dm_policy', None)
            or os.getenv("YUANBAO_DM_POLICY", "open")
        ).strip().lower()

        if policy == "disabled":
            return False
        if policy == "open":
            return True
        if policy == "allowlist":
            allow_from_str = (
                getattr(self._config, 'yuanbao_dm_allow_from', None)
                or os.getenv("YUANBAO_DM_ALLOW_FROM", "")
            )
            if isinstance(allow_from_str, list):
                allow_list = allow_from_str
            else:
                allow_list = [x.strip() for x in str(allow_from_str).split(",") if x.strip()]
            return user_id in allow_list
        return True  # 未知策略默认放行

    # ------------------------------------------------------------------
    # Agent 工具方法（可注册为 AI toolset）
    # ------------------------------------------------------------------

    async def tool_query_group_info(self, chat_id: str) -> dict:
        """
        AI 工具: 查询当前群信息。

        无需参数（从 session context 提取 group_code）。
        返回群名、群主、成员数等。
        """
        if not chat_id.startswith("group:"):
            return {"error": "此命令仅在群聊中可用"}
        group_code = chat_id[len("group:"):]
        result = await self.query_group_info(group_code)
        if result is None:
            return {"error": "查询群信息失败"}
        return result

    async def tool_query_session_members(
        self,
        chat_id: str,
        action: str = "list_all",
        name: Optional[str] = None,
    ) -> dict:
        """
        AI 工具: 查询群成员列表。

        Args:
            chat_id: 聊天 ID（从 session context 提取）
            action: 'find'（按名称查找）| 'list_bots'（列出机器人）| 'list_all'（列出全部）
            name: 当 action='find' 时的搜索关键词

        Returns:
            {"members": [...], "total": int, "mentionHint": str}
        """
        if not chat_id.startswith("group:"):
            return {"error": "此命令仅在群聊中可用"}
        group_code = chat_id[len("group:"):]
        result = await self.get_group_member_list(group_code)
        if result is None:
            return {"error": "查询群成员失败"}

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
            # 机器人通常 role=0 且 user_id 看起来像 bot ID
            # 这里简单过滤，实际场景可能需要更精确的判断
            members = [m for m in members if "bot" in (m.get("nickname", "") or "").lower()]

        # 构造 mentionHint
        mention_hint = ""
        if members and len(members) <= 10:
            names = [m.get("name_card") or m.get("nickname") or m.get("user_id", "") for m in members]
            mention_hint = "可用 @名称 提及: " + ", ".join(names)

        return {
            "members": members[:50],  # 限制返回数量
            "total": len(members),
            "mentionHint": mention_hint,
        }

    # ------------------------------------------------------------------
    # Trace Context + Markdown Hint
    # ------------------------------------------------------------------

    @staticmethod
    def _generate_trace_id() -> str:
        """生成分布式追踪 ID（32 位 hex）。"""
        return uuid.uuid4().hex

    @staticmethod
    def _build_traceparent(trace_id: str, span_id: Optional[str] = None) -> str:
        """
        构造 W3C traceparent 头（简化版）。
        格式：00-{trace_id}-{span_id}-01
        """
        if not span_id:
            span_id = uuid.uuid4().hex[:16]
        return f"00-{trace_id}-{span_id}-01"

    @staticmethod
    def _markdown_hint_system_prompt() -> str:
        """
        Markdown 渲染提示（追加到 system prompt）。

        告诉 AI 元宝平台支持 Markdown 渲染，可以使用：
        - 代码块（```lang）
        - 表格（| col | col |）
        - 加粗/斜体
        """
        return (
            "当前平台支持 Markdown 渲染。你可以使用以下格式：\n"
            "- 代码块：```语言名\\n代码\\n```\n"
            "- 表格：| 列1 | 列2 |\\n|---|---|\\n| 值1 | 值2 |\n"
            "- 加粗：**文本** / 斜体：*文本*\n"
            "请在适当时候使用 Markdown 格式化输出，提升可读性。"
        )

    @staticmethod
    def _msg_body_desensitize(msg_body: list) -> list:
        """
        消息体脱敏（日志用）。

        对 TIMTextElem 截断长文本，对 TIMImageElem 替换 URL。
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

    @staticmethod
    def _validate_media_before_queue(
        file_bytes: Optional[bytes], filename: str, max_size_mb: int = 50
    ) -> Optional[str]:
        """
        媒体前置校验：在排入出站队列前检查文件有效性。

        Returns:
            错误描述（str）如果校验失败，否则 None。
        """
        if file_bytes is None or len(file_bytes) == 0:
            return f"空文件: {filename}"
        max_bytes = max_size_mb * 1024 * 1024
        if len(file_bytes) > max_bytes:
            size_mb = len(file_bytes) / 1024 / 1024
            return f"文件过大: {filename} ({size_mb:.1f}MB > {max_size_mb}MB)"
        return None

    # ------------------------------------------------------------------
    # Outbound queue helpers
    # ------------------------------------------------------------------

    def _get_outbound_queue(
        self, chat_id: str, reply_to: Optional[str] = None
    ) -> _OutboundQueue:
        """获取或创建指定 chat_id 的出站队列。"""
        if chat_id not in self._outbound_queues:
            if len(self._outbound_queues) >= _CHAT_DICT_MAX_SIZE:
                self._outbound_queues.pop(next(iter(self._outbound_queues)))
            self._outbound_queues[chat_id] = _OutboundQueue(self, chat_id, reply_to)
        return self._outbound_queues[chat_id]

    async def _drain_text_before_media(self, chat_id: str) -> None:
        """媒体发送前先推出文本缓冲（保证文本在媒体前发出）。"""
        queue = self._outbound_queues.get(chat_id)
        if queue and not queue.is_empty:
            await queue.drain_now()

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
        发送图片消息。

        流程：
          1. 下载图片 URL 内容（httpx）
          2. 调用 genUploadInfo 获取 COS 临时凭证
          3. PUT 上传到 COS
          4. 构造 TIMImageElem 消息体，通过 WS 发送

        若 image_url 已是 COS 公网 URL（cos.*myqcloud.com 或 file.myqcloud.com），
        则跳过重复上传，直接用其构造消息体。
        """
        if self._ws is None:
            return SendResult(success=False, error="Not connected", retryable=True)

        try:
            # 0. Drain text buffer before sending media
            await self._drain_text_before_media(chat_id)

            # 1. 下载图片
            logger.info("[%s] send_image: downloading %s", self.name, image_url)
            file_bytes, content_type = await media_download_url(image_url, max_size_mb=50)

            if not content_type or content_type == "application/octet-stream":
                # 从 URL 推断 MIME
                path_part = image_url.split("?")[0]
                content_type = guess_mime_type(path_part) or "image/jpeg"

            filename = os.path.basename(image_url.split("?")[0]) or "image.jpg"
            file_uuid = md5_hex(file_bytes)

            # 2. 获取 COS 上传凭证
            token_data = await self._get_cached_token()
            token: str = token_data.get("token", "")
            bot_id: str = token_data.get("bot_id", "") or self._bot_id or ""

            credentials = await get_cos_credentials(
                app_key=self._app_key,
                sign_token_url=self._sign_token_url,
                token=token,
                filename=filename,
                bot_id=bot_id,
            )

            # 3. 上传到 COS
            upload_result = await upload_to_cos(
                file_bytes=file_bytes,
                filename=filename,
                content_type=content_type,
                credentials=credentials,
                bucket=credentials["bucketName"],
                region=credentials["region"],
            )

            # 4. 构造 TIMImageElem 消息体
            msg_body = build_image_msg_body(
                url=upload_result["url"],
                uuid=file_uuid,
                filename=filename,
                size=upload_result["size"],
                width=upload_result.get("width", 0),
                height=upload_result.get("height", 0),
                mime_type=content_type,
            )

            # 5. 若有 caption，追加文字消息体
            if caption:
                msg_body.append(
                    {"msg_type": "TIMTextElem", "msg_content": {"text": caption}}
                )

            # 6. 发送
            async with self._send_lock:
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
        发送文件消息。

        流程：
          1. 下载文件 URL 内容（httpx）
          2. 调用 genUploadInfo 获取 COS 临时凭证
          3. PUT 上传到 COS
          4. 构造 TIMFileElem 消息体，通过 WS 发送
        """
        if self._ws is None:
            return SendResult(success=False, error="Not connected", retryable=True)

        try:
            # 0. Drain text buffer before sending media
            await self._drain_text_before_media(chat_id)

            # 1. 下载文件
            logger.info("[%s] send_file: downloading %s", self.name, file_url)
            file_bytes, content_type = await media_download_url(file_url, max_size_mb=50)

            # 推断文件名
            if not filename:
                path_part = file_url.split("?")[0]
                filename = os.path.basename(path_part) or "file"

            if not content_type or content_type == "application/octet-stream":
                content_type = guess_mime_type(filename) or "application/octet-stream"

            file_uuid = md5_hex(file_bytes)

            # 2. 获取 COS 上传凭证
            token_data = await self._get_cached_token()
            token: str = token_data.get("token", "")
            bot_id: str = token_data.get("bot_id", "") or self._bot_id or ""

            credentials = await get_cos_credentials(
                app_key=self._app_key,
                sign_token_url=self._sign_token_url,
                token=token,
                filename=filename,
                bot_id=bot_id,
            )

            # 3. 上传到 COS
            upload_result = await upload_to_cos(
                file_bytes=file_bytes,
                filename=filename,
                content_type=content_type,
                credentials=credentials,
                bucket=credentials["bucketName"],
                region=credentials["region"],
            )

            # 4. 构造 TIMFileElem 消息体
            msg_body = build_file_msg_body(
                url=upload_result["url"],
                filename=filename,
                uuid=file_uuid,
                size=upload_result["size"],
            )

            # 5. 发送
            async with self._send_lock:
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
        发送表情包/贴纸（TIMFaceElem）。

        解析优先级：
          1. sticker_name 不为空 → 在 STICKER_MAP 中模糊查找，找不到返回错误
          2. face_index 不为空   → 直接用该 index 构造 TIMFaceElem（无 data）
          3. 两者均为空          → 随机发送一个内置贴纸

        chat_id 格式与 send() 相同：
          - C2C:  "direct:{account_id}" 或 "{account_id}"
          - 群聊: "group:{group_code}"
        """
        from gateway.platforms.yuanbao_sticker import (
            get_sticker_by_name,
            get_random_sticker,
            build_face_msg_body,
            build_sticker_msg_body,
        )

        if self._ws is None:
            return SendResult(success=False, error="Not connected", retryable=True)

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

            async with self._send_lock:
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
        发送本地文件（文档）消息。

        与 send_file() 类似，但接收本地路径而非 URL。
        流程：读取本地文件 → COS 上传 → 构造 TIMFileElem → WS 发送
        """
        if self._ws is None:
            return SendResult(success=False, error="Not connected", retryable=True)

        try:
            # 0. Drain text buffer before sending media
            await self._drain_text_before_media(chat_id)

            # 1. 读取本地文件
            if not os.path.isfile(file_path):
                return SendResult(success=False, error=f"File not found: {file_path}")

            logger.info("[%s] send_document: reading local file %s", self.name, file_path)
            with open(file_path, "rb") as f:
                file_bytes = f.read()

            if not file_bytes:
                return SendResult(success=False, error=f"File is empty: {file_path}")

            # 推断文件名和 MIME 类型
            if not filename:
                filename = os.path.basename(file_path) or "document"
            content_type = guess_mime_type(filename) or "application/octet-stream"
            file_uuid = md5_hex(file_bytes)

            # 2. 获取 COS 上传凭证
            token_data = await self._get_cached_token()
            token: str = token_data.get("token", "")
            bot_id: str = token_data.get("bot_id", "") or self._bot_id or ""

            credentials = await get_cos_credentials(
                app_key=self._app_key,
                sign_token_url=self._sign_token_url,
                token=token,
                filename=filename,
                bot_id=bot_id,
            )

            # 3. 上传到 COS
            upload_result = await upload_to_cos(
                file_bytes=file_bytes,
                filename=filename,
                content_type=content_type,
                credentials=credentials,
                bucket=credentials["bucketName"],
                region=credentials["region"],
            )

            # 4. 构造 TIMFileElem 消息体
            msg_body = build_file_msg_body(
                url=upload_result["url"],
                filename=filename,
                uuid=file_uuid,
                size=upload_result["size"],
            )

            # 5. 若有 caption，追加文字消息体
            if caption:
                msg_body.append(
                    {"msg_type": "TIMTextElem", "msg_content": {"text": caption}}
                )

            # 6. 发送
            async with self._send_lock:
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
        """发送任意 MsgBody 的 C2C 消息。"""
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
        """发送任意 MsgBody 的群聊消息。"""
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
        获取当前有效的签票 token（走模块级缓存）。
        """
        return await get_sign_token(
            self._app_key, self._app_secret, self._sign_token_url
        )

    async def _reconnect_with_backoff(self) -> bool:
        """
        Reconnect with exponential backoff.

        Waits 1s, 2s, 4s, … up to 60s between attempts.
        On each attempt, force-refreshes the sign token (old token may be expired).
        Returns True on successful reconnect, False after max attempts.
        """
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
                    self._app_key, self._app_secret, self._sign_token_url
                )
                if token_data.get("bot_id"):
                    self._bot_id = str(token_data["bot_id"])

                self._ws = await asyncio.wait_for(
                    websockets.connect(  # type: ignore[attr-defined]
                        self._ws_gateway_url,
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
            "ws_gateway_url": self._ws_gateway_url,
        }

    def _next_seq(self) -> int:
        """Return a monotonically increasing local sequence number."""
        self._seq += 1
        return self._seq


# ============================================================
# 模块级辅助函数（JSON 推送解析，对齐 TS 的 decodeFromContent）
# ============================================================

def _parse_json_push(raw_json: dict) -> dict | None:
    """
    将 JSON 格式的推送（来自 rawData 或 DirectedPush.content）转换为
    与 decode_inbound_push 相同结构的 dict。

    支持标准回调格式（callback_command + from_account + msg_body）
    以及旧格式字段（GroupId, MsgSeq, MsgKey, MsgBody 等）。
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
        "trace_id": (raw_json.get("log_ext") or {}).get("trace_id", "") if isinstance(raw_json.get("log_ext"), dict) else "",
    }


def _parse_push_content_to_msg_body(content: str) -> list | None:
    """
    将 DirectedPush.content（字符串）解析为 msg_body 列表。
    对齐 TS 的 parsePushContentToMsgBody。

    - 若是 JSON 且有 msg_body 数组 → 直接返回
    - 若是 JSON 且有 text 字段 → 包装为 TIMTextElem
    - 纯文本 → 包装为 TIMTextElem
    """
    if not content or not content.strip():
        return None
    try:
        parsed = json.loads(content)
        if parsed.get("msg_body") and isinstance(parsed["msg_body"], list):
            return _convert_json_msg_body(parsed["msg_body"])
        if parsed.get("text"):
            return [{"msg_type": "TIMTextElem", "msg_content": {"text": parsed["text"]}}]
    except (json.JSONDecodeError, AttributeError):
        pass
    # 纯文本
    return [{"msg_type": "TIMTextElem", "msg_content": {"text": content}}]


def _convert_json_msg_body(raw_body: list) -> list:
    """
    将原始 JSON msg_body 数组标准化为 [{"msg_type": str, "msg_content": dict}] 格式。
    兼容大驼峰（MsgType/MsgContent）和小蛇形（msg_type/msg_content）两种命名。
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
# Markdown 分块工具函数（原 yuanbao_markdown.py）
# ============================================================

def has_unclosed_fence(text: str) -> bool:
    """
    检测文本中是否有未闭合的代码块围栏。

    逐行扫描，遇到以 ``` 开头的行时切换 in/out 状态。
    奇数次切换说明存在未闭合的围栏。

    Args:
        text: 待检测的 Markdown 文本

    Returns:
        若文本以未闭合围栏结尾则返回 True，否则返回 False
    """
    in_fence = False
    for line in text.split('\n'):
        if line.startswith('```'):
            in_fence = not in_fence
    return in_fence


def ends_with_table_row(text: str) -> bool:
    """
    检测文本是否以表格行结尾（最后一个非空行以 | 开头且以 | 结尾）。

    Args:
        text: 待检测的文本

    Returns:
        若最后一个非空行是表格行则返回 True
    """
    trimmed = text.rstrip()
    if not trimmed:
        return False
    last_line = trimmed.split('\n')[-1].strip()
    return last_line.startswith('|') and last_line.endswith('|')


def split_at_paragraph_boundary(text: str, max_chars: int) -> tuple[str, str]:
    """
    在不超过 max_chars 的前提下，找到最近的段落边界切割点，返回 (前段, 后段)。

    切割优先级：
    1. 空行（段落边界）
    2. 句号/问号/感叹号（中英文）后的换行
    3. 最后一个换行
    4. 强制在 max_chars 处切割

    Args:
        text: 待切割文本
        max_chars: 最大字符数上限

    Returns:
        (head, tail) 元组，head 是切割前段，tail 是切割后段，满足 head + tail == text
    """
    if len(text) <= max_chars:
        return text, ''

    window = text[:max_chars]

    # 1. 优先找最后一个空行（\n\n）作为段落边界
    pos = window.rfind('\n\n')
    if pos > 0:
        return text[:pos + 2], text[pos + 2:]

    # 2. 其次找最后一个句子结束符后的换行
    sentence_end_re = re.compile(r'[。！？.!?]\n')
    best_pos = -1
    for m in sentence_end_re.finditer(window):
        best_pos = m.end()
    if best_pos > 0:
        return text[:best_pos], text[best_pos:]

    # 3. 退而求其次：找最后一个换行
    pos = window.rfind('\n')
    if pos > 0:
        return text[:pos + 1], text[pos + 1:]

    # 4. 实在没有合法切割点，强制在 max_chars 处切割
    return text[:max_chars], text[max_chars:]


def _is_fence_atom(text: str) -> bool:
    """判断原子块是否是代码块（以 ``` 开头）。"""
    return text.lstrip().startswith('```')


def _is_table_atom(text: str) -> bool:
    """判断原子块是否是表格（第一行以 | 开头）。"""
    first_line = text.split('\n')[0].strip()
    return first_line.startswith('|') and first_line.endswith('|')


def _split_into_atoms(text: str) -> list[str]:
    """
    将文本拆分为"原子块"列表，每个原子块是不可分割的逻辑单元：

    - 代码块（fence）：从 ``` 开到对应 ``` 关闭的整段（含首尾 fence 行）
    - 表格：连续的 |...| 行组成的整段
    - 普通段落：以空行分隔的普通文本段

    空行作为分隔符，不加入任何原子块。

    Args:
        text: 待拆分的 Markdown 文本

    Returns:
        原子块字符串列表（均非空）
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


def chunk_markdown_text(text: str, max_chars: int = 3000) -> list[str]:
    """
    将 Markdown 文本按 max_chars 切割为多个片段。

    保证：
    - 每个片段 <= max_chars 字符（除非单个代码块/表格本身超过限制）
    - 代码块（```...```）不在中间被切断
    - 表格行不在中间被切断（表格作为原子块整体输出）
    - 在段落边界切割（空行、句号后等）

    Args:
        text: 待切割的 Markdown 文本
        max_chars: 每片段最大字符数，默认 3000

    Returns:
        切割后的文本片段列表（非空）
    """
    if not text:
        return []

    if len(text) <= max_chars:
        return [text]

    # Phase 1: 提取原子块
    atoms = _split_into_atoms(text)

    # Phase 2: 贪心合并
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

    # Phase 3: 后处理 — 对仍超长的 chunk 做段落边界切割
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
    推断两个切割片段之间应使用的分隔符。

    规则（对齐 TS markdown-stream.ts）：
    - 前段以代码块围栏结尾 或 后段以围栏开头 → 单换行 '\\n'
    - 前段以表格行结尾 且 后段以表格行开头 → 单换行 '\\n'（续表）
    - 否则 → 双换行 '\\n\\n'（段落分隔）

    Args:
        prev_chunk: 前一个片段
        next_chunk: 后一个片段

    Returns:
        '\\n' 或 '\\n\\n'
    """
    prev_trimmed = prev_chunk.rstrip()
    next_trimmed = next_chunk.lstrip()

    # 前段以围栏结尾 或 后段以围栏开头
    if prev_trimmed.endswith('```') or next_trimmed.startswith('```'):
        return '\n'

    # 表格续行
    if ends_with_table_row(prev_chunk):
        first_line = next_trimmed.split('\n')[0].strip() if next_trimmed else ''
        if first_line.startswith('|') and first_line.endswith('|'):
            return '\n'

    return '\n\n'


def _merge_block_streaming_fences(chunks: list[str]) -> list[str]:
    """
    流式片段围栏感知合并。

    当流式输出产生的多个片段在围栏中间被截断时，
    尝试合并相邻片段使围栏完整。

    规则：
    - 若第 i 个片段有未闭合围栏，且第 i+1 个片段以 ``` 开头，
      则将 i+1 合并到 i（直到围栏闭合或没有更多片段）。
    - 合并时使用 _infer_block_separator 推断分隔符。

    Args:
        chunks: 原始片段列表

    Returns:
        合并后的片段列表（长度 <= 原始长度）
    """
    if not chunks:
        return []

    result: list[str] = []
    i = 0
    while i < len(chunks):
        current = chunks[i]
        # 如果当前片段有未闭合围栏，尝试合并后续片段
        while has_unclosed_fence(current) and i + 1 < len(chunks):
            sep = _infer_block_separator(current, chunks[i + 1])
            current = current + sep + chunks[i + 1]
            i += 1
        result.append(current)
        i += 1

    return result


def _strip_outer_markdown_fence(text: str) -> str:
    """
    剥除外层 Markdown 围栏。

    当 AI 回复整段被 ```markdown\\n...\\n``` 包裹时，去掉外层围栏，
    保留内容。仅当首行是 ```markdown（大小写不敏感）且末行是 ``` 时才剥除。

    Args:
        text: 待处理文本

    Returns:
        剥除外层围栏后的文本（如果不匹配则返回原文）
    """
    if not text:
        return text

    lines = text.split('\n')
    if len(lines) < 3:
        return text

    first_line = lines[0].strip()
    last_line = lines[-1].strip()

    # 首行必须是 ```markdown（可选语言标记 md/markdown）
    if not re.match(r'^```(?:markdown|md)?\s*$', first_line, re.IGNORECASE):
        return text

    # 末行必须是纯 ```
    if last_line != '```':
        return text

    # 剥除首末行
    inner = '\n'.join(lines[1:-1])
    return inner


def _sanitize_markdown_table(text: str) -> str:
    """
    表格输出净化。

    处理 AI 生成的 Markdown 表格中常见的格式问题：
    1. 去除表格行前后的多余空格
    2. 确保分隔行（|---|---|）格式正确
    3. 去除空的表格行

    Args:
        text: 包含表格的 Markdown 文本

    Returns:
        净化后的文本
    """
    if '|' not in text:
        return text

    lines = text.split('\n')
    result_lines: list[str] = []

    for line in lines:
        stripped = line.strip()

        # 表格行处理
        if stripped.startswith('|') and stripped.endswith('|'):
            # 分隔行标准化：| --- | --- | → |---|---|
            if re.match(r'^\|[\s\-:]+(\|[\s\-:]+)+\|$', stripped):
                cells = stripped.split('|')
                normalized = '|'.join(
                    cell.strip() if cell.strip() else cell
                    for cell in cells
                )
                result_lines.append(normalized)
            elif stripped == '||' or stripped.replace('|', '').strip() == '':
                # 空表格行 → 跳过
                continue
            else:
                result_lines.append(stripped)
        else:
            result_lines.append(line)

    return '\n'.join(result_lines)


# ============================================================
# 签票 API（原 yuanbao_api.py）
# ============================================================

SIGN_TOKEN_PATH = "/api/v5/robotLogic/sign-token"

#: 可重试的业务错误码
RETRYABLE_SIGN_CODE = 10099
SIGN_MAX_RETRIES = 3
SIGN_RETRY_DELAY_S = 1.0

#: 提前刷新裕量（秒），过期前 60 秒即视为即将过期
CACHE_REFRESH_MARGIN_S = 60

#: HTTP 超时（秒）
HTTP_TIMEOUT_S = 10.0

# 模块级缓存
# key: app_key
# value: {"token": str, "bot_id": str, "expire_ts": float (unix timestamp)}
_token_cache: dict[str, dict[str, Any]] = {}

# Per-app_key refresh locks — prevents concurrent duplicate sign-token requests.
_refresh_locks: dict[str, asyncio.Lock] = {}


def _get_refresh_lock(app_key: str) -> asyncio.Lock:
    """Return (creating if needed) the per-app_key refresh lock."""
    if app_key not in _refresh_locks:
        _refresh_locks[app_key] = asyncio.Lock()
    return _refresh_locks[app_key]


def _compute_signature(nonce: str, timestamp: str, app_key: str, app_secret: str) -> str:
    """
    计算签名（与 TypeScript 原版对齐）。
    plain     = nonce + timestamp + app_key + app_secret
    signature = HMAC-SHA256(key=app_secret, msg=plain).hexdigest()
    """
    plain = nonce + timestamp + app_key + app_secret
    return hmac.new(app_secret.encode(), plain.encode(), hashlib.sha256).hexdigest()


def _build_timestamp() -> str:
    """
    构造北京时间 ISO-8601 时间戳（与 TypeScript 原版对齐）。
    格式：2006-01-02T15:04:05+08:00（无毫秒）
    """
    bjtime = datetime.now(tz=timezone(timedelta(hours=8)))
    return bjtime.strftime("%Y-%m-%dT%H:%M:%S+08:00")


def _is_cache_valid(entry: dict[str, Any]) -> bool:
    """判断缓存是否有效（未过期且留有裕量）。"""
    return entry["expire_ts"] - time.time() > CACHE_REFRESH_MARGIN_S


async def _do_fetch_sign_token(
    app_key: str,
    app_secret: str,
    sign_token_url: str,
) -> dict[str, Any]:
    """
    发起签票 HTTP 请求，支持自动重试（最多 SIGN_MAX_RETRIES 次）。
    """
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
                "X-AppVersion": "hermes-agent/0.8.0",
                "X-OperationSystem": "linux",
                "X-Instance-Id": "16",
                "X-Bot-Version": "hermes-agent",
            }

            logger.info(
                "签票请求: url=%s%s",
                sign_token_url,
                f" (重试 {attempt}/{SIGN_MAX_RETRIES})" if attempt > 0 else "",
            )

            response = await client.post(sign_token_url, json=payload, headers=headers)

            if response.status_code != 200:
                body = response.text
                raise RuntimeError(f"签票接口返回 {response.status_code}: {body[:200]}")

            try:
                result_data: dict[str, Any] = response.json()
            except Exception as exc:
                raise ValueError(f"签票接口响应解析失败: {exc}") from exc

            code = result_data.get("code")
            if code == 0:
                data = result_data.get("data")
                if not isinstance(data, dict):
                    raise ValueError(f"签票响应缺少 data 字段: {result_data}")
                logger.info("签票成功: bot_id=%s", data.get("bot_id"))
                return data

            if code == RETRYABLE_SIGN_CODE and attempt < SIGN_MAX_RETRIES:
                logger.warning(
                    "签票可重试: code=%s，%ss 后重试 (attempt=%d/%d)",
                    code,
                    SIGN_RETRY_DELAY_S,
                    attempt + 1,
                    SIGN_MAX_RETRIES,
                )
                await asyncio.sleep(SIGN_RETRY_DELAY_S)
                continue

            msg = result_data.get("msg", "")
            raise RuntimeError(f"签票错误: code={code}, msg={msg}")

    raise RuntimeError("签票失败: 超过最大重试次数")


async def get_sign_token(
    app_key: str,
    app_secret: str,
    sign_token_url: str,
) -> dict[str, Any]:
    """
    获取 WS 鉴权 token（带缓存）。

    缓存命中时直接返回，不重复请求；距过期前 60 秒视为即将过期，触发刷新。
    """
    cached = _token_cache.get(app_key)
    if cached and _is_cache_valid(cached):
        remain = int(cached["expire_ts"] - time.time())
        logger.info("使用缓存 token (剩余 %ds)", remain)
        return dict(cached)

    async with _get_refresh_lock(app_key):
        cached = _token_cache.get(app_key)
        if cached and _is_cache_valid(cached):
            return dict(cached)

        data = await _do_fetch_sign_token(app_key, app_secret, sign_token_url)

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
    sign_token_url: str,
) -> dict[str, Any]:
    """
    强制刷新 token（清除缓存后重新签票）。
    """
    logger.warning("[force-refresh] 清除缓存并重新签票: app_key=****%s", app_key[-4:])
    async with _get_refresh_lock(app_key):
        _token_cache.pop(app_key, None)
        data = await _do_fetch_sign_token(app_key, app_secret, sign_token_url)

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
