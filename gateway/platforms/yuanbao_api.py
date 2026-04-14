"""
yuanbao_api.py — 元宝签票 API 异步客户端

提供获取 WS 鉴权 token 的 HTTP 异步接口，包含模块级缓存和强制刷新逻辑。

签名算法（与 TypeScript 原版对齐）：
    plain     = nonce + timestamp + appKey + appSecret
    signature = HMAC-SHA256(key=appSecret, msg=plain).hexdigest()

签票接口：POST <sign_token_url>
请求体：{ app_key, nonce, signature, timestamp }
响应体：{ code: 0, data: { token, bot_id, duration, product, source } }
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import secrets
import time
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# ============ 常量 ============

SIGN_TOKEN_PATH = "/api/v5/robotLogic/sign-token"

#: 可重试的业务错误码
RETRYABLE_SIGN_CODE = 10099
SIGN_MAX_RETRIES = 3
SIGN_RETRY_DELAY_S = 1.0

#: 提前刷新裕量（秒），过期前 60 秒即视为即将过期
CACHE_REFRESH_MARGIN_S = 60

#: HTTP 超时（秒）
HTTP_TIMEOUT_S = 10.0

# ============ 模块级缓存 ============
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


# ============ 内部工具 ============

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

    Returns:
        服务端 data 字段，包含 token、bot_id、duration 等。

    Raises:
        RuntimeError: HTTP 非 200 或业务 code != 0（不可重试）
        ValueError: 响应体解析失败
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
                result: dict[str, Any] = response.json()
            except Exception as exc:
                raise ValueError(f"签票接口响应解析失败: {exc}") from exc

            code = result.get("code")
            if code == 0:
                data = result.get("data")
                if not isinstance(data, dict):
                    raise ValueError(f"签票响应缺少 data 字段: {result}")
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

            msg = result.get("msg", "")
            raise RuntimeError(f"签票错误: code={code}, msg={msg}")

    raise RuntimeError("签票失败: 超过最大重试次数")


# ============ 公开接口 ============

async def get_sign_token(
    app_key: str,
    app_secret: str,
    sign_token_url: str,
) -> dict[str, Any]:
    """
    获取 WS 鉴权 token（带缓存）。

    缓存命中时直接返回，不重复请求；距过期前 60 秒视为即将过期，触发刷新。

    Args:
        app_key:        应用标识，也用作缓存 key。
        app_secret:     HMAC 签名密钥。
        sign_token_url: 签票接口完整 URL。

    Returns:
        包含以下字段的 dict（以服务端实际返回为准）：
            token      (str)   — WS 鉴权 token
            bot_id     (str)   — Bot 账号 ID
            duration   (int)   — token 有效期（秒）
            product    (str)
            source     (str)

    Raises:
        RuntimeError: 签票接口异常
        ValueError:   响应解析失败
    """
    cached = _token_cache.get(app_key)
    if cached and _is_cache_valid(cached):
        remain = int(cached["expire_ts"] - time.time())
        logger.info("使用缓存 token (剩余 %ds)", remain)
        return dict(cached)

    async with _get_refresh_lock(app_key):
        # Re-check under lock: another coroutine may have refreshed while we waited.
        cached = _token_cache.get(app_key)
        if cached and _is_cache_valid(cached):
            return dict(cached)

        data = await _do_fetch_sign_token(app_key, app_secret, sign_token_url)

        duration: int = data.get("duration", 0)
        expire_ts = time.time() + duration if duration > 0 else time.time() + 3600  # 默认 1h

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

    用于鉴权失败（如 401）后主动触发重新签票的场景。

    Args:
        app_key:        应用标识（同时作为缓存 key）。
        app_secret:     HMAC 签名密钥。
        sign_token_url: 签票接口完整 URL。

    Returns:
        最新的 token 信息 dict（字段同 get_sign_token）。
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
