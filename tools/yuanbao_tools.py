"""
yuanbao_tools.py - 元宝平台工具集

提供以下工具函数，供 hermes-agent 的 "hermes-yuanbao" toolset 使用：
  - get_group_info        : 查询群基本信息（群名、群主、成员数）
  - query_group_members   : 查询群成员（按名搜索、列举 bot、列举全部）
  - send_dm               : 发送私聊消息（按昵称查找用户并发送）

The active adapter singleton lives in ``gateway.platforms.yuanbao`` and is
accessed via ``get_active_adapter()``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _get_active_adapter():
    """Lazy import to avoid ImportError when gateway.platforms.yuanbao is unavailable."""
    try:
        from gateway.platforms.yuanbao import get_active_adapter
        return get_active_adapter()
    except ImportError:
        return None


if TYPE_CHECKING:
    from gateway.platforms.yuanbao import YuanbaoAdapter


# ---------------------------------------------------------------------------
# 角色标签
# ---------------------------------------------------------------------------

_USER_TYPE_LABEL = {0: "unknown", 1: "user", 2: "yuanbao_ai", 3: "bot"}

MENTION_HINT = (
    'To @mention a user, you MUST use the format: '
    'space + @ + nickname + space (e.g. " @Alice ").'
)


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

async def get_group_info(group_code: str) -> dict:
    """查询群基本信息（群名、群主、成员数）。"""
    if not group_code:
        return {"success": False, "error": "group_code is required"}

    adapter = _get_active_adapter()
    if adapter is None:
        return {"success": False, "error": "Yuanbao adapter is not connected"}

    try:
        gi = await adapter.query_group_info(group_code)
        if gi is None:
            return {"success": False, "error": "query_group_info returned None"}
        return {
            "success": True,
            "group_code": group_code,
            "group_name": gi.get("group_name", ""),
            "member_count": gi.get("member_count", 0),
            "owner": {
                "user_id": gi.get("owner_id", ""),
                "nickname": gi.get("owner_nickname", ""),
            },
            "note": 'The group is called "派 (Pai)" in the app.',
        }
    except Exception as exc:
        logger.exception("[yuanbao_tools] get_group_info error")
        return {"success": False, "error": str(exc)}


async def query_group_members(
    group_code: str,
    action: str = "list_all",
    name: str = "",
    mention: bool = False,
) -> dict:
    """
    统一的群成员查询工具（对齐 TS query_session_members）。

    action:
      - find      : 按昵称模糊搜索
      - list_bots : 列出 bot 和元宝 AI
      - list_all  : 列出全部成员
    """
    if not group_code:
        return {"success": False, "error": "group_code is required"}

    adapter = _get_active_adapter()
    if adapter is None:
        return {"success": False, "error": "Yuanbao adapter is not connected"}

    try:
        raw = await adapter.get_group_member_list(group_code)
        if raw is None:
            return {"success": False, "error": "get_group_member_list returned None"}

        all_members = [
            {
                "user_id": m.get("user_id", ""),
                "nickname": m.get("nickname", m.get("nick_name", "")),
                "role": _USER_TYPE_LABEL.get(
                    m.get("user_type", m.get("role", 0)), "unknown"
                ),
            }
            for m in raw.get("members", [])
        ]

        if not all_members:
            return {"success": False, "error": "No members found in this group."}

        hint = {"mention_hint": MENTION_HINT} if mention else {}

        if action == "list_bots":
            bots = [m for m in all_members if m["role"] in ("yuanbao_ai", "bot")]
            if not bots:
                return {"success": False, "error": "No bots found in this group."}
            return {
                "success": True,
                "msg": f"Found {len(bots)} bot(s).",
                "members": bots,
                **hint,
            }

        if action == "find":
            if name:
                filt = name.strip().lower()
                matched = [m for m in all_members if filt in m["nickname"].lower()]
                if matched:
                    return {
                        "success": True,
                        "msg": f'Found {len(matched)} member(s) matching "{name}".',
                        "members": matched,
                        **hint,
                    }
                return {
                    "success": False,
                    "msg": f'No match for "{name}". All members listed below.',
                    "members": all_members,
                    **hint,
                }
            return {
                "success": True,
                "msg": f"Found {len(all_members)} member(s).",
                "members": all_members,
                **hint,
            }

        # list_all (default)
        return {
            "success": True,
            "msg": f"Found {len(all_members)} member(s).",
            "members": all_members,
            **hint,
        }

    except Exception as exc:
        logger.exception("[yuanbao_tools] query_group_members error")
        return {"success": False, "error": str(exc)}


# Image extensions for media dispatch (mirrors MessageSender.IMAGE_EXTS)
_IMAGE_EXTS = frozenset({".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"})


async def send_dm(
    group_code: str,
    name: str,
    message: str,
    user_id: str = "",
    media_files: Optional[List[Tuple[str, bool]]] = None,
) -> dict:
    """
    Send a DM (private chat message) to a group member, with optional media.

    Workflow:
      1. If user_id is provided, send directly.
      2. Otherwise, search the group member list by name to resolve user_id.
      3. Send text via adapter.send_dm(), then iterate media_files by extension.

    Args:
        group_code: The group where the target user belongs.
        name: Target user's nickname (partial match, case-insensitive).
        message: The message text to send.
        user_id: (Optional) If already known, skip the member lookup.
        media_files: (Optional) List of (file_path, is_voice) tuples to send
                     after the text message.  Images are sent via
                     send_image_file; everything else via send_document.
    """
    if not message and not media_files:
        return {"success": False, "error": "message or media_files is required"}

    adapter = _get_active_adapter()
    if adapter is None:
        return {"success": False, "error": "Yuanbao adapter is not connected"}

    resolved_user_id = user_id.strip() if user_id else ""
    resolved_nickname = name.strip()

    # Step 1: Resolve user_id from group member list if not provided
    if not resolved_user_id:
        if not group_code:
            return {"success": False, "error": "group_code is required when user_id is not provided"}
        if not name:
            return {"success": False, "error": "name is required when user_id is not provided"}

        try:
            raw = await adapter.get_group_member_list(group_code)
            if raw is None:
                return {"success": False, "error": "get_group_member_list returned None"}

            members = raw.get("members", [])
            filt = name.strip().lower()
            matched = [
                m for m in members
                if filt in (m.get("nickname") or m.get("nick_name") or "").lower()
            ]

            if not matched:
                return {
                    "success": False,
                    "error": f'No member matching "{name}" found in group {group_code}.',
                }
            if len(matched) > 1:
                # Multiple matches — return candidates for disambiguation
                candidates = [
                    {
                        "user_id": m.get("user_id", ""),
                        "nickname": m.get("nickname", m.get("nick_name", "")),
                    }
                    for m in matched
                ]
                return {
                    "success": False,
                    "error": f'Multiple members match "{name}". Please specify which one.',
                    "candidates": candidates,
                }

            resolved_user_id = matched[0].get("user_id", "")
            resolved_nickname = matched[0].get("nickname", matched[0].get("nick_name", name))
        except Exception as exc:
            logger.exception("[yuanbao_tools] send_dm member lookup error")
            return {"success": False, "error": str(exc)}

    if not resolved_user_id:
        return {"success": False, "error": "Could not resolve user_id"}

    # Step 2: Send text DM + media
    chat_id = f"direct:{resolved_user_id}"
    last_result = None
    errors: list[str] = []
    try:
        if message and message.strip():
            last_result = await adapter.send_dm(resolved_user_id, message, group_code=group_code)
            if not last_result.success:
                errors.append(last_result.error or "text send failed")

        # Step 3: Send media files
        for media_path, _is_voice in media_files or []:
            ext = Path(media_path).suffix.lower()
            if ext in _IMAGE_EXTS:
                last_result = await adapter.send_image_file(chat_id, media_path, group_code=group_code)
            else:
                last_result = await adapter.send_document(chat_id, media_path, group_code=group_code)
            if not last_result.success:
                errors.append(last_result.error or "media send failed")

        if last_result is None:
            return {"success": False, "error": "No deliverable text or media remained"}

        if errors and (last_result is None or not last_result.success):
            return {"success": False, "error": "; ".join(errors)}

        result = {
            "success": True,
            "user_id": resolved_user_id,
            "nickname": resolved_nickname,
            "message_id": last_result.message_id,
            "note": f'DM sent to "{resolved_nickname}" successfully.',
        }
        if errors:
            result["note"] += f" (partial failure: {'; '.join(errors)})"
        return result
    except Exception as exc:
        logger.exception("[yuanbao_tools] send_dm error")
        return {"success": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# Registry registration
# ---------------------------------------------------------------------------

from tools.registry import registry, tool_result, tool_error  # noqa: E402


def _check_yuanbao():
    """Toolset availability check — True when running in a yuanbao gateway session."""
    try:
        from gateway.session_context import get_session_env
        if get_session_env("HERMES_SESSION_PLATFORM", "") == "yuanbao":
            return True
    except Exception:
        pass
    return _get_active_adapter() is not None


async def _handle_yb_query_group_info(args, **kw):
    return tool_result(await get_group_info(
        group_code=args.get("group_code", ""),
    ))


async def _handle_yb_query_group_members(args, **kw):
    return tool_result(await query_group_members(
        group_code=args.get("group_code", ""),
        action=args.get("action", "list_all"),
        name=args.get("name", ""),
        mention=bool(args.get("mention", False)),
    ))


async def _handle_yb_send_dm(args, **kw):
    # Resolve group_code: prefer explicit arg, fallback to session context.
    group_code = args.get("group_code", "")
    if not group_code:
        try:
            from gateway.session_context import get_session_env
            chat_id = get_session_env("HERMES_SESSION_CHAT_ID", "")
            # chat_id format: "group:<code>" → extract the code part
            if chat_id.startswith("group:"):
                group_code = chat_id.split(":", 1)[1]
        except Exception:
            pass

    # Parse media_files: list of {{"path": str, "is_voice": bool}} → List[Tuple[str, bool]]
    raw_media = args.get("media_files") or []
    media_files = []
    for item in raw_media:
        if isinstance(item, dict):
            media_files.append((item.get("path", ""), bool(item.get("is_voice", False))))
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            media_files.append((str(item[0]), bool(item[1])))

    # Extract MEDIA:<path> tags embedded in the message text (LLM often puts
    # file paths there instead of using the media_files parameter).
    message = args.get("message", "")
    from gateway.platforms.base import BasePlatformAdapter
    embedded_media, message = BasePlatformAdapter.extract_media(message)
    if embedded_media:
        media_files.extend(embedded_media)

    return tool_result(await send_dm(
        group_code=group_code,        name=args.get("name", ""),
        message=message,
        user_id=args.get("user_id", ""),
        media_files=media_files or None,
    ))


_TOOLSET = "hermes-yuanbao"

registry.register(
    name="yb_query_group_info",
    toolset=_TOOLSET,
    schema={
        "name": "yb_query_group_info",
        "description": (
            "Query basic info about a group (called '派/Pai' in the app), "
            "including group name, owner, and member count."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "group_code": {
                    "type": "string",
                    "description": "The unique group identifier (group_code).",
                },
            },
            "required": ["group_code"],
        },
    },
    handler=_handle_yb_query_group_info,
    check_fn=_check_yuanbao,
    is_async=True,
    emoji="👥",
)

registry.register(
    name="yb_query_group_members",
    toolset=_TOOLSET,
    schema={
        "name": "yb_query_group_members",
        "description": (
            "Query members of a group (called '派/Pai' in the app). "
            "Use this tool when you need to @mention someone, find a user by name, "
            "list bots (including Yuanbao AI), or list all members. "
            "IMPORTANT: You MUST call this tool before @mentioning any user, "
            "because you need the exact nickname to construct the @mention format."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "group_code": {
                    "type": "string",
                    "description": "The unique group identifier (group_code).",
                },
                "action": {
                    "type": "string",
                    "enum": ["find", "list_bots", "list_all"],
                    "description": (
                        "find — search a user by name (use when you need to @mention or look up someone); "
                        "list_bots — list bots and Yuanbao AI assistants; "
                        "list_all — list all members."
                    ),
                },
                "name": {
                    "type": "string",
                    "description": (
                        "User name to search (partial match, case-insensitive). "
                        "Required for 'find'. Use the name the user mentioned in the conversation."
                    ),
                },
                "mention": {
                    "type": "boolean",
                    "description": (
                        "Set to true when you need to @mention/at someone in your reply. "
                        "The response will include the exact @mention format to use."
                    ),
                },
            },
            "required": ["group_code", "action"],
        },
    },
    handler=_handle_yb_query_group_members,
    check_fn=_check_yuanbao,
    is_async=True,
    emoji="📋",
)

registry.register(
    name="yb_send_dm",
    toolset=_TOOLSET,
    schema={
        "name": "yb_send_dm",
        "description": (
            "Send a private/direct message (DM) to a user in a group, with optional media files. "
            "This tool automatically looks up the user by name in the group member list "
            "and sends the message. Use this when someone asks to privately message / 私信 / DM a user. "
            "Supports text, images, and file attachments. "
            "You can also provide user_id directly if already known."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "group_code": {
                    "type": "string",
                    "description": (
                        "The group where the target user belongs. "
                        "Extract from chat_id: 'group:328306697' → '328306697'. "
                        "Required when user_id is not provided."
                    ),
                },
                "name": {
                    "type": "string",
                    "description": (
                        "Target user's display name (partial match, case-insensitive). "
                        "Required when user_id is not provided."
                    ),
                },
                "message": {
                    "type": "string",
                    "description": "The message text to send as a DM. Can be empty if only sending media.",
                },
                "user_id": {
                    "type": "string",
                    "description": (
                        "Target user's account ID. If provided, skips the member lookup. "
                        "Usually obtained from a previous yb_query_group_members call."
                    ),
                },
                "media_files": {
                    "type": "array",
                    "description": (
                        "Optional list of media files to send along with the DM. "
                        "Images (.jpg/.png/.gif/.webp/.bmp) are sent as image messages; "
                        "other files are sent as document attachments."
                    ),
                    "items": {
                        "type": "object",
                        "properties": {
                            "path": {
                                "type": "string",
                                "description": "Absolute local file path of the media to send.",
                            },
                            "is_voice": {
                                "type": "boolean",
                                "description": "Whether this file is a voice message (default false).",
                            },
                        },
                        "required": ["path"],
                    },
                },
            },
            "required": [],
        },
    },
    handler=_handle_yb_send_dm,
    check_fn=_check_yuanbao,
    is_async=True,
    emoji="✉️",
)
