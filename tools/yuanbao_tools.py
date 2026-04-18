"""
yuanbao_tools.py - 元宝平台工具集

提供以下工具函数，供 hermes-agent 的 "hermes-yuanbao" toolset 使用：
  - get_group_info        : 查询群基本信息（群名、群主、成员数）
  - query_group_members   : 查询群成员（按名搜索、列举 bot、列举全部）

The active adapter singleton lives in ``gateway.platforms.yuanbao`` and is
accessed via ``get_active_adapter()``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

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

