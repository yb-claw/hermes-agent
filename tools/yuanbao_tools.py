"""
yuanbao_tools.py - 元宝平台工具集

提供以下工具函数，供 hermes-agent 的 "hermes-yuanbao" toolset 使用：
  - get_group_info          : 查询群基本信息（群名、群主、成员数）
  - get_group_member_list   : 获取群成员列表（支持分页）
  - get_member_info         : 查询用户信息（昵称、角色等）
  - get_group_member_info   : 查询用户在指定群的信息（群名片、角色等）
  - send_reminder           : 在指定秒数后向用户发送提醒（fire-and-forget）

适配器注入：
  在 YuanbaoAdapter.connect() 成功后调用 set_adapter(self) 完成注入。
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import TYPE_CHECKING, Optional

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from gateway.platforms.yuanbao import YuanbaoAdapter

# ---------------------------------------------------------------------------
# 模块级 adapter 引用（延迟注入）
# ---------------------------------------------------------------------------

_adapter_ref: Optional["YuanbaoAdapter"] = None


def set_adapter(adapter: "YuanbaoAdapter") -> None:
    """
    注入 YuanbaoAdapter 实例，在 adapter.connect() 成功后调用。

    Args:
        adapter: 已连接的 YuanbaoAdapter 实例。
    """
    global _adapter_ref
    _adapter_ref = adapter
    logger.info("[yuanbao_tools] adapter injected: %s", getattr(adapter, "name", adapter))


def _get_adapter() -> "YuanbaoAdapter":
    """获取 adapter，未注入时抛出 RuntimeError。"""
    if _adapter_ref is None:
        raise RuntimeError(
            "YuanbaoAdapter not injected. Call yuanbao_tools.set_adapter(adapter) "
            "after connect() succeeds."
        )
    return _adapter_ref


# ---------------------------------------------------------------------------
# 内部 proto 编码辅助
# ---------------------------------------------------------------------------
# 这里直接复用 yuanbao_proto 内部工具函数，避免重复实现 varint/protobuf 编码。

def _import_proto_utils():
    """延迟导入 proto 工具，避免循环依赖。"""
    from gateway.platforms.yuanbao_proto import (  # noqa: PLC0415
        _BIZ_PKG,
        _encode_field,
        _encode_string,
        _encode_varint,
        WT_LEN,
        WT_VARINT,
        encode_biz_msg,
        next_seq_no,
    )
    return _BIZ_PKG, _encode_field, _encode_string, _encode_varint, WT_LEN, WT_VARINT, encode_biz_msg, next_seq_no


def _encode_query_group_info_req(group_code: str) -> bytes:
    """
    编码 QueryGroupInfoReq body。

    Proto 定义（推断自 TS biz-codec.ts）：
      message QueryGroupInfoReq {
        string group_code = 1;
      }
    """
    _BIZ_PKG, _encode_field, _encode_string, _encode_varint, WT_LEN, WT_VARINT, encode_biz_msg, next_seq_no = _import_proto_utils()
    body = _encode_field(1, WT_LEN, _encode_string(group_code))
    req_id = f"qgi_{next_seq_no()}_{uuid.uuid4().hex[:8]}"
    return encode_biz_msg(
        service=_BIZ_PKG,
        method="query_group_info",
        req_id=req_id,
        body=body,
    ), req_id


def _encode_get_group_member_list_req(group_code: str, next_seq: int = 0) -> tuple:
    """
    编码 GetGroupMemberListReq body。

    Proto 定义（推断自 TS biz-codec.ts）：
      message GetGroupMemberListReq {
        string group_code = 1;
        uint64 next_seq   = 2;  // 分页游标，0 表示第一页
      }
    """
    _BIZ_PKG, _encode_field, _encode_string, _encode_varint, WT_LEN, WT_VARINT, encode_biz_msg, next_seq_no_fn = _import_proto_utils()
    body = _encode_field(1, WT_LEN, _encode_string(group_code))
    if next_seq:
        body += _encode_field(2, WT_VARINT, _encode_varint(next_seq))
    req_id = f"gml_{next_seq_no_fn()}_{uuid.uuid4().hex[:8]}"
    return encode_biz_msg(
        service=_BIZ_PKG,
        method="get_group_member_list",
        req_id=req_id,
        body=body,
    ), req_id


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

async def get_group_info(group_code: str) -> dict:
    """
    查询指定群的基本信息，包括群名称、群主和成员数。

    通过元宝 WebSocket biz 接口 `query_group_info` 获取群信息。
    在元宝 App 中，群被称为"派（Pai）"。

    Args:
        group_code: 群唯一标识码（元宝后台的 group_code）。

    Returns:
        dict，包含以下字段：
          - success (bool): 是否查询成功
          - group_code (str): 群号
          - group_name (str): 群名称（成功时）
          - group_size (int): 群成员数（成功时）
          - owner (dict): 群主信息 { user_id, nickname }（成功时）
          - error (str): 错误描述（失败时）
    """
    if not group_code:
        return {"success": False, "error": "group_code is required"}

    adapter = _get_adapter()

    try:
        encoded, req_id = _encode_query_group_info_req(group_code)
        response = await adapter._send_biz_request(encoded, req_id=req_id)

        # 解析响应 body（protobuf bytes）
        body = response.get("body", b"") if isinstance(response, dict) else b""
        if body:
            from gateway.platforms.yuanbao_proto import _parse_fields, _fields_to_dict, _get_string, _get_varint, _get_bytes  # noqa: PLC0415
            fdict = _fields_to_dict(_parse_fields(body))
            code = _get_varint(fdict, 1) or 0
            msg = _get_string(fdict, 2) or ""
            if code != 0:
                return {"success": False, "error": f"API error code={code}, msg={msg}"}

            # group_info 在 field 3（message）
            gi_bytes_list = fdict.get(3, [])
            gi_bytes = gi_bytes_list[0][1] if gi_bytes_list else b""
            if gi_bytes:
                gi_fields = _fields_to_dict(_parse_fields(gi_bytes))
                group_name = _get_string(gi_fields, 1) or ""
                owner_user_id = _get_string(gi_fields, 2) or ""
                owner_nickname = _get_string(gi_fields, 3) or ""
                group_size = _get_varint(gi_fields, 4) or 0
            else:
                group_name = ""
                owner_user_id = ""
                owner_nickname = ""
                group_size = 0

            return {
                "success": True,
                "group_code": group_code,
                "group_name": group_name,
                "group_size": group_size,
                "owner": {
                    "user_id": owner_user_id,
                    "nickname": owner_nickname,
                },
                "note": 'The group is called "派 (Pai)" in the app.',
            }
        else:
            # response 可能已经是解码后的 dict（取决于 adapter 版本）
            gi = response.get("group_info") or {}
            return {
                "success": True,
                "group_code": group_code,
                "group_name": gi.get("group_name", ""),
                "group_size": gi.get("group_size", 0),
                "owner": {
                    "user_id": gi.get("group_owner_user_id", ""),
                    "nickname": gi.get("group_owner_nickname", ""),
                },
            }
    except asyncio.TimeoutError:
        return {"success": False, "error": "Request timed out (10s)"}
    except Exception as exc:
        logger.exception("[yuanbao_tools] get_group_info error")
        return {"success": False, "error": str(exc)}


async def get_group_member_list(group_code: str, next_seq: int = 0) -> dict:
    """
    获取指定群的成员列表，支持分页。

    通过元宝 WebSocket biz 接口 `get_group_member_list` 批量拉取群成员。
    每次返回一批成员及下一页游标；若 next_seq_returned 为 0，则已到最后一页。

    Args:
        group_code: 群唯一标识码（元宝后台的 group_code）。
        next_seq: 分页游标，0 表示第一页；传入上次响应的 next_seq_returned 继续翻页。

    Returns:
        dict，包含以下字段：
          - success (bool): 是否成功
          - group_code (str): 群号
          - member_list (list[dict]): 成员列表，每条含 { user_id, nickname, user_type }
          - next_seq_returned (int): 下一页游标，0 表示已是最后一页
          - error (str): 错误描述（失败时）

        user_type 含义：0=未定义, 1=普通用户, 2=元宝AI, 3=bot
    """
    if not group_code:
        return {"success": False, "error": "group_code is required"}

    adapter = _get_adapter()

    try:
        encoded, req_id = _encode_get_group_member_list_req(group_code, next_seq)
        response = await adapter._send_biz_request(encoded, req_id=req_id)

        body = response.get("body", b"") if isinstance(response, dict) else b""
        if body:
            from gateway.platforms.yuanbao_proto import _parse_fields, _fields_to_dict, _get_string, _get_varint  # noqa: PLC0415
            fdict = _fields_to_dict(_parse_fields(body))
            code = _get_varint(fdict, 1) or 0
            msg = _get_string(fdict, 2) or ""
            if code != 0:
                return {"success": False, "error": f"API error code={code}, msg={msg}"}

            # member_list: repeated message field 3
            member_list = []
            for _, mb in fdict.get(3, []):
                mf = _fields_to_dict(_parse_fields(mb))
                member_list.append({
                    "user_id": _get_string(mf, 1) or "",
                    "nickname": _get_string(mf, 2) or "",
                    "user_type": _get_varint(mf, 3) or 0,
                })

            # next_seq: field 4 (uint64)
            next_seq_returned = _get_varint(fdict, 4) or 0

            return {
                "success": True,
                "group_code": group_code,
                "member_list": member_list,
                "next_seq_returned": next_seq_returned,
            }
        else:
            member_list = [
                {
                    "user_id": m.get("user_id", ""),
                    "nickname": m.get("nick_name", m.get("nickname", "")),
                    "user_type": m.get("user_type", 0),
                }
                for m in (response.get("member_list") or [])
            ]
            return {
                "success": True,
                "group_code": group_code,
                "member_list": member_list,
                "next_seq_returned": response.get("next_seq", 0),
            }
    except asyncio.TimeoutError:
        return {"success": False, "error": "Request timed out (10s)"}
    except Exception as exc:
        logger.exception("[yuanbao_tools] get_group_member_list error")
        return {"success": False, "error": str(exc)}


async def get_member_info(account_id: str) -> dict:
    """
    查询指定用户的基本信息，包括昵称、角色类型等。

    通过遍历已知群的成员列表来查找用户信息。由于元宝协议暂无独立的
    "GetUserInfo" 接口，本工具在运行时从已缓存的群成员数据中匹配用户。

    Args:
        account_id: 用户唯一标识（userId）。

    Returns:
        dict，包含以下字段：
          - success (bool): 是否找到
          - account_id (str): 用户 ID
          - nickname (str): 昵称（成功时）
          - user_type (int): 角色类型（0=未定义, 1=用户, 2=元宝AI, 3=bot）（成功时）
          - error (str): 错误描述（失败时）
    """
    if not account_id:
        return {"success": False, "error": "account_id is required"}

    adapter = _get_adapter()

    # 元宝协议目前没有单独的 GetUserInfo biz 接口。
    # 通过 adapter 的缓存（若存在）或返回基础信息。
    try:
        # 尝试访问 adapter 的成员缓存（由 _receive_loop 维护的群成员快照）
        member_cache = getattr(adapter, "_member_cache", {})
        for group_code, members in member_cache.items():
            for m in members:
                uid = m.get("user_id") or m.get("userId") or ""
                if uid == account_id:
                    return {
                        "success": True,
                        "account_id": account_id,
                        "nickname": m.get("nickname") or m.get("nick_name") or m.get("nickName") or "",
                        "user_type": m.get("user_type") or m.get("userType") or 0,
                        "found_in_group": group_code,
                    }

        # 缓存中未找到，返回最小信息
        return {
            "success": True,
            "account_id": account_id,
            "nickname": "",
            "user_type": 0,
            "note": "User not found in local member cache. "
                    "Consider calling get_group_member_list first to populate cache.",
        }
    except Exception as exc:
        logger.exception("[yuanbao_tools] get_member_info error")
        return {"success": False, "error": str(exc)}


async def get_group_member_info(group_code: str, account_id: str) -> dict:
    """
    查询指定用户在特定群中的信息，包括群昵称和角色等。

    先通过 get_group_member_list 获取群成员列表，再从中匹配目标用户。
    适用于查询用户群内身份（群名片、是否为管理员/机器人等场景）。

    Args:
        group_code: 群唯一标识码（元宝后台的 group_code）。
        account_id: 用户唯一标识（userId）。

    Returns:
        dict，包含以下字段：
          - success (bool): 是否找到
          - group_code (str): 群号
          - account_id (str): 用户 ID
          - nickname (str): 用户在群内的昵称（成功时）
          - user_type (int): 角色类型（0=未定义, 1=用户, 2=元宝AI, 3=bot）（成功时）
          - role_label (str): 角色名称（"user"/"yuanbao"/"bot"/"undefined"）
          - error (str): 错误描述（失败时）
    """
    if not group_code:
        return {"success": False, "error": "group_code is required"}
    if not account_id:
        return {"success": False, "error": "account_id is required"}

    USER_TYPE_LABEL = {0: "undefined", 1: "user", 2: "yuanbao", 3: "bot"}

    try:
        # 拉取第一页成员列表；如需全量可多次翻页，此处取首页以快速响应
        result = await get_group_member_list(group_code, next_seq=0)
        if not result.get("success"):
            return {
                "success": False,
                "error": f"Failed to fetch member list: {result.get('error', 'unknown')}",
            }

        members = result.get("member_list", [])
        for m in members:
            if m.get("user_id") == account_id:
                user_type = m.get("user_type", 0)
                return {
                    "success": True,
                    "group_code": group_code,
                    "account_id": account_id,
                    "nickname": m.get("nickname", ""),
                    "user_type": user_type,
                    "role_label": USER_TYPE_LABEL.get(user_type, f"type_{user_type}"),
                }

        return {
            "success": False,
            "error": f"User {account_id} not found in group {group_code} "
                     f"(checked {len(members)} members in first page).",
        }
    except Exception as exc:
        logger.exception("[yuanbao_tools] get_group_member_info error")
        return {"success": False, "error": str(exc)}


async def send_reminder(
    to_account: str,
    text: str,
    delay_seconds: int,
    chat_id: str = None,
) -> dict:
    """
    在指定秒数后向用户发送提醒消息（非阻塞，fire-and-forget 模式）。

    调用后立即返回，不等待延迟到期。提醒任务在后台异步执行。
    若需要更复杂的定时调度（cron 表达式、周期提醒），请使用 cronjob 工具。

    Args:
        to_account: 接收提醒的用户 ID（元宝 userId）。
        text: 提醒消息的内容文本。
        delay_seconds: 延迟秒数，至少 1 秒。
        chat_id: 可选，指定投递会话（如 "group:xxx" 或 "direct:xxx"）；
                 若为 None，则投递到 "direct:<to_account>"。

    Returns:
        dict，包含以下字段：
          - scheduled (bool): 是否已成功安排任务（始终为 True）
          - to_account (str): 接收方
          - delay_seconds (int): 实际延迟秒数
          - target (str): 最终投递目标
    """
    delay_seconds = max(1, int(delay_seconds))
    target = chat_id if chat_id else f"direct:{to_account}"

    async def _delayed_send():
        try:
            await asyncio.sleep(delay_seconds)
            adapter = _adapter_ref
            if adapter is None:
                logger.warning(
                    "[yuanbao_tools] send_reminder: adapter not available, "
                    "cannot send reminder to %s", target
                )
                return
            await adapter.send(target, text)
            logger.info(
                "[yuanbao_tools] send_reminder delivered to %s after %ds",
                target, delay_seconds
            )
        except Exception:
            logger.exception(
                "[yuanbao_tools] send_reminder failed for %s", target
            )

    # fire-and-forget：立即返回，延迟任务在后台运行
    asyncio.create_task(_delayed_send())

    return {
        "scheduled": True,
        "to_account": to_account,
        "delay_seconds": delay_seconds,
        "target": target,
    }
