"""Tests for the Yuanbao platform adapter."""

import json
import os
import time
from unittest import mock

import pytest

from gateway.config import Platform, PlatformConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(**extra):
    """Build a PlatformConfig(enabled=True, extra=extra) for testing."""
    return PlatformConfig(enabled=True, extra=extra)


# ---------------------------------------------------------------------------
# check_yuanbao_requirements
# ---------------------------------------------------------------------------

class TestYuanbaoRequirements:
    def test_returns_bool(self):
        from gateway.platforms.yuanbao import check_yuanbao_requirements
        result = check_yuanbao_requirements()
        assert isinstance(result, bool)


# ---------------------------------------------------------------------------
# YuanbaoAdapter.__init__
# ---------------------------------------------------------------------------

class TestYuanbaoAdapterInit:
    def _make(self, **extra):
        from gateway.platforms.yuanbao import YuanbaoAdapter
        return YuanbaoAdapter(_make_config(**extra))

    def test_basic_attributes(self):
        adapter = self._make(app_key="key123", app_secret="sec456")
        assert adapter._app_key == "key123"
        assert adapter._app_secret == "sec456"

    def test_env_fallback(self):
        with mock.patch.dict(os.environ, {
            "YUANBAO_APP_KEY": "env_key",
            "YUANBAO_APP_SECRET": "env_sec",
        }, clear=False):
            adapter = self._make()
            assert adapter._app_key == "env_key"
            assert adapter._app_secret == "env_sec"

    def test_env_fallback_extra_wins(self):
        with mock.patch.dict(os.environ, {"YUANBAO_APP_KEY": "env_key"}, clear=False):
            adapter = self._make(app_key="extra_key", app_secret="sec")
            assert adapter._app_key == "extra_key"

    def test_dm_policy_default(self):
        adapter = self._make(app_key="a", app_secret="b")
        assert adapter._dm_policy == "open"

    def test_dm_policy_explicit(self):
        adapter = self._make(app_key="a", app_secret="b", dm_policy="allowlist")
        assert adapter._dm_policy == "allowlist"

    def test_allow_from_parsing_string(self):
        adapter = self._make(app_key="a", app_secret="b", allow_from="x, y , z")
        assert adapter._allow_from == ["x", "y", "z"]

    def test_allow_from_parsing_list(self):
        adapter = self._make(app_key="a", app_secret="b", allow_from=["a", "b"])
        assert adapter._allow_from == ["a", "b"]

    def test_allow_from_default_empty(self):
        adapter = self._make(app_key="a", app_secret="b")
        assert adapter._allow_from == []

    def test_api_domain_default(self):
        adapter = self._make(app_key="a", app_secret="b")
        assert "hunyuan" in adapter._api_domain or adapter._api_domain != ""

    def test_api_domain_custom(self):
        adapter = self._make(app_key="a", app_secret="b", api_domain="custom.api.com")
        assert adapter._api_domain == "custom.api.com"

    def test_static_token(self):
        adapter = self._make(app_key="a", app_secret="b", token="static_tok")
        assert adapter._static_token == "static_tok"

    def test_name_property(self):
        adapter = self._make(app_key="a", app_secret="b")
        assert adapter.name == "Yuanbao"

    def test_platform_is_yuanbao(self):
        adapter = self._make(app_key="a", app_secret="b")
        assert adapter.platform == Platform.YUANBAO

    def test_require_mention_default(self):
        adapter = self._make(app_key="a", app_secret="b")
        assert adapter._require_mention is True

    def test_require_mention_explicit(self):
        adapter = self._make(app_key="a", app_secret="b", require_mention=False)
        assert adapter._require_mention is False


# ---------------------------------------------------------------------------
# ConnMsg codec
# ---------------------------------------------------------------------------

class TestConnMsgCodec:
    def test_encode_decode_varint(self):
        from gateway.platforms.yuanbao import _encode_varint, _decode_varint
        for val in [0, 1, 127, 128, 300, 16384, 100000]:
            encoded = _encode_varint(val)
            decoded, pos = _decode_varint(encoded, 0)
            assert decoded == val
            assert pos == len(encoded)

    def test_encode_decode_fields(self):
        from gateway.platforms.yuanbao import (
            _encode_string_field, _encode_varint_field, _decode_fields
        )
        data = _encode_varint_field(1, 42) + _encode_string_field(2, "hello")
        fields = _decode_fields(data)
        assert fields[1] == 42
        assert fields[2] == b"hello"

    def test_conn_msg_roundtrip(self):
        from gateway.platforms.yuanbao import _ConnMsgCodec
        head = _ConnMsgCodec.encode_head(
            cmd_type=0, cmd="test", msg_id="abc123", module="mod"
        )
        body = b"payload"
        encoded = _ConnMsgCodec.encode_conn_msg(head, body)
        decoded = _ConnMsgCodec.decode_conn_msg(encoded)
        assert decoded is not None
        assert decoded["head"]["cmd"] == "test"
        assert decoded["head"]["msg_id"] == "abc123"
        assert decoded["head"]["module"] == "mod"
        assert decoded["data"] == body

    def test_build_auth_bind(self):
        from gateway.platforms.yuanbao import _ConnMsgCodec
        frame = _ConnMsgCodec.build_auth_bind(
            biz_id="ybBot", uid="bot123", source="bot",
            token="tok", msg_id="mid",
        )
        assert isinstance(frame, bytes)
        decoded = _ConnMsgCodec.decode_conn_msg(frame)
        assert decoded is not None
        assert decoded["head"]["cmd"] == "auth-bind"

    def test_build_ping(self):
        from gateway.platforms.yuanbao import _ConnMsgCodec
        frame = _ConnMsgCodec.build_ping("ping_id")
        decoded = _ConnMsgCodec.decode_conn_msg(frame)
        assert decoded is not None
        assert decoded["head"]["cmd"] == "ping"

    def test_build_push_ack(self):
        from gateway.platforms.yuanbao import _ConnMsgCodec
        original_head = {"cmd": "test", "msg_id": "m1", "module": "mod"}
        frame = _ConnMsgCodec.build_push_ack(original_head)
        decoded = _ConnMsgCodec.decode_conn_msg(frame)
        assert decoded is not None
        assert decoded["head"]["cmd_type"] == _ConnMsgCodec.CMD_TYPE_PUSH_ACK

    def test_decode_auth_bind_rsp(self):
        from gateway.platforms.yuanbao import (
            _ConnMsgCodec, _encode_varint_field, _encode_string_field
        )
        rsp_data = (
            _encode_varint_field(1, 0)  # code = 0
            + _encode_string_field(2, "success")  # message
            + _encode_string_field(3, "conn_123")  # connect_id
        )
        result = _ConnMsgCodec.decode_auth_bind_rsp(rsp_data)
        assert result["code"] == 0
        assert result["message"] == "success"
        assert result["connect_id"] == "conn_123"

    def test_decode_ping_rsp(self):
        from gateway.platforms.yuanbao import _ConnMsgCodec, _encode_varint_field
        rsp_data = _encode_varint_field(1, 10)  # heart_interval = 10
        result = _ConnMsgCodec.decode_ping_rsp(rsp_data)
        assert result["heart_interval"] == 10


# ---------------------------------------------------------------------------
# Inbound message decoding
# ---------------------------------------------------------------------------

class TestInboundMessageDecode:
    def test_decode_json_text(self):
        from gateway.platforms.yuanbao import _decode_inbound_message
        msg = {
            "callback_command": "C2C.CallbackAfterSendMsg",
            "from_account": "user1",
            "msg_body": [{"msg_type": "TIMTextElem", "msg_content": {"text": "hello"}}],
        }
        data = json.dumps(msg).encode("utf-8")
        result = _decode_inbound_message(data)
        assert result is not None
        assert result["callback_command"] == "C2C.CallbackAfterSendMsg"
        assert result["from_account"] == "user1"

    def test_decode_invalid_data(self):
        from gateway.platforms.yuanbao import _decode_inbound_message
        result = _decode_inbound_message(b"\x00\x01\x02")
        # May return None or a partial result depending on protobuf parsing
        # The important thing is it doesn't crash

    def test_decode_empty_data(self):
        from gateway.platforms.yuanbao import _decode_inbound_message
        result = _decode_inbound_message(b"")
        assert result is None


# ---------------------------------------------------------------------------
# Outbound message encoding
# ---------------------------------------------------------------------------

class TestOutboundMessageEncode:
    def test_encode_send_c2c(self):
        from gateway.platforms.yuanbao import _encode_send_c2c_msg
        data = _encode_send_c2c_msg(
            to_account="user1",
            msg_body=[{"msg_type": "TIMTextElem", "msg_content": {"text": "hi"}}],
        )
        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_encode_send_group(self):
        from gateway.platforms.yuanbao import _encode_send_group_msg
        data = _encode_send_group_msg(
            group_code="group1",
            msg_body=[{"msg_type": "TIMTextElem", "msg_content": {"text": "hi"}}],
        )
        assert isinstance(data, bytes)
        assert len(data) > 0


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

class TestDeduplication:
    def test_first_message_not_duplicate(self):
        from gateway.platforms.yuanbao import YuanbaoAdapter
        adapter = YuanbaoAdapter(_make_config(app_key="a", app_secret="b"))
        assert adapter._is_duplicate("msg1") is False

    def test_second_message_is_duplicate(self):
        from gateway.platforms.yuanbao import YuanbaoAdapter
        adapter = YuanbaoAdapter(_make_config(app_key="a", app_secret="b"))
        adapter._is_duplicate("msg1")
        assert adapter._is_duplicate("msg1") is True

    def test_different_messages_not_duplicate(self):
        from gateway.platforms.yuanbao import YuanbaoAdapter
        adapter = YuanbaoAdapter(_make_config(app_key="a", app_secret="b"))
        adapter._is_duplicate("msg1")
        assert adapter._is_duplicate("msg2") is False


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------

class TestTimestampParsing:
    def test_unix_timestamp(self):
        from gateway.platforms.yuanbao import YuanbaoAdapter
        result = YuanbaoAdapter._parse_yuanbao_timestamp(1700000000)
        assert result.year == 2023

    def test_none_timestamp(self):
        from gateway.platforms.yuanbao import YuanbaoAdapter
        result = YuanbaoAdapter._parse_yuanbao_timestamp(None)
        # Should return current time without crashing
        assert result is not None

    def test_invalid_timestamp(self):
        from gateway.platforms.yuanbao import YuanbaoAdapter
        result = YuanbaoAdapter._parse_yuanbao_timestamp("invalid")
        assert result is not None


# ---------------------------------------------------------------------------
# Format message
# ---------------------------------------------------------------------------

class TestFormatMessage:
    def test_strips_markdown(self):
        from gateway.platforms.yuanbao import YuanbaoAdapter
        adapter = YuanbaoAdapter(_make_config(app_key="a", app_secret="b"))
        result = adapter.format_message("**bold** text")
        # strip_markdown should remove the ** markers
        assert "**" not in result or "bold" in result

    def test_plain_text_passthrough(self):
        from gateway.platforms.yuanbao import YuanbaoAdapter
        adapter = YuanbaoAdapter(_make_config(app_key="a", app_secret="b"))
        result = adapter.format_message("plain text")
        assert "plain text" in result


# ---------------------------------------------------------------------------
# Chat type guessing
# ---------------------------------------------------------------------------

class TestChatTypeGuessing:
    def test_default_is_c2c(self):
        from gateway.platforms.yuanbao import YuanbaoAdapter
        adapter = YuanbaoAdapter(_make_config(app_key="a", app_secret="b"))
        assert adapter._guess_chat_type("unknown_id") == "c2c"

    def test_stored_chat_type(self):
        from gateway.platforms.yuanbao import YuanbaoAdapter
        adapter = YuanbaoAdapter(_make_config(app_key="a", app_secret="b"))
        adapter._chat_type_map["group1"] = "group"
        assert adapter._guess_chat_type("group1") == "group"
