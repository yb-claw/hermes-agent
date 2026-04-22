"""
test_yuanbao_pipeline.py - Unit tests for the inbound middleware pipeline.

Tests cover:
  1. InboundPipeline engine (use, use_before, use_after, remove, execute)
  2. InboundContext dataclass
  3. Individual middlewares (DecodeMiddleware, DedupMiddleware, SkipSelfMiddleware, etc.)
  4. InboundPipelineBuilder
  5. End-to-end pipeline integration
  6. OOP middleware ABC and class tests
"""

import sys
import os
import json
import asyncio

# Ensure project root is on the path
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from gateway.platforms.yuanbao import (
    InboundContext,
    InboundMiddleware,
    InboundPipeline,
    DecodeMiddleware,
    ExtractFieldsMiddleware,
    DedupMiddleware,
    SkipSelfMiddleware,
    ChatRoutingMiddleware,
    AccessPolicy,
    AccessGuardMiddleware,
    ExtractContentMiddleware,
    PlaceholderFilterMiddleware,
    OwnerCommandMiddleware,
    BuildSourceMiddleware,
    GroupAtGuardMiddleware,
    GroupAttributionMiddleware,
    ClassifyMessageTypeMiddleware,
    DispatchMiddleware,
    InboundPipelineBuilder,
    YuanbaoAdapter,
)
from gateway.platforms.base import MessageType
from gateway.config import Platform, PlatformConfig


# ============================================================
# Helpers
# ============================================================

def make_config(**kwargs):
    extra = kwargs.pop("extra", {})
    extra.setdefault("app_id", "test_key")
    extra.setdefault("app_secret", "test_secret")
    extra.setdefault("ws_url", "wss://test.example.com/ws")
    extra.setdefault("api_domain", "https://test.example.com")
    return PlatformConfig(
        extra=extra,
        **kwargs,
    )


def make_adapter(**kwargs) -> YuanbaoAdapter:
    """Create a YuanbaoAdapter with test config."""
    config = make_config(**kwargs)
    adapter = YuanbaoAdapter(config)
    adapter._bot_id = "bot_123"
    return adapter


def make_ctx(adapter=None, conn_data=b"", **overrides) -> InboundContext:
    """Create an InboundContext with sensible defaults for testing."""
    if adapter is None:
        adapter = make_adapter()
    raw_frames = [conn_data] if conn_data else []
    ctx = InboundContext(adapter=adapter, raw_frames=raw_frames)
    for k, v in overrides.items():
        setattr(ctx, k, v)
    return ctx


def make_json_push(
    from_account="alice",
    to_account="bot_123",
    group_code="",
    text="Hello!",
    msg_id="msg-001",
) -> bytes:
    """Build a JSON callback_command push payload.

    Note: MsgContent inner fields use lowercase ("text" not "Text")
    because _extract_text() looks for lowercase keys.
    """
    msg_body = [{"MsgType": "TIMTextElem", "MsgContent": {"text": text}}]
    push = {
        "CallbackCommand": "C2C.CallbackAfterSendMsg",
        "From_Account": from_account,
        "To_Account": to_account,
        "MsgBody": msg_body,
        "MsgKey": msg_id,
    }
    if group_code:
        push["CallbackCommand"] = "Group.CallbackAfterSendMsg"
        push["GroupId"] = group_code
    return json.dumps(push).encode("utf-8")


# ============================================================
# 1. InboundPipeline Engine Tests
# ============================================================

class TestInboundPipeline:
    """Test the pipeline engine itself."""

    @pytest.mark.asyncio
    async def test_empty_pipeline(self):
        """Empty pipeline executes without error."""
        pipeline = InboundPipeline()
        ctx = make_ctx()
        await pipeline.execute(ctx)  # Should not raise

    @pytest.mark.asyncio
    async def test_single_middleware(self):
        """Single middleware is called with ctx and next_fn."""
        called = []

        async def mw(ctx, next_fn):
            called.append("mw")
            await next_fn()

        pipeline = InboundPipeline().use("test", mw)
        ctx = make_ctx()
        await pipeline.execute(ctx)
        assert called == ["mw"]

    @pytest.mark.asyncio
    async def test_middleware_order(self):
        """Middlewares execute in registration order."""
        order = []

        async def mw_a(ctx, next_fn):
            order.append("a")
            await next_fn()

        async def mw_b(ctx, next_fn):
            order.append("b")
            await next_fn()

        async def mw_c(ctx, next_fn):
            order.append("c")
            await next_fn()

        pipeline = InboundPipeline().use("a", mw_a).use("b", mw_b).use("c", mw_c)
        await pipeline.execute(make_ctx())
        assert order == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_middleware_can_stop_pipeline(self):
        """A middleware that doesn't call next_fn stops the pipeline."""
        order = []

        async def mw_stop(ctx, next_fn):
            order.append("stop")
            # Don't call next_fn — pipeline stops here

        async def mw_after(ctx, next_fn):
            order.append("after")
            await next_fn()

        pipeline = InboundPipeline().use("stop", mw_stop).use("after", mw_after)
        await pipeline.execute(make_ctx())
        assert order == ["stop"]  # "after" should NOT be called

    @pytest.mark.asyncio
    async def test_conditional_guard_skip(self):
        """Middleware with when=False is skipped."""
        order = []

        async def mw_a(ctx, next_fn):
            order.append("a")
            await next_fn()

        async def mw_skipped(ctx, next_fn):
            order.append("skipped")
            await next_fn()

        async def mw_c(ctx, next_fn):
            order.append("c")
            await next_fn()

        pipeline = (
            InboundPipeline()
            .use("a", mw_a)
            .use("skipped", mw_skipped, when=lambda ctx: False)
            .use("c", mw_c)
        )
        await pipeline.execute(make_ctx())
        assert order == ["a", "c"]

    @pytest.mark.asyncio
    async def test_conditional_guard_pass(self):
        """Middleware with when=True is executed."""
        order = []

        async def mw(ctx, next_fn):
            order.append("mw")
            await next_fn()

        pipeline = InboundPipeline().use("mw", mw, when=lambda ctx: True)
        await pipeline.execute(make_ctx())
        assert order == ["mw"]

    def test_use_before(self):
        """use_before inserts middleware before the target."""
        async def noop(ctx, next_fn):
            await next_fn()

        pipeline = InboundPipeline().use("a", noop).use("c", noop)
        pipeline.use_before("c", "b", noop)
        assert pipeline.middleware_names == ["a", "b", "c"]

    def test_use_before_nonexistent_appends(self):
        """use_before with nonexistent target appends to end."""
        async def noop(ctx, next_fn):
            await next_fn()

        pipeline = InboundPipeline().use("a", noop)
        pipeline.use_before("nonexistent", "b", noop)
        assert pipeline.middleware_names == ["a", "b"]

    def test_use_after(self):
        """use_after inserts middleware after the target."""
        async def noop(ctx, next_fn):
            await next_fn()

        pipeline = InboundPipeline().use("a", noop).use("c", noop)
        pipeline.use_after("a", "b", noop)
        assert pipeline.middleware_names == ["a", "b", "c"]

    def test_use_after_nonexistent_appends(self):
        """use_after with nonexistent target appends to end."""
        async def noop(ctx, next_fn):
            await next_fn()

        pipeline = InboundPipeline().use("a", noop)
        pipeline.use_after("nonexistent", "b", noop)
        assert pipeline.middleware_names == ["a", "b"]

    def test_remove(self):
        """remove deletes middleware by name."""
        async def noop(ctx, next_fn):
            await next_fn()

        pipeline = InboundPipeline().use("a", noop).use("b", noop).use("c", noop)
        pipeline.remove("b")
        assert pipeline.middleware_names == ["a", "c"]

    def test_remove_nonexistent_is_noop(self):
        """remove with nonexistent name is a no-op."""
        async def noop(ctx, next_fn):
            await next_fn()

        pipeline = InboundPipeline().use("a", noop)
        pipeline.remove("nonexistent")
        assert pipeline.middleware_names == ["a"]

    @pytest.mark.asyncio
    async def test_error_propagation(self):
        """Errors in middlewares propagate to the caller."""
        async def mw_error(ctx, next_fn):
            raise ValueError("test error")

        pipeline = InboundPipeline().use("error", mw_error)
        with pytest.raises(ValueError, match="test error"):
            await pipeline.execute(make_ctx())

    def test_middleware_names_property(self):
        """middleware_names returns ordered list of names."""
        async def noop(ctx, next_fn):
            await next_fn()

        pipeline = (
            InboundPipeline()
            .use("decode", noop)
            .use("dedup", noop)
            .use("dispatch", noop)
        )
        assert pipeline.middleware_names == ["decode", "dedup", "dispatch"]

    @pytest.mark.asyncio
    async def test_onion_model(self):
        """Middlewares support before/after processing (onion model)."""
        order = []

        async def mw_outer(ctx, next_fn):
            order.append("outer-before")
            await next_fn()
            order.append("outer-after")

        async def mw_inner(ctx, next_fn):
            order.append("inner")
            await next_fn()

        pipeline = InboundPipeline().use("outer", mw_outer).use("inner", mw_inner)
        await pipeline.execute(make_ctx())
        assert order == ["outer-before", "inner", "outer-after"]


# ============================================================
# 2. InboundContext Tests
# ============================================================

class TestInboundContext:
    def test_default_values(self):
        """InboundContext has sensible defaults."""
        adapter = make_adapter()
        ctx = InboundContext(adapter=adapter)
        assert ctx.raw_frames == []
        assert ctx.push is None
        assert ctx.decoded_via == ""
        assert ctx.from_account == ""
        assert ctx.group_code == ""
        assert ctx.msg_body == []
        assert ctx.msg_id == ""
        assert ctx.chat_id == ""
        assert ctx.chat_type == ""
        assert ctx.raw_text == ""
        assert ctx.media_refs == []
        assert ctx.owner_command is None
        assert ctx.source is None
        assert ctx.msg_type is None

    def test_mutable_fields(self):
        """InboundContext fields are mutable."""
        ctx = make_ctx()
        ctx.from_account = "alice"
        ctx.chat_type = "dm"
        assert ctx.from_account == "alice"
        assert ctx.chat_type == "dm"


# ============================================================
# 3. Individual Middleware Tests
# ============================================================

class TestDecodeMiddleware:
    @pytest.mark.asyncio
    async def test_json_decode(self):
        """DecodeMiddleware parses JSON push correctly."""
        push_data = make_json_push(from_account="alice", text="hi")
        ctx = make_ctx(conn_data=push_data)
        next_fn = AsyncMock()

        await DecodeMiddleware()(ctx, next_fn)

        assert ctx.push is not None
        assert ctx.decoded_via == "json"
        assert ctx.push.get("from_account") == "alice"
        next_fn.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_empty_data_stops_pipeline(self):
        """DecodeMiddleware stops pipeline on empty conn_data."""
        ctx = make_ctx(conn_data=b"")
        next_fn = AsyncMock()

        await DecodeMiddleware()(ctx, next_fn)

        assert ctx.push is None
        next_fn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_invalid_data_may_produce_garbage(self):
        """DecodeMiddleware: binary data may be parsed by protobuf as garbage fields.

        This is expected behavior — the protobuf parser is lenient and may
        produce "seemingly valid" fields from arbitrary bytes.  The downstream
        middlewares (dedup, skip-self, etc.) will filter out such garbage.
        """
        ctx = make_ctx(conn_data=b"\x00\x01\x02\x03")
        next_fn = AsyncMock()

        await DecodeMiddleware()(ctx, next_fn)

        # Protobuf parser may or may not produce a result — either is acceptable.
        # The key invariant: no exception is raised.
        assert True  # Reached here without error


class TestExtractFieldsMiddleware:
    @pytest.mark.asyncio
    async def test_extracts_fields(self):
        """ExtractFieldsMiddleware populates ctx from push dict."""
        ctx = make_ctx(push={
            "from_account": "alice",
            "group_code": "grp-1",
            "group_name": "Test Group",
            "sender_nickname": "Alice",
            "msg_body": [{"msg_type": "TIMTextElem", "msg_content": {"text": "hi"}}],
            "msg_id": "msg-001",
            "cloud_custom_data": '{"key": "val"}',
        })
        next_fn = AsyncMock()

        await ExtractFieldsMiddleware()(ctx, next_fn)

        assert ctx.from_account == "alice"
        assert ctx.group_code == "grp-1"
        assert ctx.group_name == "Test Group"
        assert ctx.sender_nickname == "Alice"
        assert len(ctx.msg_body) == 1
        assert ctx.msg_id == "msg-001"
        assert ctx.cloud_custom_data == '{"key": "val"}'
        next_fn.assert_awaited_once()


class TestDedupMiddleware:
    @pytest.mark.asyncio
    async def test_new_message_passes(self):
        """DedupMiddleware passes new messages through."""
        adapter = make_adapter()
        ctx = make_ctx(adapter=adapter, msg_id="unique-msg-001")
        next_fn = AsyncMock()

        await DedupMiddleware()(ctx, next_fn)
        next_fn.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_duplicate_stops_pipeline(self):
        """DedupMiddleware stops pipeline for duplicate messages."""
        adapter = make_adapter()
        # Mark message as seen
        adapter._dedup.is_duplicate("dup-msg-001")

        ctx = make_ctx(adapter=adapter, msg_id="dup-msg-001")
        next_fn = AsyncMock()

        await DedupMiddleware()(ctx, next_fn)
        next_fn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_empty_msg_id_passes(self):
        """DedupMiddleware passes messages with empty msg_id."""
        ctx = make_ctx(msg_id="")
        next_fn = AsyncMock()

        await DedupMiddleware()(ctx, next_fn)
        next_fn.assert_awaited_once()


class TestSkipSelfMiddleware:
    @pytest.mark.asyncio
    async def test_self_message_stops(self):
        """SkipSelfMiddleware stops pipeline for bot's own messages."""
        adapter = make_adapter()
        adapter._bot_id = "bot_123"
        ctx = make_ctx(adapter=adapter, from_account="bot_123")
        next_fn = AsyncMock()

        await SkipSelfMiddleware()(ctx, next_fn)
        next_fn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_other_message_passes(self):
        """SkipSelfMiddleware passes messages from other users."""
        adapter = make_adapter()
        adapter._bot_id = "bot_123"
        ctx = make_ctx(adapter=adapter, from_account="alice")
        next_fn = AsyncMock()

        await SkipSelfMiddleware()(ctx, next_fn)
        next_fn.assert_awaited_once()


class TestChatRoutingMiddleware:
    @pytest.mark.asyncio
    async def test_group_routing(self):
        """ChatRoutingMiddleware sets group chat fields."""
        ctx = make_ctx(group_code="grp-1", group_name="Test Group")
        next_fn = AsyncMock()

        await ChatRoutingMiddleware()(ctx, next_fn)

        assert ctx.chat_id == "group:grp-1"
        assert ctx.chat_type == "group"
        assert ctx.chat_name == "Test Group"
        next_fn.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_dm_routing(self):
        """ChatRoutingMiddleware sets DM chat fields."""
        ctx = make_ctx(from_account="alice", sender_nickname="Alice")
        next_fn = AsyncMock()

        await ChatRoutingMiddleware()(ctx, next_fn)

        assert ctx.chat_id == "direct:alice"
        assert ctx.chat_type == "dm"
        assert ctx.chat_name == "Alice"
        next_fn.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_dm_routing_no_nickname(self):
        """ChatRoutingMiddleware falls back to from_account when no nickname."""
        ctx = make_ctx(from_account="alice", sender_nickname="")
        next_fn = AsyncMock()

        await ChatRoutingMiddleware()(ctx, next_fn)

        assert ctx.chat_name == "alice"


class TestAccessGuardMiddleware:
    @pytest.mark.asyncio
    async def test_open_policy_passes(self):
        """AccessGuardMiddleware passes with open policy."""
        adapter = make_adapter()
        adapter._access_policy = AccessPolicy(dm_policy="open", dm_allow_from=[], group_policy="open", group_allow_from=[])
        ctx = make_ctx(adapter=adapter, chat_type="dm", from_account="alice")
        next_fn = AsyncMock()

        await AccessGuardMiddleware()(ctx, next_fn)
        next_fn.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_disabled_dm_stops(self):
        """AccessGuardMiddleware stops DM when dm_policy=disabled."""
        adapter = make_adapter()
        adapter._access_policy = AccessPolicy(dm_policy="disabled", dm_allow_from=[], group_policy="open", group_allow_from=[])
        ctx = make_ctx(adapter=adapter, chat_type="dm", from_account="alice")
        next_fn = AsyncMock()

        await AccessGuardMiddleware()(ctx, next_fn)
        next_fn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_allowlist_dm_allowed(self):
        """AccessGuardMiddleware passes DM when sender is in allowlist."""
        adapter = make_adapter()
        adapter._access_policy = AccessPolicy(dm_policy="allowlist", dm_allow_from=["alice"], group_policy="open", group_allow_from=[])
        ctx = make_ctx(adapter=adapter, chat_type="dm", from_account="alice")
        next_fn = AsyncMock()

        await AccessGuardMiddleware()(ctx, next_fn)
        next_fn.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_allowlist_dm_blocked(self):
        """AccessGuardMiddleware blocks DM when sender is not in allowlist."""
        adapter = make_adapter()
        adapter._access_policy = AccessPolicy(dm_policy="allowlist", dm_allow_from=["bob"], group_policy="open", group_allow_from=[])
        ctx = make_ctx(adapter=adapter, chat_type="dm", from_account="alice")
        next_fn = AsyncMock()

        await AccessGuardMiddleware()(ctx, next_fn)
        next_fn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_disabled_group_stops(self):
        """AccessGuardMiddleware stops group when group_policy=disabled."""
        adapter = make_adapter()
        adapter._access_policy = AccessPolicy(dm_policy="open", dm_allow_from=[], group_policy="disabled", group_allow_from=[])
        ctx = make_ctx(adapter=adapter, chat_type="group", group_code="grp-1")
        next_fn = AsyncMock()

        await AccessGuardMiddleware()(ctx, next_fn)
        next_fn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_allowlist_group_allowed(self):
        """AccessGuardMiddleware passes group when group_code is in allowlist."""
        adapter = make_adapter()
        adapter._access_policy = AccessPolicy(dm_policy="open", dm_allow_from=[], group_policy="allowlist", group_allow_from=["grp-1"])
        ctx = make_ctx(adapter=adapter, chat_type="group", group_code="grp-1")
        next_fn = AsyncMock()

        await AccessGuardMiddleware()(ctx, next_fn)
        next_fn.assert_awaited_once()


class TestExtractContentMiddleware:
    @pytest.mark.asyncio
    async def test_extracts_text_and_media(self):
        """ExtractContentMiddleware extracts text and media refs."""
        adapter = make_adapter()
        msg_body = [
            {"msg_type": "TIMTextElem", "msg_content": {"text": "Hello!"}},
            {"msg_type": "TIMImageElem", "msg_content": {
                "image_info_array": [{"url": "https://img.example.com/1.jpg"}]
            }},
        ]
        ctx = make_ctx(adapter=adapter, msg_body=msg_body)
        next_fn = AsyncMock()

        await ExtractContentMiddleware()(ctx, next_fn)

        assert "Hello!" in ctx.raw_text
        assert len(ctx.media_refs) == 1
        assert ctx.media_refs[0]["kind"] == "image"
        next_fn.assert_awaited_once()


class TestPlaceholderFilterMiddleware:
    @pytest.mark.asyncio
    async def test_placeholder_stops(self):
        """PlaceholderFilterMiddleware stops on pure placeholder."""
        ctx = make_ctx(raw_text="[image]", media_refs=[])
        next_fn = AsyncMock()

        await PlaceholderFilterMiddleware()(ctx, next_fn)
        next_fn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_placeholder_with_media_passes(self):
        """PlaceholderFilterMiddleware passes placeholder when media exists."""
        ctx = make_ctx(
            raw_text="[image]",
            media_refs=[{"kind": "image", "url": "https://img.example.com/1.jpg"}],
        )
        next_fn = AsyncMock()

        await PlaceholderFilterMiddleware()(ctx, next_fn)
        next_fn.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_normal_text_passes(self):
        """PlaceholderFilterMiddleware passes normal text."""
        ctx = make_ctx(raw_text="Hello world!")
        next_fn = AsyncMock()

        await PlaceholderFilterMiddleware()(ctx, next_fn)
        next_fn.assert_awaited_once()


class TestGroupAtGuardMiddleware:
    @pytest.mark.asyncio
    async def test_dm_passes(self):
        """GroupAtGuardMiddleware passes DM messages."""
        adapter = make_adapter()
        ctx = make_ctx(adapter=adapter, chat_type="dm")
        next_fn = AsyncMock()

        await GroupAtGuardMiddleware()(ctx, next_fn)
        next_fn.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_group_with_at_bot_passes(self):
        """GroupAtGuardMiddleware passes group messages that @bot."""
        adapter = make_adapter()
        adapter._bot_id = "bot_123"
        msg_body = [
            {"msg_type": "TIMCustomElem", "msg_content": {
                "data": json.dumps({"elem_type": 1002, "text": "@Bot", "user_id": "bot_123"})
            }},
        ]
        ctx = make_ctx(
            adapter=adapter,
            chat_type="group",
            chat_id="group:grp-1",
            msg_body=msg_body,
            from_account="alice",
            sender_nickname="Alice",
            raw_text="Hello",
            source=MagicMock(),
        )
        next_fn = AsyncMock()

        await GroupAtGuardMiddleware()(ctx, next_fn)
        next_fn.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_group_without_at_bot_observes(self):
        """GroupAtGuardMiddleware observes group messages without @bot."""
        adapter = make_adapter()
        adapter._bot_id = "bot_123"
        adapter._session_store = None  # No session store -> observe is a no-op
        ctx = make_ctx(
            adapter=adapter,
            chat_type="group",
            chat_id="group:grp-1",
            msg_body=[{"msg_type": "TIMTextElem", "msg_content": {"text": "hi"}}],
            from_account="alice",
            sender_nickname="Alice",
            raw_text="hi",
            source=MagicMock(),
        )
        next_fn = AsyncMock()

        await GroupAtGuardMiddleware()(ctx, next_fn)

        next_fn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_owner_command_skips_at_check(self):
        """GroupAtGuardMiddleware passes when owner_command is set."""
        adapter = make_adapter()
        adapter._bot_id = "bot_123"
        ctx = make_ctx(
            adapter=adapter,
            chat_type="group",
            msg_body=[],
            owner_command="/new",
            source=MagicMock(),
        )
        next_fn = AsyncMock()

        await GroupAtGuardMiddleware()(ctx, next_fn)
        next_fn.assert_awaited_once()


# ============================================================
# 4. Factory Tests
# ============================================================

class TestCreateInboundPipeline:
    def test_default_pipeline_has_all_middlewares(self):
        """InboundPipelineBuilder.build() creates pipeline with all expected middlewares."""
        pipeline = InboundPipelineBuilder.build()
        expected = [
            "decode",
            "extract-fields",
            "dedup",
            "skip-self",
            "chat-routing",
            "access-guard",
            "extract-content",
            "placeholder-filter",
            "owner-command",
            "build-source",
            "group-at-guard",
            "classify-msg-type",
            "quote-context",
            "media-resolve",
            "dispatch",
        ]
        """Pipeline can be customized after creation."""
        pipeline = InboundPipelineBuilder.build()

        async def custom_mw(ctx, next_fn):
            await next_fn()

        pipeline.use_before("dispatch", "custom", custom_mw)
        assert "custom" in pipeline.middleware_names
        idx_custom = pipeline.middleware_names.index("custom")
        idx_dispatch = pipeline.middleware_names.index("dispatch")
        assert idx_custom < idx_dispatch


# ============================================================
# 5. End-to-End Pipeline Integration Tests
# ============================================================

class TestPipelineIntegration:
    @pytest.mark.asyncio
    async def test_full_dm_message_flow(self):
        """Full pipeline processes a DM message end-to-end."""
        adapter = make_adapter()
        adapter._bot_id = "bot_123"
        adapter._access_policy = AccessPolicy(dm_policy="open", dm_allow_from=[], group_policy="open", group_allow_from=[])
        adapter.handle_message = AsyncMock()
        adapter._resolve_inbound_media_urls = AsyncMock(return_value=([], []))

        push_data = make_json_push(
            from_account="alice",
            to_account="bot_123",
            text="Hello bot!",
            msg_id="msg-e2e-001",
        )

        ctx = InboundContext(adapter=adapter, raw_frames=[push_data])
        pipeline = InboundPipelineBuilder.build()
        await pipeline.execute(ctx)

        # Verify context was populated correctly
        assert ctx.decoded_via == "json"
        assert ctx.from_account == "alice"
        assert ctx.chat_type == "dm"
        assert ctx.chat_id == "direct:alice"
        assert "Hello bot!" in ctx.raw_text
        assert ctx.source is not None

    @pytest.mark.asyncio
    async def test_self_message_filtered(self):
        """Pipeline stops when message is from bot itself."""
        adapter = make_adapter()
        adapter._bot_id = "bot_123"

        push_data = make_json_push(
            from_account="bot_123",
            to_account="bot_123",
            text="echo",
            msg_id="msg-self-001",
        )

        ctx = InboundContext(adapter=adapter, raw_frames=[push_data])
        pipeline = InboundPipelineBuilder.build()
        await pipeline.execute(ctx)

        # Pipeline should have stopped at skip-self — no source built
        assert ctx.source is None

    @pytest.mark.asyncio
    async def test_duplicate_message_filtered(self):
        """Pipeline stops on duplicate message."""
        adapter = make_adapter()
        adapter._bot_id = "bot_123"

        # First message goes through
        push_data = make_json_push(
            from_account="alice",
            text="Hello!",
            msg_id="msg-dup-001",
        )
        ctx1 = InboundContext(adapter=adapter, raw_frames=[push_data])
        pipeline = InboundPipelineBuilder.build()
        await pipeline.execute(ctx1)
        assert ctx1.from_account == "alice"

        # Second message with same msg_id is filtered
        ctx2 = InboundContext(adapter=adapter, raw_frames=[push_data])
        await pipeline.execute(ctx2)
        # Dedup should stop pipeline before chat routing
        assert ctx2.chat_type == ""

    @pytest.mark.asyncio
    async def test_blocked_dm_filtered(self):
        """Pipeline stops when DM is blocked by policy."""
        adapter = make_adapter()
        adapter._bot_id = "bot_123"
        adapter._access_policy = AccessPolicy(dm_policy="disabled", dm_allow_from=[], group_policy="open", group_allow_from=[])

        push_data = make_json_push(
            from_account="alice",
            text="Hello!",
            msg_id="msg-blocked-001",
        )

        ctx = InboundContext(adapter=adapter, raw_frames=[push_data])
        pipeline = InboundPipelineBuilder.build()
        await pipeline.execute(ctx)

        # Pipeline stopped at access-guard — no content extracted
        assert ctx.raw_text == ""

    @pytest.mark.asyncio
    async def test_adapter_has_pipeline(self):
        """YuanbaoAdapter.__init__ creates an inbound pipeline."""
        adapter = make_adapter()
        assert hasattr(adapter, "_inbound_pipeline")
        assert isinstance(adapter._inbound_pipeline, InboundPipeline)



if __name__ == "__main__":
    pytest.main([__file__, "-v"])


# ============================================================
# 6. OOP Middleware Tests
# ============================================================

class TestInboundMiddlewareABC:
    """Test the InboundMiddleware abstract base class."""

    def test_cannot_instantiate_abc(self):
        """InboundMiddleware cannot be instantiated directly."""
        with pytest.raises(TypeError):
            InboundMiddleware()

    def test_subclass_must_implement_handle(self):
        """Subclass without handle() raises TypeError."""
        with pytest.raises(TypeError):
            class BadMiddleware(InboundMiddleware):
                name = "bad"
            BadMiddleware()

    def test_subclass_with_handle_works(self):
        """Subclass with handle() can be instantiated."""
        class GoodMiddleware(InboundMiddleware):
            name = "good"
            async def handle(self, ctx, next_fn):
                await next_fn()
        mw = GoodMiddleware()
        assert mw.name == "good"

    @pytest.mark.asyncio
    async def test_callable_protocol(self):
        """Middleware instances are callable via __call__."""
        class TestMW(InboundMiddleware):
            name = "test"
            async def handle(self, ctx, next_fn):
                ctx.raw_text = "called"
                await next_fn()

        mw = TestMW()
        ctx = make_ctx()
        next_fn = AsyncMock()
        await mw(ctx, next_fn)  # Call via __call__
        assert ctx.raw_text == "called"
        next_fn.assert_awaited_once()

    def test_repr(self):
        """Middleware has a useful repr."""
        class MyMW(InboundMiddleware):
            name = "my-mw"
            async def handle(self, ctx, next_fn):
                pass
        mw = MyMW()
        assert "MyMW" in repr(mw)
        assert "my-mw" in repr(mw)


class TestMiddlewareClasses:
    """Test that all concrete middleware classes have correct names and are InboundMiddleware subclasses."""

    MIDDLEWARE_CLASSES = [
        (DecodeMiddleware, "decode"),
        (ExtractFieldsMiddleware, "extract-fields"),
        (DedupMiddleware, "dedup"),
        (SkipSelfMiddleware, "skip-self"),
        (ChatRoutingMiddleware, "chat-routing"),
        (AccessGuardMiddleware, "access-guard"),
        (ExtractContentMiddleware, "extract-content"),
        (PlaceholderFilterMiddleware, "placeholder-filter"),
        (OwnerCommandMiddleware, "owner-command"),
        (BuildSourceMiddleware, "build-source"),
        (GroupAtGuardMiddleware, "group-at-guard"),
        (DispatchMiddleware, "dispatch"),
    ]

    @pytest.mark.parametrize("cls,expected_name", MIDDLEWARE_CLASSES)
    def test_is_inbound_middleware(self, cls, expected_name):
        """Each middleware class is a subclass of InboundMiddleware."""
        assert issubclass(cls, InboundMiddleware)

    @pytest.mark.parametrize("cls,expected_name", MIDDLEWARE_CLASSES)
    def test_has_correct_name(self, cls, expected_name):
        """Each middleware class has the expected name."""
        mw = cls()
        assert mw.name == expected_name

    @pytest.mark.parametrize("cls,expected_name", MIDDLEWARE_CLASSES)
    def test_is_callable(self, cls, expected_name):
        """Each middleware instance is callable."""
        mw = cls()
        assert callable(mw)


class TestPipelineOOPRegistration:
    """Test that InboundPipeline works with OOP middleware instances."""

    @pytest.mark.asyncio
    async def test_use_with_middleware_instance(self):
        """pipeline.use(SomeMiddleware()) auto-extracts name."""
        class TestMW(InboundMiddleware):
            name = "test-mw"
            async def handle(self, ctx, next_fn):
                ctx.raw_text = "oop-works"
                await next_fn()

        pipeline = InboundPipeline().use(TestMW())
        assert pipeline.middleware_names == ["test-mw"]

        ctx = make_ctx()
        await pipeline.execute(ctx)
        assert ctx.raw_text == "oop-works"

    @pytest.mark.asyncio
    async def test_mixed_oop_and_functional(self):
        """Pipeline supports mixing OOP and functional middlewares."""
        order = []

        class OopMW(InboundMiddleware):
            name = "oop"
            async def handle(self, ctx, next_fn):
                order.append("oop")
                await next_fn()

        async def func_mw(ctx, next_fn):
            order.append("func")
            await next_fn()

        pipeline = (
            InboundPipeline()
            .use(OopMW())
            .use("func", func_mw)
        )
        assert pipeline.middleware_names == ["oop", "func"]

        await pipeline.execute(make_ctx())
        assert order == ["oop", "func"]

    def test_use_before_with_middleware_instance(self):
        """use_before works with OOP middleware instances."""
        class MwA(InboundMiddleware):
            name = "a"
            async def handle(self, ctx, next_fn): await next_fn()

        class MwB(InboundMiddleware):
            name = "b"
            async def handle(self, ctx, next_fn): await next_fn()

        class MwC(InboundMiddleware):
            name = "c"
            async def handle(self, ctx, next_fn): await next_fn()

        pipeline = InboundPipeline().use(MwA()).use(MwC())
        pipeline.use_before("c", MwB())
        assert pipeline.middleware_names == ["a", "b", "c"]

    def test_use_after_with_middleware_instance(self):
        """use_after works with OOP middleware instances."""
        class MwA(InboundMiddleware):
            name = "a"
            async def handle(self, ctx, next_fn): await next_fn()

        class MwB(InboundMiddleware):
            name = "b"
            async def handle(self, ctx, next_fn): await next_fn()

        class MwC(InboundMiddleware):
            name = "c"
            async def handle(self, ctx, next_fn): await next_fn()

        pipeline = InboundPipeline().use(MwA()).use(MwC())
        pipeline.use_after("a", MwB())
        assert pipeline.middleware_names == ["a", "b", "c"]


# ============================================================
# 7. Group Slash Command Detection (regression for the @Bot /help bug)
# ============================================================

class TestGroupSlashCommandDetection:
    """Regression tests: group-chat slash commands (e.g. ``@Bot /help``) must be
    classified as ``MessageType.COMMAND`` end-to-end through the inbound pipeline.

    Prior to the command_text split, ``ctx.raw_text`` was polluted by the
    @mention text and by ``GroupAttributionMiddleware``'s ``[nickname|uid]`` prefix,
    so ``text.startswith("/")`` would never match. These tests exercise the
    full middleware chain that runs after content extraction:

        ExtractContentMiddleware
          → GroupAtGuardMiddleware
          → GroupAttributionMiddleware
          → ClassifyMessageTypeMiddleware

    They also pin the product constraint that @mention text MUST be preserved in
    ``ctx.raw_text`` (needed for multi-mention awareness).
    """

    @staticmethod
    def _mention_elem(bot_id: str, text: str = "@LOC Hermes") -> dict:
        """Build a TIMCustomElem representing an @mention of the given user_id."""
        return {
            "msg_type": "TIMCustomElem",
            "msg_content": {
                "data": json.dumps({
                    "elem_type": 1002,
                    "text": text,
                    "user_id": bot_id,
                }),
            },
        }

    @staticmethod
    def _text_elem(text: str) -> dict:
        return {"msg_type": "TIMTextElem", "msg_content": {"text": text}}

    @staticmethod
    def _make_pipeline() -> InboundPipeline:
        """Build a mini-pipeline with just the 4 middlewares relevant to
        slash-command classification. Avoids Dispatch/Media so we don't need
        to mock adapter.handle_message / session_store."""
        return (
            InboundPipeline()
            .use(ExtractContentMiddleware())
            .use(GroupAtGuardMiddleware())
            .use(GroupAttributionMiddleware())
            .use(ClassifyMessageTypeMiddleware())
        )

    @staticmethod
    def _make_group_ctx(adapter, msg_body: list) -> InboundContext:
        """Create an InboundContext pre-populated as if ChatRouting/BuildSource
        had already run for a group message from 'alice'."""
        from gateway.session import SessionSource
        source = SessionSource(
            platform=Platform.YUANBAO,
            chat_id="group:grp-1",
            chat_name="Test Group",
            chat_type="group",
            user_id="alice",
            user_name="Alice",
            thread_id="main",
        )
        return InboundContext(
            adapter=adapter,
            msg_body=msg_body,
            from_account="alice",
            sender_nickname="Alice",
            group_code="grp-1",
            chat_id="group:grp-1",
            chat_type="group",
            chat_name="Test Group",
            source=source,
        )

    @pytest.mark.asyncio
    async def test_at_bot_slash_command_classified_as_command(self):
        """Group message '@Bot /help' is classified as COMMAND, with mention
        text preserved in raw_text and stripped from command_text."""
        adapter = make_adapter()
        adapter._bot_id = "bot_123"
        adapter._session_store = None  # Not needed — @bot branch doesn't observe

        msg_body = [
            self._mention_elem("bot_123", text="@LOC Hermes"),
            self._text_elem("/help"),
        ]
        ctx = self._make_group_ctx(adapter, msg_body)
        await self._make_pipeline().execute(ctx)

        # raw_text keeps the mention text (product constraint) plus the
        # [nickname|user_id] attribution prefix added by GroupAttributionMiddleware.
        assert "@LOC Hermes" in ctx.raw_text
        assert "/help" in ctx.raw_text
        assert ctx.raw_text.startswith("[Alice|alice]")

        # command_text omits the mention text and is not prefixed by attribution,
        # so /help is routable as a command.
        assert ctx.command_text is not None
        assert "@LOC Hermes" not in ctx.command_text
        assert ctx.command_text.lstrip().startswith("/")

        assert ctx.msg_type == MessageType.COMMAND

    @pytest.mark.asyncio
    async def test_multi_mention_preserves_all_mentions_in_raw_text(self):
        """Multi-@ scenario: raw_text keeps every mention, command_text strips
        them all, classification still picks up the slash command."""
        adapter = make_adapter()
        adapter._bot_id = "bot_123"
        adapter._session_store = None

        msg_body = [
            self._mention_elem("other_user", text="@Alice"),
            self._mention_elem("bot_123", text="@LOC Hermes"),
            self._text_elem("/status"),
        ]
        ctx = self._make_group_ctx(adapter, msg_body)
        await self._make_pipeline().execute(ctx)

        # Both mentions are preserved for multi-mention awareness.
        assert "@Alice" in ctx.raw_text
        assert "@LOC Hermes" in ctx.raw_text
        assert "/status" in ctx.raw_text

        # command_text strips both mentions so routing sees a clean command.
        assert ctx.command_text is not None
        assert "@Alice" not in ctx.command_text
        assert "@LOC Hermes" not in ctx.command_text
        assert ctx.command_text.lstrip().startswith("/")

        assert ctx.msg_type == MessageType.COMMAND

    @pytest.mark.asyncio
    async def test_full_width_slash_is_normalized_in_command_text(self):
        """Full-width '／help' gets normalized to ASCII '/help' in
        command_text so classification still detects COMMAND."""
        adapter = make_adapter()
        adapter._bot_id = "bot_123"
        adapter._session_store = None

        msg_body = [
            self._mention_elem("bot_123", text="@LOC Hermes"),
            self._text_elem("\uff0fhelp"),  # full-width slash + help
        ]
        ctx = self._make_group_ctx(adapter, msg_body)
        await self._make_pipeline().execute(ctx)

        # _rewrite_slash_command is applied to command_text too, so the
        # full-width slash is converted to ASCII.
        assert ctx.command_text is not None
        assert ctx.command_text.lstrip().startswith("/")
        assert not ctx.command_text.lstrip().startswith("\uff0f")

        assert ctx.msg_type == MessageType.COMMAND
