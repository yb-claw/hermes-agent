"""
test_yuanbao_media_resolve_perf.py — Benchmark / RED-phase tests for
MediaResolveMiddleware performance under heavy-image group scenarios
(e.g. 跑步打卡群: 用户每日上传 大量跑步图片).

Goal:
  Quantify each suspected root cause as a measurable wall-clock cost,
  using deterministic mocks (no network), so that subsequent fixes can
  be compared against this baseline.

Suspected root causes (read into context from systematic-debugging Phase 1):
  R1. Serial download in `_resolve_media_urls` (for ref in refs: await ...)
  R2. Serial download in `_collect_observed_media` (for rid in order: await ...)
  R3. Lookback window over-broad: scans last 50 transcript msgs, no sender
      / time filter, so other people's stale images get hydrated
  R4. 401 token cascade: each ref independently triggers force_refresh

Mocking strategy (per Phase 2 plan, "推荐" branch):
  - Method-level mocks: patch `_resolve_download_url`, `_download_and_cache`,
    `_resolve_by_resource_id` directly. Faster, more stable than HTTP-level.
  - asyncio.sleep is used to simulate per-call latency. Each helper call
    pays ~LATENCY_PER_CALL_S; total wall-clock under serial = N * latency,
    under ideal concurrency = ~latency.
  - Soft assertions: only assert on order-of-magnitude (serial > k *
    concurrent_lower_bound), no hard millisecond thresholds (CI jitter).

Run with `pytest -s tests/test_yuanbao_media_resolve_perf.py` to see the
measurement table printed at the end.
"""

import sys
import os
import asyncio
import time
from typing import List, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

# Ensure project root is on the path
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import pytest

from gateway.platforms.yuanbao import (
    MediaResolveMiddleware,
    OBSERVED_MEDIA_BACKFILL_MAX_RESOLVE_PER_TURN,
    YuanbaoAdapter,
)
from gateway.config import PlatformConfig

# ============================================================
# Tunables — kept low so the suite finishes in a few seconds
# ============================================================

# Per-call simulated latency. Chosen to be (a) larger than asyncio.sleep
# scheduling jitter (~5ms) and (b) small enough to keep the suite fast.
LATENCY_PER_CALL_S = 0.05  # 50 ms per token-fetch / per-download leg

# A "real" image fetch is roughly: 1 token-RTT + 1 HTTPS download.
# In _resolve_media_urls that's _resolve_download_url + _download_and_cache.
# In _collect_observed_media that's _resolve_by_resource_id + _download_and_cache.
LATENCY_PER_REF_S = 2 * LATENCY_PER_CALL_S  # ~100ms per ref end-to-end


# ============================================================
# Result collector — one process-wide table for human-readable output
# ============================================================

_results: List[Tuple[str, float, int, float]] = []  # (scenario, total_s, refs, per_ref_s)


def _record(scenario: str, total_s: float, refs: int) -> None:
    per_ref = (total_s / refs) if refs else 0.0
    _results.append((scenario, total_s, refs, per_ref))


def teardown_module(_module):
    """Print a human-readable benchmark table at the end of the module."""
    if not _results:
        return
    print("\n" + "=" * 78)
    print(f"{'Scenario':<48}{'Time(ms)':>10}{'Refs':>6}{'Per-ref(ms)':>14}")
    print("-" * 78)
    for name, total, refs, per_ref in _results:
        print(f"{name:<48}{total*1000:>10.0f}{refs:>6}{per_ref*1000:>14.0f}")
    print("=" * 78)


# ============================================================
# Adapter / mock helpers
# ============================================================


def _make_adapter() -> YuanbaoAdapter:
    cfg = PlatformConfig(extra={
        "app_id": "test_key",
        "app_secret": "test_secret",
        "ws_url": "wss://test.example.com/ws",
        "api_domain": "https://test.example.com",
    })
    adapter = YuanbaoAdapter(cfg)
    adapter._bot_id = "bot_perf"
    return adapter


def _make_media_refs(n: int) -> List[dict]:
    """Build N inbound media-refs that are all 'image' kind with a yb-resource URL."""
    return [
        {
            "kind": "image",
            "url": (
                "https://hunyuan.tencent.com/api/resource/download"
                f"?resourceId=run_{i:04d}"
            ),
            "name": f"run_{i:04d}.jpg",
        }
        for i in range(n)
    ]


def _make_session_store_with_history(history: List[dict]) -> MagicMock:
    """Build a fake adapter._session_store that returns a fixed transcript."""
    store = MagicMock()
    session_entry = MagicMock()
    session_entry.session_id = "sess-perf"
    store.get_or_create_session.return_value = session_entry
    store.load_transcript.return_value = history
    return store


async def _slow_resolve_download_url(adapter, url: str) -> str:
    """Mock for _resolve_download_url: simulate one token-RTT, return placeholder real URL."""
    await asyncio.sleep(LATENCY_PER_CALL_S)
    return url + "&realUrl=1"


async def _slow_resolve_by_resource_id(adapter, resource_id: str) -> str:
    """Mock for _resolve_by_resource_id: simulate one token-RTT, return placeholder real URL."""
    await asyncio.sleep(LATENCY_PER_CALL_S)
    return f"https://cos.example.com/file/{resource_id}.jpg"


async def _slow_download_and_cache(adapter, *, fetch_url: str, kind: str,
                                    file_name=None, log_tag: str = ""):
    """Mock for _download_and_cache: simulate one HTTPS download, return fake (path, mime)."""
    await asyncio.sleep(LATENCY_PER_CALL_S)
    return (f"/tmp/cache/{hash(fetch_url) & 0xffff:04x}.jpg", "image/jpeg")


# ============================================================
# Scenario 1 — _resolve_media_urls: serial baseline for placeholder URLs
# ============================================================


@pytest.mark.asyncio
async def test_resolve_media_urls_concurrent_12_refs():
    """
    Post-fix expectation (Phase A):
      _resolve_media_urls runs per-ref work concurrently up to
      MEDIA_RESOLVE_CONCURRENCY. With 12 refs and concurrency=4, expected
      wall-clock is ceil(12/4) * 2 * LATENCY_PER_CALL ≈ 300ms,
      vs. ~1.2s for the pre-fix serial implementation.
    """
    from gateway.platforms.yuanbao import MEDIA_RESOLVE_CONCURRENCY

    adapter = _make_adapter()
    refs = _make_media_refs(OBSERVED_MEDIA_BACKFILL_MAX_RESOLVE_PER_TURN)  # 12
    expected_concurrent_s = (
        ((len(refs) + MEDIA_RESOLVE_CONCURRENCY - 1) // MEDIA_RESOLVE_CONCURRENCY)
        * LATENCY_PER_REF_S
    )

    with patch.object(MediaResolveMiddleware, "_resolve_download_url",
                      new=_slow_resolve_download_url), \
         patch.object(MediaResolveMiddleware, "_download_and_cache",
                      new=_slow_download_and_cache):
        t0 = time.monotonic()
        urls, mimes = await MediaResolveMiddleware._resolve_media_urls(adapter, refs)
        elapsed = time.monotonic() - t0

    assert len(urls) == 12
    assert len(mimes) == 12
    _record(
        f"S1 _resolve_media_urls concurrent (12 refs, conc={MEDIA_RESOLVE_CONCURRENCY})",
        elapsed, len(urls),
    )

    serial_floor = len(refs) * LATENCY_PER_REF_S * 0.85  # what serial would take
    concurrent_ceiling = expected_concurrent_s * 2.0      # 2x slack for jitter
    assert elapsed < serial_floor, (
        f"Expected concurrent execution; elapsed={elapsed:.2f}s ≥ serial floor "
        f"{serial_floor:.2f}s — concurrency seems broken."
    )
    assert elapsed < concurrent_ceiling, (
        f"Expected ~{expected_concurrent_s:.2f}s for conc={MEDIA_RESOLVE_CONCURRENCY}, "
        f"got {elapsed:.2f}s — semaphore may be over-throttling."
    )


@pytest.mark.asyncio
async def test_resolve_media_urls_concurrent_5_refs():
    """5 refs with concurrency=4 → 2 batches ≈ 200ms, well under serial 500ms."""
    from gateway.platforms.yuanbao import MEDIA_RESOLVE_CONCURRENCY

    adapter = _make_adapter()
    refs = _make_media_refs(5)

    with patch.object(MediaResolveMiddleware, "_resolve_download_url",
                      new=_slow_resolve_download_url), \
         patch.object(MediaResolveMiddleware, "_download_and_cache",
                      new=_slow_download_and_cache):
        t0 = time.monotonic()
        urls, _ = await MediaResolveMiddleware._resolve_media_urls(adapter, refs)
        elapsed = time.monotonic() - t0

    assert len(urls) == 5
    _record(
        f"S1b _resolve_media_urls concurrent (5 refs, conc={MEDIA_RESOLVE_CONCURRENCY})",
        elapsed, len(urls),
    )

    serial_floor = len(refs) * LATENCY_PER_REF_S * 0.85
    assert elapsed < serial_floor, (
        f"Expected concurrent < serial floor {serial_floor:.2f}s, got {elapsed:.2f}s"
    )


@pytest.mark.asyncio
async def test_resolve_media_urls_preserves_order():
    """
    Concurrent execution must not reorder results — downstream callers
    correlate paths[i] with refs[i] by position. Use staggered delays so
    refs finish in a different order than they were submitted, and verify
    the output still matches input order.
    """
    adapter = _make_adapter()
    refs = _make_media_refs(8)

    async def _stub_resolve(adapter_, url):
        # Echo the rid-bearing url back as fetch_url
        return f"https://cdn.example.com/{url}"

    async def _stub_download(adapter_, *, fetch_url, kind, file_name=None, log_tag=""):
        # Extract index from url like "https://yuanbao.test/file/test_image_3"
        idx_str = fetch_url.rsplit("_", 1)[-1]
        try:
            idx = int(idx_str)
        except ValueError:
            idx = 0
        # Reverse-staggered delay: later refs finish first
        await asyncio.sleep(0.005 * (10 - idx))
        return (f"/cache/{idx}.jpg", "image/jpeg")

    with patch.object(MediaResolveMiddleware, "_resolve_download_url", new=_stub_resolve), \
         patch.object(MediaResolveMiddleware, "_download_and_cache", new=_stub_download):
        urls, _ = await MediaResolveMiddleware._resolve_media_urls(adapter, refs)

    expected = [f"/cache/{i}.jpg" for i in range(8)]
    assert urls == expected, f"Order broken: {urls} != {expected}"


# ============================================================
# Scenario 3 — concurrency upper bound: what's the best we *could* do?
# ============================================================


@pytest.mark.asyncio
async def test_concurrent_upper_bound_demonstration_12_refs():
    """
    Reference benchmark — NOT exercising production code. Demonstrates that
    when 12 logically-independent fetches are issued via asyncio.gather,
    wall-clock collapses from ~12*latency to ~latency. This is the headroom
    the fix can recover.
    """
    async def one_ref(_i: int):
        await asyncio.sleep(LATENCY_PER_CALL_S)  # token
        await asyncio.sleep(LATENCY_PER_CALL_S)  # download
        return f"path_{_i}"

    t0 = time.monotonic()
    paths = await asyncio.gather(*(one_ref(i) for i in range(12)))
    elapsed = time.monotonic() - t0

    assert len(paths) == 12
    _record("REF concurrent upper bound (12 refs, gather)", elapsed, len(paths))

    # Concurrent must be at least as long as one ref's two sequential calls,
    # but dramatically less than 12x.
    assert elapsed >= LATENCY_PER_REF_S * 0.85
    assert elapsed < 12 * LATENCY_PER_REF_S * 0.5, (
        f"Concurrent run should be << serial; got {elapsed:.2f}s"
    )


# ============================================================
# Scenario 4 — sanity: empty-input fast paths
# ============================================================


@pytest.mark.asyncio
async def test_resolve_media_urls_empty_returns_immediately():
    adapter = _make_adapter()
    t0 = time.monotonic()
    urls, mimes = await MediaResolveMiddleware._resolve_media_urls(adapter, [])
    elapsed = time.monotonic() - t0
    assert urls == [] and mimes == []
    assert elapsed < 0.05  # no awaits should fire


@pytest.mark.asyncio
async def test_collect_observed_media_no_session_store_returns_immediately():
    adapter = _make_adapter()
    # Don't set _session_store — should short-circuit
    source = MagicMock()
    source.from_account = "alice"
    t0 = time.monotonic()
    paths, mimes = await MediaResolveMiddleware._collect_observed_media(adapter, source)
    elapsed = time.monotonic() - t0
    assert paths == [] and mimes == []
    assert elapsed < 0.05
