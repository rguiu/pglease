"""Unit tests for AsyncPGLease — underlying PGLease is mocked, no DB required."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pglease.async_pglease import AsyncPGLease
from pglease.exceptions import AcquisitionError
from pglease.models import Lease
from pglease.pglease import PGLease

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(UTC)


def _lease(task: str = "task") -> Lease:
    now = _now()
    return Lease(task, "worker", now, now + timedelta(seconds=60), now)


def _make_sync_mock(*, acquired: bool = True, task: str = "task") -> MagicMock:
    sync = MagicMock(spec=PGLease)
    sync.owner_id = "test-worker"
    lease = _lease(task)
    sync.try_acquire.return_value = acquired
    sync.release.return_value = True
    sync.get_lease.return_value = lease
    sync.list_leases.return_value = [lease]
    sync.cleanup_expired.return_value = 5
    sync.close.return_value = None
    return sync


def _make_async(*, acquired: bool = True, task: str = "task") -> tuple[AsyncPGLease, MagicMock]:
    sync = _make_sync_mock(acquired=acquired, task=task)
    apg = AsyncPGLease(sync)  # pass PGLease instance directly
    return apg, sync


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestAsyncPGLeaseInit:
    async def test_accepts_pglease_instance(self):
        sync = _make_sync_mock()
        apg = AsyncPGLease(sync)
        assert apg._sync is sync

    async def test_owner_id_delegated(self):
        sync = _make_sync_mock()
        apg = AsyncPGLease(sync)
        assert apg.owner_id == "test-worker"

    async def test_accepts_connection_string(self):
        with patch("pglease.async_pglease.PGLease") as mock_pglease:
            mock_pglease.return_value = _make_sync_mock()
            AsyncPGLease("postgresql://user:pass@localhost/db", owner_id="w")
            mock_pglease.assert_called_once()


# ---------------------------------------------------------------------------
# Core async API
# ---------------------------------------------------------------------------


class TestAsyncCoreAPI:
    async def test_try_acquire_true(self):
        apg, sync = _make_async(acquired=True)
        result = await apg.try_acquire("task", ttl=60)
        assert result is True
        sync.try_acquire.assert_called_once_with("task", 60)

    async def test_try_acquire_false(self):
        apg, sync = _make_async(acquired=False)
        result = await apg.try_acquire("task", ttl=60)
        assert result is False

    async def test_release(self):
        apg, sync = _make_async()
        result = await apg.release("task")
        assert result is True
        sync.release.assert_called_once_with("task")

    async def test_get_lease_returns_lease(self):
        apg, sync = _make_async()
        lease = await apg.get_lease("task")
        assert lease is not None
        sync.get_lease.assert_called_once_with("task")

    async def test_get_lease_returns_none(self):
        apg, sync = _make_async()
        sync.get_lease.return_value = None
        lease = await apg.get_lease("missing")
        assert lease is None

    async def test_list_leases(self):
        apg, sync = _make_async()
        leases = await apg.list_leases()
        assert len(leases) == 1
        sync.list_leases.assert_called_once()

    async def test_cleanup_expired(self):
        apg, sync = _make_async()
        count = await apg.cleanup_expired()
        assert count == 5
        sync.cleanup_expired.assert_called_once()

    async def test_close(self):
        apg, sync = _make_async()
        await apg.close()
        sync.close.assert_called_once()


# ---------------------------------------------------------------------------
# acquire() async context manager
# ---------------------------------------------------------------------------


class TestAsyncAcquireContextManager:
    async def test_acquired_true(self):
        apg, _ = _make_async(acquired=True)
        async with apg.acquire("task", ttl=60) as acquired:
            assert acquired is True

    async def test_acquired_false(self):
        apg, _ = _make_async(acquired=False)
        async with apg.acquire("task", ttl=60) as acquired:
            assert acquired is False

    async def test_releases_after_body(self):
        apg, sync = _make_async(acquired=True)
        async with apg.acquire("task", ttl=60):
            pass
        sync.release.assert_called_once_with("task")

    async def test_no_release_when_not_acquired(self):
        apg, sync = _make_async(acquired=False)
        async with apg.acquire("task", ttl=60):
            pass
        sync.release.assert_not_called()

    async def test_raises_on_failure_when_requested(self):
        apg, _ = _make_async(acquired=False)
        with pytest.raises(AcquisitionError):
            async with apg.acquire("task", ttl=60, raise_on_failure=True):
                pass

    async def test_releases_even_on_exception(self):
        apg, sync = _make_async(acquired=True)
        with pytest.raises(ValueError):
            async with apg.acquire("task", ttl=60):
                raise ValueError("body error")
        sync.release.assert_called_once_with("task")


# ---------------------------------------------------------------------------
# wait_for_lease
# ---------------------------------------------------------------------------


class TestAsyncWaitForLease:
    async def test_raises_value_error_for_timeout_zero(self):
        apg, _ = _make_async()
        with pytest.raises(ValueError, match="timeout=0"):
            await apg.wait_for_lease("task", ttl=60, timeout=0)

    async def test_succeeds_immediately(self):
        apg, sync = _make_async(acquired=True)
        result = await apg.wait_for_lease("task", ttl=60, timeout=5, poll_interval=0.01)
        assert result is True

    async def test_retries_until_success(self):
        apg, sync = _make_async()
        sync.try_acquire.side_effect = [False, False, True]
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await apg.wait_for_lease("task", ttl=60, timeout=10, poll_interval=0.01)
        assert result is True
        assert sync.try_acquire.call_count == 3

    async def test_raises_acquisition_error_on_timeout(self):
        apg, sync = _make_async(acquired=False)
        # Real short timeout + real asyncio.sleep; will raise after 2-3 retries
        with pytest.raises(AcquisitionError):
            await apg.wait_for_lease("task", ttl=60, timeout=0.15, poll_interval=0.05)

    async def test_none_timeout_succeeds_on_first_try(self):
        apg, sync = _make_async(acquired=True)
        result = await apg.wait_for_lease("task", ttl=60, timeout=None, poll_interval=0.01)
        assert result is True

    async def test_inf_timeout_succeeds_on_first_try(self):
        apg, sync = _make_async(acquired=True)
        result = await apg.wait_for_lease("task", ttl=60, timeout=float("inf"), poll_interval=0.01)
        assert result is True


# ---------------------------------------------------------------------------
# Async context manager (__aenter__ / __aexit__)
# ---------------------------------------------------------------------------


class TestAsyncContextManager:
    async def test_aenter_returns_self(self):
        apg, _ = _make_async()
        async with apg as p:
            assert p is apg

    async def test_aexit_calls_close(self):
        apg, sync = _make_async()
        async with apg:
            pass
        sync.close.assert_called_once()

    async def test_aexit_calls_close_on_exception(self):
        apg, sync = _make_async()
        with pytest.raises(RuntimeError):
            async with apg:
                raise RuntimeError("crash")
        sync.close.assert_called_once()
