"""Unit tests for pglease.models — no DB required."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from pglease.models import AcquisitionResult, Lease

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(UTC)


def _lease(
    *,
    task_name: str = "t",
    owner_id: str = "w",
    ttl: int = 60,
    heartbeat_age: float = 0.0,
    now: datetime | None = None,
) -> Lease:
    if now is None:
        now = _now()
    return Lease(
        task_name=task_name,
        owner_id=owner_id,
        acquired_at=now,
        expires_at=now + timedelta(seconds=ttl),
        heartbeat_at=now - timedelta(seconds=heartbeat_age),
    )


# ---------------------------------------------------------------------------
# Lease — construction & __post_init__ normalisation
# ---------------------------------------------------------------------------


class TestLeaseConstruction:
    def test_basic_roundtrip(self):
        now = _now()
        lease = Lease(
            task_name="my-task",
            owner_id="worker-1",
            acquired_at=now,
            expires_at=now + timedelta(seconds=30),
            heartbeat_at=now,
        )
        assert lease.task_name == "my-task"
        assert lease.owner_id == "worker-1"

    def test_naive_acquired_at_normalised_to_utc(self):
        naive = datetime(2024, 1, 1, 12, 0, 0)  # no tzinfo
        lease = Lease(
            task_name="t",
            owner_id="w",
            acquired_at=naive,
            expires_at=naive + timedelta(seconds=60),
            heartbeat_at=naive,
        )
        assert lease.acquired_at.tzinfo is not None
        assert lease.acquired_at.tzinfo == UTC

    def test_naive_expires_at_normalised(self):
        naive = datetime(2024, 6, 1, 8, 0, 0)
        lease = Lease(
            task_name="t",
            owner_id="w",
            acquired_at=naive,
            expires_at=naive + timedelta(seconds=60),
            heartbeat_at=naive,
        )
        assert lease.expires_at.tzinfo == UTC

    def test_naive_heartbeat_at_normalised(self):
        naive = datetime(2024, 6, 1, 8, 0, 0)
        lease = Lease(
            task_name="t",
            owner_id="w",
            acquired_at=naive,
            expires_at=naive + timedelta(seconds=60),
            heartbeat_at=naive,
        )
        assert lease.heartbeat_at.tzinfo == UTC

    def test_aware_datetimes_preserved(self):
        now = _now()
        lease = _lease(now=now)
        # should not raise, tzinfo preserved
        assert lease.acquired_at == now

    def test_lease_is_frozen(self):
        lease = _lease()
        with pytest.raises(Exception):  # dataclass frozen raises FrozenInstanceError
            lease.task_name = "other"  # type: ignore


# ---------------------------------------------------------------------------
# Lease.is_expired
# ---------------------------------------------------------------------------


class TestLeaseIsExpired:
    def test_not_expired_future(self):
        now = _now()
        lease = _lease(ttl=60, now=now)
        assert not lease.is_expired(now)

    def test_expired_past(self):
        now = _now()
        lease = Lease(
            task_name="t",
            owner_id="w",
            acquired_at=now - timedelta(seconds=120),
            expires_at=now - timedelta(seconds=1),
            heartbeat_at=now - timedelta(seconds=120),
        )
        assert lease.is_expired(now)

    def test_expires_exactly_now_is_expired(self):
        now = _now()
        lease = Lease(
            task_name="t",
            owner_id="w",
            acquired_at=now - timedelta(seconds=60),
            expires_at=now,
            heartbeat_at=now - timedelta(seconds=60),
        )
        # expires_at <= now → expired
        assert lease.is_expired(now)

    def test_uses_utcnow_when_no_reference(self):
        lease = _lease(ttl=3600)  # expires far in the future
        assert not lease.is_expired()


# ---------------------------------------------------------------------------
# Lease.time_remaining
# ---------------------------------------------------------------------------


class TestLeaseTimeRemaining:
    def test_positive_remaining(self):
        now = _now()
        lease = _lease(ttl=60, now=now)
        remaining = lease.time_remaining(now)
        assert 59.0 <= remaining <= 60.0

    def test_zero_when_expired(self):
        now = _now()
        lease = Lease(
            task_name="t",
            owner_id="w",
            acquired_at=now - timedelta(seconds=90),
            expires_at=now - timedelta(seconds=10),
            heartbeat_at=now - timedelta(seconds=90),
        )
        assert lease.time_remaining(now) == 0.0

    def test_uses_utcnow_when_no_reference(self):
        lease = _lease(ttl=3600)
        assert lease.time_remaining() > 0.0

    def test_large_ttl(self):
        now = _now()
        lease = _lease(ttl=86400, now=now)
        assert lease.time_remaining(now) > 86390


# ---------------------------------------------------------------------------
# Lease.is_zombie
# ---------------------------------------------------------------------------


class TestLeaseIsZombie:
    def test_not_zombie_fresh_heartbeat(self):
        now = _now()
        lease = _lease(ttl=60, heartbeat_age=5, now=now)
        assert not lease.is_zombie(heartbeat_timeout=30, now=now)

    def test_zombie_stale_heartbeat_active_lease(self):
        now = _now()
        # lease still active (expires in future), but heartbeat is 90s old
        lease = _lease(ttl=120, heartbeat_age=90, now=now)
        assert lease.is_zombie(heartbeat_timeout=30, now=now)

    def test_not_zombie_if_already_expired(self):
        # An expired lease is not a zombie — it's just expired
        now = _now()
        lease = Lease(
            task_name="t",
            owner_id="w",
            acquired_at=now - timedelta(seconds=120),
            expires_at=now - timedelta(seconds=10),  # already past
            heartbeat_at=now - timedelta(seconds=120),
        )
        assert not lease.is_zombie(heartbeat_timeout=30, now=now)

    def test_zombie_uses_utcnow_when_no_reference(self):
        lease = _lease(ttl=3600, heartbeat_age=3600)  # heartbeat 1h ago
        assert lease.is_zombie(heartbeat_timeout=60)

    def test_not_zombie_exactly_at_timeout(self):
        now = _now()
        # heartbeat_age == heartbeat_timeout → boundary: should be zombie (>)
        lease = _lease(ttl=120, heartbeat_age=30, now=now)
        # 30s old heartbeat, timeout=30 → not zombie (only past-timeout counts)
        # depends on implementation: > vs >=
        # Implementation: (now - heartbeat_at).total_seconds() > heartbeat_timeout
        result = lease.is_zombie(heartbeat_timeout=30, now=now)
        # 30s elapsed is NOT > 30s timeout → not zombie
        assert not result

    def test_zombie_just_past_timeout(self):
        now = _now()
        lease = _lease(ttl=120, heartbeat_age=31, now=now)
        assert lease.is_zombie(heartbeat_timeout=30, now=now)

    def test_default_timeout_is_60s(self):
        now = _now()
        lease = _lease(ttl=3600, heartbeat_age=61, now=now)
        assert lease.is_zombie(now=now)  # default heartbeat_timeout=60

        lease2 = _lease(ttl=3600, heartbeat_age=59, now=now)
        assert not lease2.is_zombie(now=now)


# ---------------------------------------------------------------------------
# AcquisitionResult
# ---------------------------------------------------------------------------


class TestAcquisitionResult:
    def test_acquired(self):
        lease = _lease()
        result = AcquisitionResult.acquired(lease)
        assert result.success is True
        assert result.lease is lease
        assert result.reason is None

    def test_failed(self):
        result = AcquisitionResult.failed("already held")
        assert result.success is False
        assert result.lease is None
        assert result.reason is not None
        assert "held" in result.reason

    def test_failed_with_reason(self):
        result = AcquisitionResult.failed("Lease expires in 30.0s")
        assert "expires" in result.reason

    def test_bool_truthy_on_success(self):
        lease = _lease()
        result = AcquisitionResult.acquired(lease)
        assert bool(result)

    def test_bool_falsy_on_failure(self):
        result = AcquisitionResult.failed("blocked")
        assert not bool(result)
