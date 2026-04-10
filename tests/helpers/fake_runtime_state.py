from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
from typing import Any

_ALLOWED_STATES = {"READY", "DEGRADED", "UNKNOWN", "NOT_READY"}


@dataclass(slots=True)
class FakeRuntimeState:
    """Deterministic in-memory runtime-state builder for observability tests."""

    runtime_state_label: str = "READY"

    alerts_enabled: bool = False
    sender_available: bool = False
    deliverable: bool = False
    reason: str | None = None

    inflight_count: int = 0
    ambiguous_count: int = 0
    duplicate_blocked_count: int = 0
    open_positions: int = 0
    recent_failures: int = 0

    db_latency_p95: float = 0.0
    db_locked_errors: int = 0
    async_queue_depth: int = 0

    local_status: str = "OK"
    remote_exists: bool = True
    local_exposure: float = 0.0
    remote_exposure: float = 0.0

    incidents_open: int = 0
    anomalies_active: int = 0
    last_heartbeat_age: float = 0.0

    @classmethod
    def ready(cls, **overrides: Any) -> "FakeRuntimeState":
        base = cls(runtime_state_label="READY", alerts_enabled=True, sender_available=True, deliverable=True, reason=None)
        return base.with_overrides(**overrides)

    @classmethod
    def degraded(cls, *, reason: str = "degraded", **overrides: Any) -> "FakeRuntimeState":
        base = cls(runtime_state_label="DEGRADED", alerts_enabled=True, sender_available=False, deliverable=False, reason=reason)
        return base.with_overrides(**overrides)

    @classmethod
    def unknown(cls, *, reason: str = "unknown", **overrides: Any) -> "FakeRuntimeState":
        base = cls(runtime_state_label="UNKNOWN", alerts_enabled=False, sender_available=False, deliverable=False, reason=reason)
        return base.with_overrides(**overrides)

    def with_overrides(self, **overrides: Any) -> "FakeRuntimeState":
        invalid = sorted(set(overrides) - set(self.__dataclass_fields__))
        if invalid:
            raise KeyError(f"unsupported override fields: {invalid}")
        data = asdict(self)
        data.update(overrides)
        candidate = FakeRuntimeState(**data)
        candidate._validate()
        return candidate

    def to_snapshot(self) -> dict[str, Any]:
        self._validate()
        return deepcopy(asdict(self))

    def alert_pipeline_snapshot(self) -> dict[str, Any]:
        self._validate()
        if self.runtime_state_label == "DEGRADED" and self.reason is None:
            raise ValueError("degraded runtime requires non-empty reason")

        if self.alerts_enabled and self.sender_available and self.deliverable:
            status = "READY"
        elif self.alerts_enabled and not self.sender_available:
            status = "DEGRADED"
        elif self.alerts_enabled and not self.deliverable:
            status = "DEGRADED"
        else:
            status = "DISABLED"

        return {
            "alerts_enabled": self.alerts_enabled,
            "sender_available": self.sender_available,
            "deliverable": self.deliverable,
            "status": status,
            "reason": self.reason,
            "last_delivery_ok": self.deliverable,
            "last_delivery_error": "" if self.deliverable else (self.reason or ""),
        }

    def mark_sender_unavailable(self, reason: str = "sender_unavailable") -> "FakeRuntimeState":
        return self.with_overrides(runtime_state_label="DEGRADED", sender_available=False, deliverable=False, reason=reason)

    def mark_db_contention(self, *, latency_p95: float = 2.0, locked_errors: int = 1) -> "FakeRuntimeState":
        return self.with_overrides(
            runtime_state_label="DEGRADED",
            db_latency_p95=latency_p95,
            db_locked_errors=locked_errors,
            reason="db_contention",
        )

    def mark_ghost_order(self) -> "FakeRuntimeState":
        return self.with_overrides(
            runtime_state_label="DEGRADED",
            local_status="GHOST_ORDER",
            remote_exists=False,
            reason="ghost_order",
        )

    def mark_heartbeat_stale(self, *, age_sec: float = 120.0) -> "FakeRuntimeState":
        return self.with_overrides(runtime_state_label="DEGRADED", last_heartbeat_age=age_sec, reason="heartbeat_stale")

    def mark_exposure_mismatch(self, *, local_exposure: float = 2.0, remote_exposure: float = 0.0) -> "FakeRuntimeState":
        return self.with_overrides(
            runtime_state_label="DEGRADED",
            local_exposure=local_exposure,
            remote_exposure=remote_exposure,
            reason="exposure_mismatch",
        )

    def _validate(self) -> None:
        if self.runtime_state_label not in _ALLOWED_STATES:
            raise ValueError(f"invalid runtime_state_label: {self.runtime_state_label!r}")
        for name in ("inflight_count", "ambiguous_count", "duplicate_blocked_count", "open_positions", "recent_failures", "db_locked_errors", "async_queue_depth", "incidents_open", "anomalies_active"):
            value = getattr(self, name)
            if value < 0:
                raise ValueError(f"{name} must be >= 0")
