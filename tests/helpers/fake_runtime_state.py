from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

_VALID_HEALTH_STATES = {"READY", "DEGRADED", "UNKNOWN", "NOT_READY"}


@dataclass(slots=True)
class FakeRuntimeState:
    runtime_state_label: str = "READY"
    alerts_enabled: bool = True
    sender_available: bool = True
    deliverable: bool = True
    reason: str | None = None
    inflight_count: float = 0.0
    ambiguous_count: int = 0
    duplicate_blocked_count: int = 0
    open_positions: int = 0
    recent_failures: int = 0
    db_latency_p95: float = 12.0
    db_locked_errors: int = 0
    async_queue_depth: int = 0
    local_status: str = "SYNCED"
    remote_exists: bool = True
    local_exposure: float = 0.0
    remote_exposure: float = 0.0
    incidents_open: int = 0
    anomalies_active: int = 0
    last_heartbeat_age: float = 1.0
    ghost_orders_count: int = 0

    @classmethod
    def ready(cls, **overrides: Any) -> "FakeRuntimeState":
        state = cls(runtime_state_label="READY")
        return state.with_overrides(**overrides)

    @classmethod
    def degraded(cls, *, reason: str = "degraded", **overrides: Any) -> "FakeRuntimeState":
        state = cls(
            runtime_state_label="DEGRADED",
            deliverable=False,
            reason=reason,
            recent_failures=1,
            anomalies_active=1,
        )
        return state.with_overrides(**overrides)

    @classmethod
    def unknown(cls, *, reason: str = "unknown", **overrides: Any) -> "FakeRuntimeState":
        state = cls(
            runtime_state_label="UNKNOWN",
            deliverable=False,
            reason=reason,
            sender_available=False,
        )
        return state.with_overrides(**overrides)

    def with_overrides(self, **overrides: Any) -> "FakeRuntimeState":
        unknown = sorted(set(overrides) - set(self.__dataclass_fields__))
        if unknown:
            raise ValueError(f"Unsupported FakeRuntimeState override(s): {', '.join(unknown)}")
        return replace(self, **overrides)

    def mark_sender_unavailable(self) -> "FakeRuntimeState":
        self.sender_available = False
        self.deliverable = False
        self.reason = "sender_unavailable"
        if self.runtime_state_label == "READY":
            self.runtime_state_label = "DEGRADED"
        return self

    def mark_db_contention(self, *, latency_p95: float = 500.0, locked_errors: int = 3) -> "FakeRuntimeState":
        self.db_latency_p95 = latency_p95
        self.db_locked_errors = locked_errors
        if self.runtime_state_label == "READY":
            self.runtime_state_label = "DEGRADED"
        return self

    def mark_ghost_order(self, *, ghost_orders_count: int = 1) -> "FakeRuntimeState":
        self.ghost_orders_count = ghost_orders_count
        if self.runtime_state_label == "READY":
            self.runtime_state_label = "DEGRADED"
        return self

    def mark_heartbeat_stale(self, *, age_seconds: float = 300.0) -> "FakeRuntimeState":
        self.last_heartbeat_age = age_seconds
        self.runtime_state_label = "UNKNOWN"
        return self

    def mark_exposure_mismatch(self, *, local_exposure: float, remote_exposure: float) -> "FakeRuntimeState":
        self.local_exposure = local_exposure
        self.remote_exposure = remote_exposure
        if self.runtime_state_label == "READY":
            self.runtime_state_label = "DEGRADED"
        return self

    def to_snapshot(self) -> dict[str, Any]:
        if self.runtime_state_label not in _VALID_HEALTH_STATES:
            raise ValueError(f"Invalid runtime_state_label: {self.runtime_state_label}")

        return {
            "runtime_state": self.runtime_state_label,
            "health": {"state": self.runtime_state_label},
            "alert_pipeline": {
                "alerts_enabled": self.alerts_enabled,
                "sender_available": self.sender_available,
                "deliverable": self.deliverable,
                "status": self.runtime_state_label if self.alerts_enabled else "DISABLED",
                "reason": self.reason,
            },
            "orders": {
                "inflight_count": self.inflight_count,
                "ambiguous_count": self.ambiguous_count,
                "duplicate_blocked_count": self.duplicate_blocked_count,
                "open_positions": self.open_positions,
                "recent_failures": self.recent_failures,
            },
            "db_io": {
                "db_latency_p95": self.db_latency_p95,
                "db_locked_errors": self.db_locked_errors,
                "async_queue_depth": self.async_queue_depth,
            },
            "consistency": {
                "local_status": self.local_status,
                "remote_exists": self.remote_exists,
                "local_exposure": self.local_exposure,
                "remote_exposure": self.remote_exposure,
            },
            "incidents_open": self.incidents_open,
            "anomalies_active": self.anomalies_active,
            "last_heartbeat_age": self.last_heartbeat_age,
            "reconcile": {"ghost_orders_count": self.ghost_orders_count},
            "risk": {
                "expected_exposure": self.local_exposure,
                "actual_exposure": self.remote_exposure,
                "exposure_tolerance": 0.01,
            },
            "db": {
                "lock_wait_ms": self.db_latency_p95,
                "contention_events": self.db_locked_errors,
                "lock_wait_threshold_ms": 200.0,
            },
        }


class FakeRuntimeProbeSource:
    def __init__(self, state: FakeRuntimeState | None = None) -> None:
        self.state = state or FakeRuntimeState.ready()

    def collect_runtime_state(self) -> dict[str, Any]:
        return self.state.to_snapshot()

    def collect_health(self) -> dict[str, dict[str, Any]]:
        status = self.state.runtime_state_label
        return {"runtime": {"status": status, "reason": self.state.reason, "details": {}}}

    def collect_metrics(self) -> dict[str, float]:
        return {
            "inflight_count": float(self.state.inflight_count),
            "memory_rss_mb": float(100.0 + self.state.db_latency_p95),
        }

    def collect_forensics_evidence(self) -> dict[str, Any]:
        return {}
