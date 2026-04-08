from observability.alerts_manager import AlertsManager
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService
from tests.helpers.fake_runtime_state import FakeRuntimeState


class _ProbeStub:
    def collect_health(self):
        return {"runtime": {"status": "READY", "reason": "ok", "details": {}}}

    def collect_metrics(self):
        return {}


class _SnapshotStub:
    def collect_and_store(self):
        return None


def _make_watchdog(*, anomaly_enabled: bool) -> WatchdogService:
    return WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_enabled=anomaly_enabled,
        interval_sec=60.0,
    )


def test_anomaly_flag_defaults_to_off():
    watchdog = _make_watchdog(anomaly_enabled=False)
    assert watchdog.anomaly_enabled is False


def test_anomaly_hook_runs_anomaly_invariant_and_correlation_when_enabled(monkeypatch):
    watchdog = _make_watchdog(anomaly_enabled=True)
    calls = []

    monkeypatch.setattr(watchdog, "_evaluate_anomalies", lambda: calls.append("anomaly"))
    monkeypatch.setattr(watchdog, "_evaluate_invariants", lambda: calls.append("invariant"))
    monkeypatch.setattr(watchdog, "_evaluate_correlations", lambda: calls.append("correlation"))

    watchdog._tick()

    assert calls == ["anomaly", "invariant", "correlation"]


def test_anomaly_hook_is_skipped_when_flag_disabled(monkeypatch):
    watchdog = _make_watchdog(anomaly_enabled=False)

    def _unexpected_call() -> None:
        raise AssertionError("anomaly hook must stay disabled by default")

    monkeypatch.setattr(watchdog, "_run_anomaly_hook", _unexpected_call)

    watchdog._tick()


def test_runtime_contradictions_are_expressible_deterministically():
    contradiction = (
        FakeRuntimeState.ready()
        .mark_ghost_order()
        .mark_exposure_mismatch(local_exposure=3.5, remote_exposure=1.0)
        .mark_db_contention(latency_p95=3.0, locked_errors=4)
        .mark_heartbeat_stale(age_sec=180.0)
    )

    snapshot = contradiction.to_snapshot()

    assert snapshot["runtime_state_label"] == "DEGRADED"
    assert snapshot["reason"] == "heartbeat_stale"
    assert snapshot["db_locked_errors"] == 4
    assert snapshot["local_exposure"] == 3.5
    assert snapshot["remote_exposure"] == 1.0
    assert snapshot["last_heartbeat_age"] == 180.0


def test_fake_runtime_state_rejects_invalid_override_field():
    state = FakeRuntimeState.ready()

    try:
        state.with_overrides(not_a_field=True)
    except KeyError as exc:
        assert "unsupported override fields" in str(exc)
    else:
        raise AssertionError("expected KeyError for unsupported runtime-state override")
