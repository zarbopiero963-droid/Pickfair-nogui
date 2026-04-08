from observability.alerts_manager import AlertsManager
from observability.anomaly_engine import AnomalyEngine
from observability.anomaly_rules import (
    db_contention_detected,
    exposure_mismatch,
    ghost_order_detected,
)
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService
from tests.helpers.fake_runtime_state import FakeRuntimeProbeSource, FakeRuntimeState


class _SnapshotStub:
    def collect_and_store(self):
        return None


def _make_watchdog(*, anomaly_enabled: bool, probe=None, anomaly_context_provider=None, anomaly_engine=None) -> WatchdogService:
    return WatchdogService(
        probe=probe or FakeRuntimeProbeSource(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_context_provider=anomaly_context_provider,
        anomaly_engine=anomaly_engine,
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


def test_runtime_anomaly_state_detects_ghost_order():
    fake_state = FakeRuntimeState.ready().mark_ghost_order(ghost_orders_count=2)
    watchdog = _make_watchdog(
        anomaly_enabled=True,
        probe=FakeRuntimeProbeSource(fake_state),
        anomaly_engine=AnomalyEngine([ghost_order_detected]),
    )

    watchdog._tick()

    alerts = watchdog.alerts_manager.snapshot()["alerts"]
    assert any(item["code"] == "GHOST_ORDER_DETECTED" for item in alerts)


def test_runtime_anomaly_state_detects_exposure_mismatch():
    fake_state = FakeRuntimeState.ready().mark_exposure_mismatch(local_exposure=10.0, remote_exposure=8.5)
    watchdog = _make_watchdog(
        anomaly_enabled=True,
        probe=FakeRuntimeProbeSource(fake_state),
        anomaly_engine=AnomalyEngine([exposure_mismatch]),
        anomaly_context_provider=lambda: {"risk": fake_state.to_snapshot()["risk"]},
    )

    watchdog._tick()

    alerts = watchdog.alerts_manager.snapshot()["alerts"]
    assert any(item["code"] == "EXPOSURE_MISMATCH" for item in alerts)


def test_runtime_anomaly_state_detects_db_contention_and_stale_heartbeat():
    fake_state = FakeRuntimeState.ready().mark_db_contention(latency_p95=325.0, locked_errors=2).mark_heartbeat_stale(
        age_seconds=480.0
    )

    watchdog = _make_watchdog(
        anomaly_enabled=True,
        probe=FakeRuntimeProbeSource(fake_state),
        anomaly_engine=AnomalyEngine([db_contention_detected]),
        anomaly_context_provider=lambda: {"db": fake_state.to_snapshot()["db"]},
    )

    watchdog._tick()

    alerts = watchdog.alerts_manager.snapshot()["alerts"]
    assert any(item["code"] == "DB_CONTENTION_DETECTED" for item in alerts)
    assert fake_state.runtime_state_label == "UNKNOWN"
    assert fake_state.last_heartbeat_age == 480.0
