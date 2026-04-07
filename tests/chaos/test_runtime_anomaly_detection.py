from observability.alerts_manager import AlertsManager
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService


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
