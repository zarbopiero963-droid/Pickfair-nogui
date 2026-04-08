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

    def collect_runtime_state(self):
        return {}


class _SnapshotStub:
    def collect_and_store(self):
        return None


class _AnomalyEngineStub:
    def __init__(self, anomalies):
        self._anomalies = anomalies

    def evaluate(self, _context):
        return list(self._anomalies)


def _make_watchdog(
    *,
    anomaly_enabled: bool,
    anomaly_alerts_enabled: bool = False,
    anomaly_actions_enabled: bool = False,
    anomalies=None,
) -> WatchdogService:
    return WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_engine=_AnomalyEngineStub(anomalies or []),
        anomaly_enabled=anomaly_enabled,
        anomaly_alerts_enabled=anomaly_alerts_enabled,
        anomaly_actions_enabled=anomaly_actions_enabled,
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


def test_detection_only_runs_without_alert_or_escalation():
    watchdog = _make_watchdog(
        anomaly_enabled=True,
        anomaly_alerts_enabled=False,
        anomaly_actions_enabled=False,
        anomalies=[{"code": "ANOM_1", "severity": "warning", "description": "detect only"}],
    )

    watchdog._tick()

    alerts = watchdog.alerts_manager.snapshot()["alerts"]
    assert [a for a in alerts if a.get("source") == "anomaly"] == []
    assert watchdog.escalation_requested is False
    assert watchdog.last_escalation_event is None


def test_detection_with_alerts_and_actions_invokes_safe_escalation_hook():
    events = []
    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_engine=_AnomalyEngineStub(
            [{"code": "ANOM_2", "severity": "critical", "description": "escalate", "details": {"component": "watchdog"}}]
        ),
        anomaly_enabled=True,
        anomaly_alerts_enabled=True,
        anomaly_actions_enabled=True,
        anomaly_escalation_hook=lambda payload: events.append(payload),
        interval_sec=60.0,
    )

    watchdog._tick()

    anomaly_alerts = [a for a in watchdog.alerts_manager.snapshot()["alerts"] if a.get("source") == "anomaly"]
    assert len(anomaly_alerts) == 1
    assert watchdog.escalation_requested is True
    assert watchdog.last_escalation_event is not None
    assert watchdog.last_escalation_event["escalation_requested"] is True
    assert events and events[0]["code"] == "ANOM_2"
