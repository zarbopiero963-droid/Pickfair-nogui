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
    def evaluate(self, _context):
        return [
            {
                "code": "RUNTIME_SPIKE",
                "severity": "warning",
                "description": "latency spike",
                "details": {"component": "runtime_probe"},
            }
        ]


def _run_watchdog(anomaly_enabled, anomaly_alerts_enabled, anomaly_actions_enabled):
    alerts = AlertsManager()
    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_engine=_AnomalyEngineStub(),
        anomaly_enabled=anomaly_enabled,
        anomaly_alerts_enabled=anomaly_alerts_enabled,
        anomaly_actions_enabled=anomaly_actions_enabled,
        interval_sec=60.0,
    )
    watchdog._tick()
    return watchdog, alerts


def test_all_toggles_off_disables_detection_alerts_and_escalation():
    watchdog, alerts = _run_watchdog(False, False, False)
    assert [a for a in alerts.snapshot()["alerts"] if a.get("source") == "anomaly"] == []
    assert watchdog.escalation_requested is False
    assert watchdog.last_escalation_event is None


def test_detection_only_mode_runs_without_alert_or_escalation():
    watchdog, alerts = _run_watchdog(True, False, False)
    assert [a for a in alerts.snapshot()["alerts"] if a.get("source") == "anomaly"] == []
    assert watchdog.escalation_requested is False
    assert watchdog.last_escalation_event is None


def test_detection_and_alerts_mode_emits_alert_without_escalation():
    watchdog, alerts = _run_watchdog(True, True, False)
    anomaly_alerts = [a for a in alerts.snapshot()["alerts"] if a.get("source") == "anomaly"]
    assert len(anomaly_alerts) == 1
    assert anomaly_alerts[0]["code"] == "RUNTIME_SPIKE"
    assert watchdog.escalation_requested is False
    assert watchdog.last_escalation_event is None


def test_detection_alerts_and_actions_mode_requests_safe_escalation_only():
    watchdog, alerts = _run_watchdog(True, True, True)
    anomaly_alerts = [a for a in alerts.snapshot()["alerts"] if a.get("source") == "anomaly"]
    assert len(anomaly_alerts) == 1
    assert watchdog.escalation_requested is True
    payload = watchdog.last_escalation_event
    assert payload is not None
    assert payload["code"] == "RUNTIME_SPIKE"
    assert payload["severity"] == "warning"
    assert payload["source"] == "anomaly"
    assert payload["escalation_requested"] is True
from tests.helpers.fake_settings import FakeSettingsService


TOGGLES = (
    "anomaly_enabled",
    "anomaly_alerts_enabled",
    "anomaly_actions_enabled",
)


def test_all_anomaly_toggles_persist_across_reload():
    settings = FakeSettingsService()

    for key in TOGGLES:
        settings.set_bool(key, True)

    reloaded = FakeSettingsService.from_state(settings.export_state())

    for key in TOGGLES:
        assert reloaded.get_bool(key, default=False) is True


def test_missing_anomaly_toggles_fall_back_false_deterministically():
    settings = FakeSettingsService()

    for key in TOGGLES:
        assert settings.get_bool(key, default=False) is False
