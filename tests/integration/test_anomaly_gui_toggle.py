from services.settings_service import SettingsService
from observability.alerts_manager import AlertsManager
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService


class _InMemorySettingsDb:
    def __init__(self):
        self.data = {}

    def get_settings(self):
        return dict(self.data)

    def save_settings(self, payload):
        self.data.update({str(k): str(v) for k, v in (payload or {}).items()})


class _ProbeStub:
    def __init__(self, runtime_state=None):
        self.runtime_state = runtime_state or {"trading_state": {"mode": "RUNNING", "orders": 2}}

    def collect_health(self):
        return {"runtime": {"status": "READY", "reason": "ok", "details": {}}}

    def collect_metrics(self):
        return {}

    def collect_runtime_state(self):
        return self.runtime_state


class _SnapshotStub:
    def collect_and_store(self):
        return None


class _EngineStub:
    def __init__(self, anomalies=None):
        self.anomalies = list(anomalies or [{"code": "GUI", "severity": "warning", "description": "gui-path"}])
        self.calls = 0

    def evaluate(self, _context):
        self.calls += 1
        return list(self.anomalies)


def _watchdog(settings_service=None, *, anomaly_enabled=False, anomaly_alerts_enabled=False, anomaly_actions_enabled=False, hook=None):
    return WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        settings_service=settings_service,
        anomaly_engine=_EngineStub(),
        anomaly_enabled=anomaly_enabled,
        anomaly_alerts_enabled=anomaly_alerts_enabled,
        anomaly_actions_enabled=anomaly_actions_enabled,
        anomaly_escalation_hook=hook,
        interval_sec=60.0,
    )


def test_missing_setting_defaults_safe_off_and_headless_safe_without_settings():
    svc = SettingsService(_InMemorySettingsDb())
    assert svc.load_anomaly_enabled() is False
    assert svc.load_anomaly_alerts_enabled() is False
    assert svc.load_anomaly_actions_enabled() is False

    watchdog = _watchdog(settings_service=None, anomaly_enabled=False)
    watchdog._tick()
    assert watchdog.last_anomalies == []


def test_toggle_off_keeps_hook_disabled():
    svc = SettingsService(_InMemorySettingsDb())
    svc.save_anomaly_enabled(True)
    svc.save_anomaly_alerts_enabled(True)
    svc.save_anomaly_actions_enabled(False)

    hook_calls = []
    watchdog = _watchdog(settings_service=svc, hook=lambda payload: hook_calls.append(payload))

    watchdog._tick()

    assert watchdog.last_anomalies
    assert watchdog.escalation_requested is False
    assert hook_calls == []


def test_toggle_on_enables_safe_hook_path():
    svc = SettingsService(_InMemorySettingsDb())
    svc.save_anomaly_enabled(True)
    svc.save_anomaly_alerts_enabled(True)
    svc.save_anomaly_actions_enabled(True)

    hook_calls = []
    watchdog = _watchdog(settings_service=svc, hook=lambda payload: hook_calls.append(payload))

    watchdog._tick()

    assert watchdog.escalation_requested is True
    assert watchdog.last_escalation_event is not None
    assert hook_calls and hook_calls[0]["escalation_requested"] is True


def test_toggle_persistence_across_service_instances():
    db = _InMemorySettingsDb()
    first = SettingsService(db)
    first.save_anomaly_enabled(True)
    first.save_anomaly_alerts_enabled(False)
    first.save_anomaly_actions_enabled(True)

    second = SettingsService(db)
    assert second.load_anomaly_enabled() is True
    assert second.load_anomaly_alerts_enabled() is False
    assert second.load_anomaly_actions_enabled() is True
