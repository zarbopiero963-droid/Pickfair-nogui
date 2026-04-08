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
    def __init__(self):
        self.runtime_state = {"trading_state": {"mode": "RUNNING", "orders": 2}}

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
    def __init__(self, anomalies):
        self.anomalies = list(anomalies)
        self.calls = 0

    def evaluate(self, _context):
        self.calls += 1
        return list(self.anomalies)


class _AlertSpy:
    def __init__(self):
        self.calls = []

    def notify_alert(self, payload):
        self.calls.append(payload)


def _make_watchdog(*, settings, anomaly_engine, alert_service=None, hook=None, probe=None):
    return WatchdogService(
        probe=probe or _ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        settings_service=settings,
        anomaly_engine=anomaly_engine,
        anomaly_alert_service=alert_service,
        anomaly_escalation_hook=hook,
        interval_sec=60.0,
    )


def test_escalation_level_0_all_toggles_off():
    db = _InMemorySettingsDb()
    settings = SettingsService(db)
    engine = _EngineStub([{"code": "A1", "severity": "warning", "description": "d"}])
    alerts = _AlertSpy()
    hook_calls = []
    watchdog = _make_watchdog(settings=settings, anomaly_engine=engine, alert_service=alerts, hook=lambda p: hook_calls.append(p))

    watchdog._tick()

    assert engine.calls == 0
    assert alerts.calls == []
    assert hook_calls == []
    assert watchdog.escalation_requested is False


def test_escalation_level_1_detection_only():
    db = _InMemorySettingsDb()
    settings = SettingsService(db)
    settings.save_anomaly_enabled(True)

    engine = _EngineStub([{"code": "A1", "severity": "warning", "description": "d"}])
    alerts = _AlertSpy()
    hook_calls = []
    watchdog = _make_watchdog(settings=settings, anomaly_engine=engine, alert_service=alerts, hook=lambda p: hook_calls.append(p))

    watchdog._tick()

    assert engine.calls == 1
    assert watchdog.last_anomalies and watchdog.last_anomalies[0]["code"] == "A1"
    assert alerts.calls == []
    assert hook_calls == []
    assert watchdog.escalation_requested is False


def test_escalation_level_2_detection_plus_alerts():
    db = _InMemorySettingsDb()
    settings = SettingsService(db)
    settings.save_anomaly_enabled(True)
    settings.save_anomaly_alerts_enabled(True)

    engine = _EngineStub([{"code": "A2", "severity": "error", "description": "warn"}])
    alerts = _AlertSpy()
    hook_calls = []
    watchdog = _make_watchdog(settings=settings, anomaly_engine=engine, alert_service=alerts, hook=lambda p: hook_calls.append(p))

    watchdog._tick()

    assert engine.calls == 1
    assert len(alerts.calls) == 1
    assert alerts.calls[0]["code"] == "A2"
    assert hook_calls == []
    assert watchdog.escalation_requested is False


def test_escalation_level_3_detection_alerts_actions_non_destructive():
    db = _InMemorySettingsDb()
    settings = SettingsService(db)
    settings.save_anomaly_enabled(True)
    settings.save_anomaly_alerts_enabled(True)
    settings.save_anomaly_actions_enabled(True)

    probe = _ProbeStub()
    trading_state_before = dict(probe.runtime_state["trading_state"])

    engine = _EngineStub([{"code": "A3", "severity": "critical", "description": "panic", "details": {"k": "v"}}])
    alerts = _AlertSpy()
    hook_calls = []
    watchdog = _make_watchdog(
        settings=settings,
        anomaly_engine=engine,
        alert_service=alerts,
        hook=lambda p: hook_calls.append(p),
        probe=probe,
    )

    watchdog._tick()

    assert len(alerts.calls) == 1
    assert watchdog.escalation_requested is True
    assert watchdog.last_escalation_event is not None
    assert watchdog.last_escalation_event["code"] == "A3"
    assert hook_calls and hook_calls[0]["escalation_requested"] is True
    assert probe.runtime_state["trading_state"] == trading_state_before


def test_persisted_toggle_reload_and_missing_fallback_false():
    db = _InMemorySettingsDb()
    first = SettingsService(db)
    first.save_anomaly_enabled(True)
    first.save_anomaly_alerts_enabled(True)
    first.save_anomaly_actions_enabled(True)

    reloaded = SettingsService(db)
    assert reloaded.load_anomaly_enabled() is True
    assert reloaded.load_anomaly_alerts_enabled() is True
    assert reloaded.load_anomaly_actions_enabled() is True

    fresh = SettingsService(_InMemorySettingsDb())
    assert fresh.load_anomaly_enabled() is False
    assert fresh.load_anomaly_alerts_enabled() is False
    assert fresh.load_anomaly_actions_enabled() is False
