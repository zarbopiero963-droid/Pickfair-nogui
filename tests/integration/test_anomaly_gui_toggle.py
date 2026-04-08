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


def _watchdog(settings_service, probe=None):
    return WatchdogService(
        probe=probe or _ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        settings_service=settings_service,
        anomaly_enabled=False,
        interval_sec=60.0,
    )


def test_missing_setting_defaults_to_off_and_skips_anomaly_hook(monkeypatch):
    svc = SettingsService(_InMemorySettingsDb())
    watchdog = _watchdog(svc)
    assert svc.load_anomaly_enabled() is False

    monkeypatch.setattr(watchdog, "_run_anomaly_hook", lambda: (_ for _ in ()).throw(AssertionError()))
    watchdog._tick()


def test_toggle_off_keeps_hook_disabled(monkeypatch):
    svc = SettingsService(_InMemorySettingsDb())
    svc.save_anomaly_enabled(False)
    watchdog = _watchdog(svc)
    calls = []
    monkeypatch.setattr(watchdog, "_run_anomaly_hook", lambda: calls.append("hook"))
    watchdog._tick()
    assert calls == []


def test_toggle_on_runs_hook_without_mutating_runtime_state(monkeypatch):
    svc = SettingsService(_InMemorySettingsDb())
    svc.save_anomaly_enabled(True)
    probe = _ProbeStub()
    original_state = {"trading_state": dict(probe.runtime_state["trading_state"])}
    watchdog = _watchdog(svc, probe=probe)
    calls = []
    monkeypatch.setattr(watchdog, "_run_anomaly_hook", lambda: calls.append("hook"))

    watchdog._tick()

    assert calls == ["hook"]
    assert probe.runtime_state == original_state


def test_toggle_persistence_across_service_instances():
    db = _InMemorySettingsDb()
    first = SettingsService(db)
    first.save_anomaly_enabled(True)
    second = SettingsService(db)
    assert second.load_anomaly_enabled() is True

    second.save_anomaly_enabled(False)
    third = SettingsService(db)
    assert third.load_anomaly_enabled() is False


def test_headless_safety_without_settings_service(monkeypatch):
    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_enabled=False,
        interval_sec=60.0,
    )
    calls = []
    monkeypatch.setattr(watchdog, "_run_anomaly_hook", lambda: calls.append("hook"))

    watchdog._tick()
    assert calls == []
