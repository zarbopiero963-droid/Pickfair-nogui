from observability.alerts_manager import AlertsManager
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService
from tests.helpers.fake_runtime_state import FakeRuntimeState


class _ProbeStub:
    def __init__(self, runtime_state=None):
        self.runtime_state = runtime_state if runtime_state is not None else {}

    def collect_health(self):
        return {"runtime": {"status": "READY", "reason": "ok", "details": {}}}

    def collect_metrics(self):
        return {}

    def collect_runtime_state(self):
        return self.runtime_state


class _SnapshotStub:
    def collect_and_store(self):
        return None


class _SettingsStub:
    def __init__(self, enabled=False, alerts=False, actions=False):
        self.enabled = enabled
        self.alerts = alerts
        self.actions = actions

    def load_anomaly_enabled(self):
        return self.enabled

    def load_anomaly_alerts_enabled(self):
        return self.alerts

    def load_anomaly_actions_enabled(self):
        return self.actions


class _EngineStub:
    def __init__(self, anomalies=None, raises=False):
        self.anomalies = list(anomalies or [])
        self.raises = raises
        self.calls = 0

    def evaluate(self, _context):
        self.calls += 1
        if self.raises:
            raise RuntimeError("boom")
        return list(self.anomalies)


class _AnomalyAlertServiceSpy:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls = []

    def notify_alert(self, payload):
        self.calls.append(payload)
        if self.fail:
            raise RuntimeError("send failed")
        return {"delivered": True}


def _make_watchdog(
    *,
    anomaly_enabled: bool,
    anomaly_alerts_enabled: bool = False,
    anomaly_actions_enabled: bool = False,
    anomaly_alert_service=None,
    anomaly_engine=None,
    anomalies=None,
    runtime_state=None,
    anomaly_escalation_hook=None,
) -> WatchdogService:
    engine = anomaly_engine or _EngineStub(anomalies=anomalies)
    return WatchdogService(
        probe=_ProbeStub(runtime_state=runtime_state),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_engine=engine,
        anomaly_enabled=anomaly_enabled,
        anomaly_alerts_enabled=anomaly_alerts_enabled,
        anomaly_actions_enabled=anomaly_actions_enabled,
        anomaly_alert_service=anomaly_alert_service,
        anomaly_escalation_hook=anomaly_escalation_hook,
        interval_sec=60.0,
    )


def test_anomaly_disabled_no_effect():
    engine = _EngineStub(anomalies=[{"code": "IGNORED"}])
    watchdog = _make_watchdog(anomaly_enabled=False, anomaly_engine=engine)

    watchdog._tick()

    assert engine.calls == 0
    assert watchdog.last_anomalies == []
    assert watchdog.escalation_requested is False


def test_detection_only_runs_without_alert_or_escalation():
    engine = _EngineStub(anomalies=[{"code": "GHOST_ORDER", "severity": "warning", "description": "ghost"}])
    alert_service = _AnomalyAlertServiceSpy()
    hook_calls = []
    watchdog = _make_watchdog(
        anomaly_enabled=True,
        anomaly_alerts_enabled=False,
        anomaly_actions_enabled=False,
        anomaly_alert_service=alert_service,
        anomaly_engine=engine,
        anomaly_escalation_hook=lambda payload: hook_calls.append(payload),
    )

    watchdog._tick()

    assert engine.calls == 1
    assert watchdog.last_anomalies and watchdog.last_anomalies[0]["code"] == "GHOST_ORDER"
    assert alert_service.calls == []
    assert watchdog.escalation_requested is False
    assert hook_calls == []


def test_detection_and_alerts_emits_alert():
    engine = _EngineStub(anomalies=[{"code": "EXPOSURE_MISMATCH", "severity": "error", "description": "mismatch"}])
    alert_service = _AnomalyAlertServiceSpy()
    watchdog = _make_watchdog(
        anomaly_enabled=True,
        anomaly_alerts_enabled=True,
        anomaly_actions_enabled=False,
        anomaly_alert_service=alert_service,
        anomaly_engine=engine,
    )

    watchdog._tick()

    assert engine.calls == 1
    assert len(alert_service.calls) == 1
    assert alert_service.calls[0]["code"] == "EXPOSURE_MISMATCH"
    assert watchdog.escalation_requested is False


def test_detection_alerts_actions_requests_escalation_safely():
    engine = _EngineStub(anomalies=[{"code": "DB_CONTENTION", "severity": "critical", "description": "db lock"}])
    alert_service = _AnomalyAlertServiceSpy()
    hook_calls = []
    watchdog = _make_watchdog(
        anomaly_enabled=True,
        anomaly_alerts_enabled=True,
        anomaly_actions_enabled=True,
        anomaly_alert_service=alert_service,
        anomaly_engine=engine,
        anomaly_escalation_hook=lambda payload: hook_calls.append(payload),
    )

    watchdog._tick()

    assert len(alert_service.calls) == 1
    assert watchdog.escalation_requested is True
    assert watchdog.last_escalation_event is not None
    assert watchdog.last_escalation_event["code"] == "DB_CONTENTION"
    assert hook_calls and hook_calls[0]["escalation_requested"] is True


def test_anomaly_alert_delivery_failure_is_contained():
    engine = _EngineStub(anomalies=[{"code": "FANOUT_INCOMPLETE", "severity": "warning", "description": "fanout missing"}])
    alert_service = _AnomalyAlertServiceSpy(fail=True)
    watchdog = _make_watchdog(
        anomaly_enabled=True,
        anomaly_alerts_enabled=True,
        anomaly_engine=engine,
        anomaly_alert_service=alert_service,
    )

    watchdog._tick()

    assert engine.calls == 1
    assert watchdog.last_anomalies and watchdog.last_anomalies[0]["code"] == "FANOUT_INCOMPLETE"


def test_anomaly_hook_exception_is_contained():
    engine = _EngineStub(raises=True)
    watchdog = _make_watchdog(anomaly_enabled=True, anomaly_engine=engine)

    watchdog._tick()

    assert engine.calls == 1
    assert watchdog.last_anomalies == []


def test_anomaly_hook_exception_from_escalation_hook_is_contained():
    engine = _EngineStub(anomalies=[{"code": "ANY", "severity": "warning", "description": "x"}])

    def _failing_hook(_payload):
        raise RuntimeError("hook failed")

    watchdog = _make_watchdog(
        anomaly_enabled=True,
        anomaly_alerts_enabled=True,
        anomaly_actions_enabled=True,
        anomaly_engine=engine,
        anomaly_escalation_hook=_failing_hook,
    )

    watchdog._tick()

    assert engine.calls == 1
    assert watchdog.escalation_requested is True


def test_anomaly_enabled_with_empty_runtime_state_is_safe():
    engine = _EngineStub(anomalies=[])
    watchdog = _make_watchdog(anomaly_enabled=True, runtime_state={}, anomaly_engine=engine)

    watchdog._tick()

    assert engine.calls == 1
    assert watchdog.last_anomalies == []


def test_runtime_settings_toggle_controls_anomaly_hook(monkeypatch):
    settings = _SettingsStub(enabled=False, alerts=False, actions=False)
    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        settings_service=settings,
        anomaly_enabled=True,
        interval_sec=60.0,
    )
    calls = []
    monkeypatch.setattr(watchdog, "_run_anomaly_hook", lambda: calls.append("hook"))

    watchdog._tick()
    assert calls == []

    settings.enabled = True
    watchdog._tick()
    assert calls == ["hook"]


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
