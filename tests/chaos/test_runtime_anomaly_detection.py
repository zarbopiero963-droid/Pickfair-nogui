from observability.alerts_manager import AlertsManager
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService
from tests.helpers.fake_runtime_state import FakeRuntimeState


class _ProbeStub:
    def __init__(self, runtime_state=None):
        self.runtime_state = runtime_state

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
    def __init__(self, enabled=False):
        self.enabled = enabled

    def load_anomaly_enabled(self):
        return self.enabled


def _make_watchdog(*, anomaly_enabled: bool) -> WatchdogService:
class _AnomalyEngineStub:
    def __init__(self, anomalies):
        self._anomalies = list(anomalies)
        self.calls = 0

    def evaluate(self, _context):
        self.calls += 1
        return list(self._anomalies)


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
    anomalies=None,
) -> WatchdogService:
    anomaly_alert_service=None,
    anomaly_engine=None,
) -> WatchdogService:
class _EngineStub:
    def __init__(self, response=None, raises=False):
        self.response = response if response is not None else []
        self.raises = raises
        self.calls = 0

    def evaluate(self, context):
        del context
        self.calls += 1
        if self.raises:
            raise RuntimeError("boom")
        return self.response


def _make_watchdog(*, anomaly_enabled: bool, runtime_state=None, anomaly_engine=None) -> WatchdogService:
    return WatchdogService(
        probe=_ProbeStub(runtime_state=runtime_state),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_engine=_AnomalyEngineStub(anomalies or []),
        anomaly_enabled=anomaly_enabled,
        anomaly_alerts_enabled=anomaly_alerts_enabled,
        anomaly_actions_enabled=anomaly_actions_enabled,
        anomaly_engine=anomaly_engine,
        anomaly_enabled=anomaly_enabled,
        anomaly_alerts_enabled=anomaly_alerts_enabled,
        anomaly_alert_service=anomaly_alert_service,
        anomaly_engine=anomaly_engine,
        interval_sec=60.0,
    )


def test_anomaly_disabled_no_effect():
    engine = _EngineStub(response=[{"code": "IGNORED"}])
    watchdog = _make_watchdog(anomaly_enabled=False, anomaly_engine=engine)

    watchdog._tick()

    assert engine.calls == 0
    assert watchdog.last_anomalies == []

def test_anomaly_enabled_alerts_disabled_detects_without_delivery():
    engine = _AnomalyEngineStub([
        {
            "code": "GHOST_ORDER",
            "severity": "warning",
            "description": "ghost order found",
            "details": {"order_id": "A1"},
        }
    ])
    alert_service = _AnomalyAlertServiceSpy()
    watchdog = _make_watchdog(
        anomaly_enabled=True,
        anomaly_alerts_enabled=False,
        anomaly_alert_service=alert_service,
        anomaly_engine=engine,
    )

    watchdog._tick()

    assert engine.calls == 1
    assert alert_service.calls == []


def test_anomaly_enabled_alerts_enabled_sends_structured_payload():
    engine = _AnomalyEngineStub([
        {
            "code": "EXPOSURE_MISMATCH",
            "severity": "error",
            "description": "exposure mismatch",
            "details": {"symbol": "BTCUSDT", "expected": 1.0, "actual": 0.5},
        }
    ])
    alert_service = _AnomalyAlertServiceSpy()
    watchdog = _make_watchdog(
        anomaly_enabled=True,
        anomaly_alerts_enabled=True,
        anomaly_alert_service=alert_service,

def test_anomaly_enabled_collects_anomalies_and_stays_alive():
    engine = _EngineStub(response=[{"code": "CONTRADICTION", "severity": "warning", "message": "bad"}])
    watchdog = _make_watchdog(
        anomaly_enabled=True,
        runtime_state={"reconcile": {"ghost_orders_count": 1}},
        anomaly_engine=engine,
    )

    watchdog._tick()

    assert len(alert_service.calls) == 1
    payload = alert_service.calls[0]
    assert payload["code"] == "EXPOSURE_MISMATCH"
    assert payload["severity"] == "error"
    assert payload["source"] == "watchdog_service"
    assert payload["description"] == "exposure mismatch"
    assert payload["details"]["symbol"] == "BTCUSDT"


def test_anomaly_hook_is_skipped_when_flag_disabled():
    engine = _AnomalyEngineStub([
        {"code": "DB_CONTENTION", "severity": "warning", "description": "db lock contention"}
    ])
    alert_service = _AnomalyAlertServiceSpy()
    watchdog = _make_watchdog(
        anomaly_enabled=False,
        anomaly_alerts_enabled=True,
        anomaly_alert_service=alert_service,
        anomaly_engine=engine,
    )

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
    assert engine.calls == 0
    assert alert_service.calls == []


def test_anomaly_alert_delivery_failure_is_contained():
    engine = _AnomalyEngineStub([
        {"code": "FANOUT_INCOMPLETE", "severity": "warning", "description": "fanout missing"}
    ])
    alert_service = _AnomalyAlertServiceSpy(fail=True)
    watchdog = _make_watchdog(
        anomaly_enabled=True,
        anomaly_alerts_enabled=True,
        anomaly_alert_service=alert_service,
        anomaly_engine=engine,
    )
    assert engine.calls == 1
    assert any(item.get("code") == "CONTRADICTION" for item in watchdog.last_anomalies)
    assert any(item.get("code") == "GHOST_ORDER_DETECTED" for item in watchdog.last_anomalies)


def test_anomaly_hook_exception_is_contained():
    engine = _EngineStub(raises=True)
    watchdog = _make_watchdog(anomaly_enabled=True, anomaly_engine=engine)

    watchdog._tick()

    assert engine.calls == 1
    assert watchdog.last_anomalies == []


def test_anomaly_enabled_with_empty_runtime_state_is_safe():
    engine = _EngineStub(response=[])
    watchdog = _make_watchdog(anomaly_enabled=True, runtime_state={}, anomaly_engine=engine)

    anomalies = watchdog._run_anomaly_checks()

    assert anomalies == []
    assert watchdog.last_anomalies == []
    watchdog._tick()


def test_runtime_settings_toggle_controls_anomaly_hook(monkeypatch):
    settings = _SettingsStub(enabled=False)
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
