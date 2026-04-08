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
    anomaly_alert_service=None,
    anomaly_engine=None,
) -> WatchdogService:
    return WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_enabled=anomaly_enabled,
        anomaly_alerts_enabled=anomaly_alerts_enabled,
        anomaly_alert_service=anomaly_alert_service,
        anomaly_engine=anomaly_engine,
        interval_sec=60.0,
    )


def test_anomaly_flag_defaults_to_off():
    watchdog = _make_watchdog(anomaly_enabled=False)
    assert watchdog.anomaly_enabled is False


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

    watchdog._tick()

    assert engine.calls == 1
    assert len(alert_service.calls) == 1
