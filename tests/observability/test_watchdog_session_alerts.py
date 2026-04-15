from observability.alerts_manager import AlertsManager
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService


class ProbeStub:
    def __init__(self, runtime_state=None):
        self.runtime_state = runtime_state or {}

    def collect_health(self):
        return {"runtime": {"status": "READY", "reason": "ok", "details": {}}}

    def collect_metrics(self):
        return {}

    def collect_runtime_state(self):
        return self.runtime_state


class SnapshotStub:
    def collect_and_store(self):
        return None


def make_watchdog(runtime_state, alerts=None):
    return WatchdogService(
        probe=ProbeStub(runtime_state),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts or AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=SnapshotStub(),
    )


def _session(**overrides):
    data = {
        "state": "ACTIVE",
        "consecutive_keepalive_failures": 0,
        "consecutive_login_failures": 0,
        "last_error_code": None,
        "token_present": True,
        "logged_in_at": 1000.0,
        "session_expires_at": 2200.0,
        "session_ttl_sec": 1200,
    }
    data.update(overrides)
    return {"session_manager": data}


def _active_codes(alerts):
    return {a["code"] for a in alerts.active_alerts()}


def test_watchdog_raises_session_keepalive_failed_alert():
    alerts = AlertsManager()
    w = make_watchdog(_session(state="DEGRADED", consecutive_keepalive_failures=1), alerts)
    w._evaluate_alerts()
    assert "SESSION_KEEPALIVE_FAILED" in _active_codes(alerts)


def test_watchdog_raises_session_expired_alert():
    alerts = AlertsManager()
    w = make_watchdog(_session(state="EXPIRED", token_present=False), alerts)
    w._evaluate_alerts()
    assert "SESSION_EXPIRED" in _active_codes(alerts)


def test_watchdog_raises_session_relogin_failed_alert():
    alerts = AlertsManager()
    w = make_watchdog(_session(state="DEGRADED", consecutive_login_failures=2), alerts)
    w._evaluate_alerts()
    assert "SESSION_RELOGIN_FAILED" in _active_codes(alerts)


def test_watchdog_raises_session_login_throttled_alert():
    alerts = AlertsManager()
    w = make_watchdog(_session(state="LOCKED_OUT", last_error_code="TEMPORARY_BAN_TOO_MANY_REQUESTS"), alerts)
    w._evaluate_alerts()
    assert "SESSION_LOGIN_THROTTLED" in _active_codes(alerts)


def test_watchdog_resolves_session_alert_after_recovery():
    alerts = AlertsManager()
    w = make_watchdog(_session(state="EXPIRED", token_present=False), alerts)
    w._evaluate_alerts()
    assert "SESSION_EXPIRED" in _active_codes(alerts)

    w.probe.runtime_state = _session(state="ACTIVE", token_present=True)
    w._evaluate_alerts()
    assert "SESSION_EXPIRED" not in _active_codes(alerts)


def test_watchdog_does_not_touch_unrelated_alerts_when_resolving_session_alerts():
    alerts = AlertsManager()
    alerts.upsert_alert("UNRELATED", "warning", "keep", source="system")
    w = make_watchdog(_session(state="EXPIRED", token_present=False), alerts)
    w._evaluate_alerts()
    w.probe.runtime_state = _session(state="ACTIVE", token_present=True)
    w._evaluate_alerts()
    unrelated = [a for a in alerts.snapshot().get("alerts", []) if a.get("code") == "UNRELATED"][0]
    assert unrelated["active"] is True
    assert unrelated["source"] == "system"
