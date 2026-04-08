from observability.runtime_probe import RuntimeProbe
from tests.helpers.fake_settings import FakeSettingsService


class _TelegramStub:
    def get_sender(self):
        return object()


class _AlertsSvcStub:
    def availability_status(self):
        return {
            "alerts_enabled": True,
            "sender_available": False,
            "deliverable": False,
            "reason": "sender_unavailable",
            "last_delivery_ok": False,
            "last_delivery_error": "sender_unavailable",
        }


class _SafeModeStub:
    def is_enabled(self):
        return True


class _DbStub:
    def get_recent_observability_snapshots(self, limit=1):
        return [{"id": 1}]


class _RuntimeControllerNoChecker:
    pass


class _TradingEngineReadyNoHealth:
    def readiness(self):
        return {"state": "READY", "health": {}}


def test_runtime_probe_alert_pipeline_state_uses_wired_services():
    probe = RuntimeProbe(
        db=_DbStub(),
        settings_service=FakeSettingsService({"anomaly_alerts_enabled": True}),
        telegram_service=_TelegramStub(),
        telegram_alerts_service=_AlertsSvcStub(),
        safe_mode=_SafeModeStub(),
    )

    state = probe.collect_runtime_state()

    assert state["alert_pipeline"]["alerts_enabled"] is True
    assert state["alert_pipeline"]["sender_available"] is False
    assert state["alert_pipeline"]["deliverable"] is False
    assert state["safe_mode_enabled"] is True


def test_collect_health_reports_unknown_with_ready_fallback_for_missing_health_checks():
    probe = RuntimeProbe(runtime_controller=_RuntimeControllerNoChecker())

    health = probe.collect_health()
    runtime_health = health["runtime_controller"]

    assert runtime_health["status"] == "UNKNOWN"
    assert runtime_health["reason"] == "no-checker"
    assert runtime_health["details"]["fallback_status"] == "READY"


def test_collect_health_reports_unknown_for_ready_state_without_health_payload():
    probe = RuntimeProbe(trading_engine=_TradingEngineReadyNoHealth())

    health = probe.collect_health()
    engine_health = health["trading_engine"]

    assert engine_health["status"] == "UNKNOWN"
    assert engine_health["reason"] == "ready_without_health"
    assert engine_health["details"]["fallback_status"] == "READY"


def test_runtime_probe_alert_pipeline_safe_on_missing_fake_settings_keys():
    probe = RuntimeProbe(
        db=_DbStub(),
        settings_service=FakeSettingsService(),
        telegram_service=_TelegramStub(),
    )

    state = probe.collect_runtime_state()

    assert state["alert_pipeline"]["alerts_enabled"] is False
    assert state["alert_pipeline"]["status"] == "DISABLED"
