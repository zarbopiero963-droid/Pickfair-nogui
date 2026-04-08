from observability.runtime_probe import RuntimeProbe
from tests.helpers.fake_runtime_state import FakeRuntimeState


class _SettingsStub:
    def __init__(self, fake_state: FakeRuntimeState):
        self._state = fake_state

    def load_telegram_config_row(self):
        return {"alerts_enabled": self._state.alerts_enabled}


class _TelegramStub:
    def __init__(self, fake_state: FakeRuntimeState):
        self._state = fake_state

    def get_sender(self):
        return object() if self._state.sender_available else None


class _AlertsSvcStub:
    def __init__(self, fake_state: FakeRuntimeState):
        self._state = fake_state

    def availability_status(self):
        return self._state.to_snapshot()["alert_pipeline"] | {
            "last_delivery_ok": self._state.deliverable,
            "last_delivery_error": self._state.reason or "",
        }


class _SafeModeStub:
    def is_enabled(self):
        return True


class _DbStub:
    def get_recent_observability_snapshots(self, limit=1):
        return [{"id": 1}]


class _RuntimeControllerNoChecker:
    pass


class _TradingEngineFromState:
    def __init__(self, fake_state: FakeRuntimeState):
        self._state = fake_state

    def readiness(self):
        if self._state.runtime_state_label == "UNKNOWN":
            return {"state": "READY", "health": {}}
        return {
            "state": self._state.runtime_state_label,
            "health": {"recent_failures": self._state.recent_failures},
        }


def test_runtime_probe_alert_pipeline_state_uses_wired_services():
    fake_state = FakeRuntimeState.degraded(reason="sender_unavailable").mark_sender_unavailable()
    probe = RuntimeProbe(
        db=_DbStub(),
        settings_service=_SettingsStub(fake_state),
        telegram_service=_TelegramStub(fake_state),
        telegram_alerts_service=_AlertsSvcStub(fake_state),
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
    fake_state = FakeRuntimeState.unknown(reason="ready_without_health").mark_heartbeat_stale(age_seconds=240.0)
    probe = RuntimeProbe(trading_engine=_TradingEngineFromState(fake_state))

    health = probe.collect_health()
    engine_health = health["trading_engine"]

    assert engine_health["status"] == "UNKNOWN"
    assert engine_health["reason"] == "ready_without_health"
    assert engine_health["details"]["fallback_status"] == "READY"
