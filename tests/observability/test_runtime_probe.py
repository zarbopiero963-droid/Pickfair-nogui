from observability.runtime_probe import RuntimeProbe
from tests.helpers.fake_runtime_state import FakeRuntimeState
from tests.helpers.fake_settings import FakeSettingsService


class _TelegramStub:
    def get_sender(self):
        return object()


class _AlertsSvcStub:
    def __init__(self, fake_state: FakeRuntimeState):
        self._state = fake_state

    def availability_status(self):
        return self._state.alert_pipeline_snapshot()


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


class _SettingsStub:
    def __init__(self, fake_state: FakeRuntimeState):
        self._state = fake_state

    def load_telegram_config_row(self):
        return {"alerts_enabled": self._state.alerts_enabled}


class _HealthProbeStub(RuntimeProbe):
    def __init__(self, status):
        super().__init__()
        self._status = status

    def collect_health(self):
        return {
            "component_a": {
                "name": "component_a",
                "status": self._status,
                "reason": "test_reason",
                "details": {},
            }
        }


def test_runtime_probe_alert_pipeline_state_uses_wired_services():
    fake_state = FakeRuntimeState.degraded(reason="sender_unavailable")
    probe = RuntimeProbe(
        db=_DbStub(),
        settings_service=_SettingsStub(fake_state),
        telegram_service=_TelegramStub(),
        telegram_alerts_service=_AlertsSvcStub(fake_state),
        safe_mode=_SafeModeStub(),
    )

    state = probe.collect_runtime_state()

    assert state["alert_pipeline"] == fake_state.alert_pipeline_snapshot()
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


def test_fake_settings_snapshot_reload_round_trip():
    settings = FakeSettingsService({"anomaly_alerts_enabled": "yes"})
    settings.set("region", "eu-west")

    reloaded = FakeSettingsService.from_snapshot(settings.snapshot())

    assert reloaded.get_bool("anomaly_alerts_enabled") is True
    assert reloaded.get("region") == "eu-west"


def test_fake_runtime_state_builder_variants_are_deterministic():
    ready = FakeRuntimeState.ready()
    unknown = FakeRuntimeState.unknown(reason="no_data")

    assert ready.to_snapshot()["runtime_state_label"] == "READY"
    assert ready.alert_pipeline_snapshot()["status"] == "READY"
    assert unknown.to_snapshot()["runtime_state_label"] == "UNKNOWN"
    assert unknown.alert_pipeline_snapshot()["status"] == "DISABLED"


def test_unknown_status_is_blocker():
    probe = _HealthProbeStub("BROKEN")

    report = probe.get_live_readiness_report()

    assert report["level"] == "NOT_READY"
    assert report["ready"] is False
    assert report["blockers"][0]["name"] == "component_a"
    assert "UNRECOGNIZED_STATE::BROKEN" in report["blockers"][0]["reason"]


def test_starting_status_is_blocker():
    probe = _HealthProbeStub("STARTING")

    report = probe.get_live_readiness_report()

    assert report["level"] == "NOT_READY"
    assert report["ready"] is False
    assert report["blockers"][0]["name"] == "component_a"
    assert report["blockers"][0]["status"] == "STARTING"
    assert "UNRECOGNIZED_STATE::STARTING" in report["blockers"][0]["reason"]


def test_ready_still_passes():
    probe = _HealthProbeStub("READY")

    report = probe.get_live_readiness_report()

    assert report["level"] == "READY"
    assert report["ready"] is True
    assert report["blockers"] == []


def test_degraded_separated():
    probe = _HealthProbeStub("DEGRADED")

    report = probe.get_live_readiness_report()

    assert report["level"] == "DEGRADED"
    assert report["ready"] is False
    assert report["blockers"] == []
    assert report["details"]["degraded"][0]["name"] == "component_a"


def test_none_status_fail_closed():
    probe = _HealthProbeStub(None)

    report = probe.get_live_readiness_report()

    assert report["level"] == "NOT_READY"
    assert report["ready"] is False
    assert report["blockers"][0]["name"] == "component_a"
    assert report["blockers"][0]["status"] == "UNKNOWN"
    assert report["blockers"][0]["reason"] == "test_reason"
