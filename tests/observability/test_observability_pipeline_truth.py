from observability.runtime_probe import RuntimeProbe
from services.telegram_alerts_service import TelegramAlertsService
from safe_mode import get_safe_mode_manager
from tests.helpers.fake_runtime_state import FakeRuntimeState


class _SettingsEnabled:
    def load_telegram_config_row(self):
        return {
            "alerts_enabled": True,
            "alerts_chat_id": "123",
            "min_alert_severity": "WARNING",
            "alert_cooldown_sec": 0,
            "alert_dedup_enabled": False,
            "alert_format_rich": True,
        }


class _SettingsEnabledMissingSender(_SettingsEnabled):
    pass


class _SenderOk:
    def __init__(self):
        self.calls = []

    def send_alert_message(self, chat_id, text):
        self.calls.append((chat_id, text))


class _DbStub:
    def get_recent_observability_snapshots(self, limit=1):
        return [{"id": 1}]


def test_observability_pipeline_truth_for_missing_sender_and_safe_mode_state():
    safe_mode = get_safe_mode_manager()
    safe_mode.reset()
    safe_mode.report_error("x", "y")
    safe_mode.report_error("x", "y")

    fake_state = FakeRuntimeState.ready().mark_sender_unavailable()
    alerts = TelegramAlertsService(settings_service=_SettingsEnabledMissingSender(), telegram_sender=None)
    probe = RuntimeProbe(
        db=_DbStub(),
        settings_service=_SettingsEnabledMissingSender(),
        telegram_service=None,
        telegram_alerts_service=alerts,
        safe_mode=safe_mode,
    )

    pipeline = probe.collect_runtime_state()["alert_pipeline"]

    assert pipeline["alerts_enabled"] is True
    assert pipeline["sender_available"] is False
    assert pipeline["deliverable"] is False
    assert pipeline["status"] == "DEGRADED"
    assert pipeline["reason"] == fake_state.reason
    assert pipeline["status"] != "READY"


def test_observability_pipeline_truth_for_deliverable_sender():
    fake_state = FakeRuntimeState.ready()
    sender = _SenderOk()
    alerts = TelegramAlertsService(settings_service=_SettingsEnabled(), telegram_sender=sender)
    alerts.notify_alert({"severity": "error", "code": "OBS-1", "message": "boom"})

    probe = RuntimeProbe(
        db=_DbStub(),
        settings_service=_SettingsEnabled(),
        telegram_service=None,
        telegram_alerts_service=alerts,
        safe_mode=None,
    )

    pipeline = probe.collect_runtime_state()["alert_pipeline"]

    assert pipeline["alerts_enabled"] is True
    assert pipeline["sender_available"] is True
    assert pipeline["deliverable"] is True
    assert pipeline["status"] == fake_state.runtime_state_label
    assert pipeline["reason"] is None
    assert pipeline["last_delivery_ok"] is True
