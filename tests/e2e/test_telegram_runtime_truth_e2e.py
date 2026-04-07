from observability.runtime_probe import RuntimeProbe
from services.telegram_alerts_service import TelegramAlertsService


class _SettingsEnabled:
    def load_telegram_config_row(self):
        return {
            "alerts_enabled": True,
            "alerts_chat_id": "321",
            "min_alert_severity": "WARNING",
            "alert_cooldown_sec": 0,
            "alert_dedup_enabled": False,
            "alert_format_rich": True,
        }


class _SettingsEnabledMissingChat:
    def load_telegram_config_row(self):
        return {
            "alerts_enabled": True,
            "alerts_chat_id": "",
            "min_alert_severity": "WARNING",
            "alert_cooldown_sec": 0,
            "alert_dedup_enabled": False,
            "alert_format_rich": True,
        }


class _SenderOk:
    def send_alert_message(self, chat_id, text):
        return None


class _DbStub:
    def get_recent_observability_snapshots(self, limit=1):
        return [{"id": 1}]


def test_telegram_runtime_truth_e2e_sender_missing_is_explicit_degraded_and_not_deliverable():
    alerts = TelegramAlertsService(settings_service=_SettingsEnabled(), telegram_sender=None)
    probe = RuntimeProbe(db=_DbStub(), telegram_alerts_service=alerts, settings_service=_SettingsEnabled())

    state = probe.collect_runtime_state()["alert_pipeline"]

    assert set(("alerts_enabled", "sender_available", "deliverable", "reason")) <= set(state)
    assert state["alerts_enabled"] is True
    assert state["sender_available"] is False
    assert state["deliverable"] is False
    assert state["status"] == "DEGRADED"
    assert state["reason"] == "sender_unavailable"
    assert state["status"] != "READY"


def test_telegram_runtime_truth_e2e_sender_present_is_deliverable_and_ready():
    alerts = TelegramAlertsService(settings_service=_SettingsEnabled(), telegram_sender=_SenderOk())
    probe = RuntimeProbe(db=_DbStub(), telegram_alerts_service=alerts, settings_service=_SettingsEnabled())

    state = probe.collect_runtime_state()["alert_pipeline"]

    assert state["alerts_enabled"] is True
    assert state["sender_available"] is True
    assert state["deliverable"] is True
    assert state["status"] == "READY"
    assert state["reason"] is None


def test_telegram_runtime_truth_e2e_probe_uses_real_deliverability_not_only_enabled_flag():
    alerts = TelegramAlertsService(settings_service=_SettingsEnabledMissingChat(), telegram_sender=_SenderOk())
    probe = RuntimeProbe(db=_DbStub(), telegram_alerts_service=alerts, settings_service=_SettingsEnabledMissingChat())

    state = probe.collect_runtime_state()["alert_pipeline"]

    assert state["alerts_enabled"] is True
    assert state["sender_available"] is True
    assert state["deliverable"] is False
    assert state["status"] == "DEGRADED"
    assert state["reason"] == "alerts_chat_id_missing"
