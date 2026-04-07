from observability.runtime_probe import RuntimeProbe
from services.telegram_alerts_service import TelegramAlertsService


class _SettingsEnabled:
    def load_telegram_config_row(self):
        return {
            "alerts_enabled": True,
            "alerts_chat_id": "111",
            "min_alert_severity": "INFO",
            "alert_cooldown_sec": 0,
            "alert_dedup_enabled": False,
            "alert_format_rich": True,
        }


class _Sender:
    def send_alert_message(self, chat_id, text):
        return None


class _DbStub:
    def get_recent_observability_snapshots(self, limit=1):
        return [{"id": 1}]


def test_telegram_truth_contract_sender_absent_means_degraded_everywhere():
    alerts = TelegramAlertsService(settings_service=_SettingsEnabled(), telegram_sender=None)

    availability = alerts.availability_status()
    probe = RuntimeProbe(db=_DbStub(), telegram_alerts_service=alerts, settings_service=_SettingsEnabled())
    pipeline = probe.collect_runtime_state()["alert_pipeline"]

    assert availability["status"] == "DEGRADED"
    assert availability["reason"] == "sender_unavailable"
    assert availability["deliverable"] is False

    assert pipeline["status"] == "DEGRADED"
    assert pipeline["reason"] == "sender_unavailable"
    assert pipeline["deliverable"] is False


def test_telegram_truth_contract_sender_present_means_ready_when_deliverable():
    alerts = TelegramAlertsService(settings_service=_SettingsEnabled(), telegram_sender=_Sender())

    availability = alerts.availability_status()
    probe = RuntimeProbe(db=_DbStub(), telegram_alerts_service=alerts, settings_service=_SettingsEnabled())
    pipeline = probe.collect_runtime_state()["alert_pipeline"]

    assert availability["status"] == "READY"
    assert availability["reason"] is None
    assert availability["deliverable"] is True

    assert pipeline["status"] == "READY"
    assert pipeline["reason"] is None
    assert pipeline["deliverable"] is True
