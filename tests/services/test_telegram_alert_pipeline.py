import pytest
import services.telegram_alerts_service as telegram_alerts_module

from observability.cto_reviewer import CtoReviewer
from services.telegram_alerts_service import TelegramAlertsService


class SettingsStub:
    def load_telegram_config_row(self):
        return {
            "alerts_enabled": True,
            "telegram_alerts_enabled": True,
            "alerts_chat_id": "12345",
            "telegram_alert_chat_id": "12345",
            "alerts_chat_name": "ops",
            "telegram_alert_name": "ops",
            "min_alert_severity": "WARNING",
            "telegram_alert_min_severity": "WARNING",
            "alert_cooldown_sec": 0,
            "telegram_alert_cooldown_sec": 0,
            "alert_dedup_enabled": True,
            "alert_format_rich": True,
        }


class SenderStub:
    def __init__(self):
        self.calls = []

    def send_alert_message(self, chat_id, text):
        self.calls.append((chat_id, text))


class DisabledSettingsStub:
    def load_telegram_config_row(self):
        return {
            "alerts_enabled": False,
            "alerts_chat_id": "12345",
            "alerts_chat_name": "ops",
            "min_alert_severity": "WARNING",
            "alert_cooldown_sec": 0,
            "alert_dedup_enabled": True,
            "alert_format_rich": True,
        }


class StrictSeveritySettingsStub(SettingsStub):
    def load_telegram_config_row(self):
        data = super().load_telegram_config_row()
        data["min_alert_severity"] = "CRITICAL"
        data["telegram_alert_min_severity"] = "CRITICAL"
        return data


class CooldownSettingsStub(SettingsStub):
    def load_telegram_config_row(self):
        data = super().load_telegram_config_row()
        data["alert_cooldown_sec"] = 600
        data["alert_dedup_enabled"] = True
        return data


@pytest.mark.smoke
def test_telegram_alert_pipeline_calls_sender_when_enabled():
    sender = SenderStub()
    svc = TelegramAlertsService(settings_service=SettingsStub(), telegram_sender=sender)

    result = svc.notify_alert({"severity": "critical", "code": "X1", "message": "boom"})

    assert result["delivered"] is True
    assert len(sender.calls) == 1
    assert sender.calls[0][0] == "12345"


@pytest.mark.smoke
def test_telegram_alert_pipeline_disabled_path_is_noop():
    sender = SenderStub()
    svc = TelegramAlertsService(settings_service=DisabledSettingsStub(), telegram_sender=sender)

    result = svc.notify_alert({"severity": "critical", "code": "X1", "message": "boom"})

    assert result["delivered"] is False
    assert sender.calls == []


@pytest.mark.smoke
def test_telegram_alert_pipeline_applies_min_severity_threshold():
    sender = SenderStub()
    svc = TelegramAlertsService(settings_service=StrictSeveritySettingsStub(), telegram_sender=sender)

    warn = svc.notify_alert({"severity": "warning", "code": "X1", "message": "warn"})
    crash = svc.notify_alert({"severity": "critical", "code": "X2", "message": "crash"})

    assert warn["delivered"] is False
    assert crash["delivered"] is True
    assert len(sender.calls) == 1
    assert "Code: X2" in sender.calls[0][1]


@pytest.mark.smoke
def test_telegram_alert_pipeline_dedup_and_cooldown():
    sender = SenderStub()
    svc = TelegramAlertsService(settings_service=CooldownSettingsStub(), telegram_sender=sender)

    payload = {"severity": "critical", "code": "X1", "message": "boom", "details": {"b": 1, "a": 2}}
    first = svc.notify_alert(payload)
    second = svc.notify_alert(payload)

    assert first["delivered"] is True
    assert second["reason"] == "dedup_cooldown"
    assert len(sender.calls) == 1
    assert "Details: a=2, b=1" in sender.calls[0][1]


@pytest.mark.smoke
def test_telegram_alert_pipeline_enabled_but_missing_sender_is_truthful_degraded():
    svc = TelegramAlertsService(settings_service=SettingsStub(), telegram_sender=None)

    result = svc.notify_alert({"severity": "critical", "code": "X3", "message": "lost"})

    assert result["delivered"] is False
    assert result["reason"] == "sender_unavailable"
    assert result["sender_available"] is False


def test_telegram_alert_pipeline_missing_chat_id_no_send():
    class MissingChat(SettingsStub):
        def load_telegram_config_row(self):
            data = super().load_telegram_config_row()
            data["alerts_chat_id"] = ""
            data["telegram_alert_chat_id"] = ""
            return data

    sender = SenderStub()
    svc = TelegramAlertsService(settings_service=MissingChat(), telegram_sender=sender)
    result = svc.notify_alert({"severity": "critical", "code": "X4", "message": "boom"})
    assert result["delivered"] is False
    assert result["reason"] == "alerts_chat_id_missing"
    assert sender.calls == []


def test_invalid_min_severity_falls_back_and_allows_high_signal():
    class InvalidSeverity(SettingsStub):
        def load_telegram_config_row(self):
            data = super().load_telegram_config_row()
            data["telegram_alert_min_severity"] = "NOPE"
            return data

    sender = SenderStub()
    svc = TelegramAlertsService(settings_service=InvalidSeverity(), telegram_sender=sender)
    result = svc.notify_alert({"severity": "high", "code": "X5", "message": "boom"})
    assert result["delivered"] is True
    assert len(sender.calls) == 1


def test_telegram_alert_pipeline_resend_after_cooldown(monkeypatch):
    sender = SenderStub()
    svc = TelegramAlertsService(settings_service=CooldownSettingsStub(), telegram_sender=sender)
    now = {"t": 1000.0}
    monkeypatch.setattr(telegram_alerts_module.time, "time", lambda: now["t"])

    payload = {"severity": "critical", "code": "RESEND", "message": "m1"}
    first = svc.notify_alert(payload)
    second = svc.notify_alert(payload)
    now["t"] += 601.0
    third = svc.notify_alert(payload)

    assert first["delivered"] is True
    assert second["reason"] == "dedup_cooldown"
    assert third["delivered"] is True
    assert len(sender.calls) == 2


def test_telegram_alert_pipeline_grouping_count_summary(monkeypatch):
    class AggSettings(SettingsStub):
        def load_telegram_config_row(self):
            data = super().load_telegram_config_row()
            data.update(
                {
                    "alert_aggregation_enabled": True,
                    "alert_aggregation_threshold": 3,
                    "alert_aggregation_window_sec": 60,
                    "alert_dedup_enabled": False,
                }
            )
            return data

    sender = SenderStub()
    svc = TelegramAlertsService(settings_service=AggSettings(), telegram_sender=sender)
    monkeypatch.setattr(telegram_alerts_module.time, "time", lambda: 2000.0)

    svc.notify_alert({"severity": "warning", "code": "G1", "message": "a"})
    svc.notify_alert({"severity": "warning", "code": "G2", "message": "b"})
    third = svc.notify_alert({"severity": "warning", "code": "G3", "message": "c"})

    assert third["delivered"] is True
    assert len(sender.calls) == 3
    assert "Alert Burst Detected" in sender.calls[-1][1]
    assert "Alerts in 60s: 3" in sender.calls[-1][1]


def test_telegram_alert_pipeline_primary_loader_accepts_new_key_schema_only():
    class NewOnlySettings:
        def load_telegram_config_row(self):
            return {
                "telegram_alerts_enabled": True,
                "telegram_alert_chat_id": "777",
                "telegram_alert_name": "ops-new",
                "telegram_alert_min_severity": "WARNING",
                "telegram_alert_cooldown_sec": 0,
                "alert_dedup_enabled": True,
                "alert_format_rich": True,
            }

    sender = SenderStub()
    svc = TelegramAlertsService(settings_service=NewOnlySettings(), telegram_sender=sender)
    result = svc.notify_alert({"severity": "critical", "code": "NEW-SCHEMA", "message": "ok"})
    assert result["delivered"] is True
    assert len(sender.calls) == 1
    assert sender.calls[0][0] == "777"


def test_cto_originated_alert_uses_same_telegram_pipeline(monkeypatch):
    sender = SenderStub()
    svc = TelegramAlertsService(settings_service=CooldownSettingsStub(), telegram_sender=sender)
    reviewer = CtoReviewer(history_window=4, cooldown_sec=60)

    cto_findings = reviewer.evaluate(
        {
            "now_ts": 100.0,
            "health_snapshot": {"overall_status": "DEGRADED"},
            "metrics_snapshot": {"gauges": {"stalled_ticks": 3, "completed_delta": 0, "repeated_high_ticks": 2, "missing_observability_sections": 1}},
            "anomaly_alerts": [{"code": "STALL", "severity": "high"}, {"code": "LAG", "severity": "high"}],
            "forensics_alerts": [],
            "incidents_snapshot": {"open_count": 1},
            "runtime_probe_state": {"component": "runtime-a", "alert_pipeline": {"enabled": True, "deliverable": False}},
            "diagnostics_bundle": {"available": False},
        }
    )
    silent = next(item for item in cto_findings if item["rule_name"] == "SILENT_FAILURE_DETECTED")
    payload = {
        "code": f"CTO::{silent['rule_name']}",
        "severity": silent["severity"],
        "title": silent["rule_name"],
        "message": silent["short_explanation"],
        "source": "cto_reviewer",
        "details": {"key_metrics": silent["key_metrics"]},
        "suggested_action": silent["suggested_action"],
        "timestamp": "2026-04-14 12:30:00 UTC",
    }

    now = {"t": 2000.0}
    monkeypatch.setattr(telegram_alerts_module.time, "time", lambda: now["t"])
    first = svc.notify_alert(payload)
    second = svc.notify_alert(payload)

    assert first["delivered"] is True
    assert second["reason"] == "dedup_cooldown"
    assert len(sender.calls) == 1
    text = sender.calls[0][1]
    assert "CTO::SILENT_FAILURE_DETECTED" in text
    assert "Source: cto_reviewer" in text
    assert "Suggested action:" in text
