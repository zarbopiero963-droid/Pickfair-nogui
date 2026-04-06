import tempfile
from pathlib import Path

from database import Database


def test_telegram_advanced_alert_settings_are_persisted():
    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))

        db.save_telegram_settings(
            {
                "alerts_enabled": True,
                "alerts_chat_id": "123",
                "alerts_chat_name": "ops",
                "min_alert_severity": "warning",
                "alert_cooldown_sec": 45,
                "alert_dedup_enabled": True,
                "alert_format_rich": True,
            }
        )

        settings = db.get_settings()
        telegram = db.get_telegram_settings()
        assert settings.get("telegram.alert_cooldown_sec") in {45, "45"}
        assert str(settings.get("telegram.alert_dedup_enabled")) in {"1", "True", "true"}
        assert str(settings.get("telegram.alert_format_rich")) in {"1", "True", "true"}
        assert telegram["alert_cooldown_sec"] == 45
        assert telegram["alert_dedup_enabled"] is True
        assert telegram["alert_format_rich"] is True


def test_telegram_advanced_alert_settings_defaults_are_safe():
    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))

        db.save_telegram_settings({})

        settings = db.get_settings()
        telegram = db.get_telegram_settings()
        assert settings.get("telegram.alert_cooldown_sec") in {0, "0"}
        assert str(settings.get("telegram.alert_dedup_enabled")) in {"0", "False", "false"}
        assert str(settings.get("telegram.alert_format_rich")) in {"0", "False", "false"}
        assert telegram["alert_cooldown_sec"] == 0
        assert telegram["alert_dedup_enabled"] is False
        assert telegram["alert_format_rich"] is False
