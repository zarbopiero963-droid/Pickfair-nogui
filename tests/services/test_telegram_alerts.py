from services.telegram_alerts_service import TelegramAlertsService


def test_telegram_alerts_module_smoke_import():
    assert TelegramAlertsService is not None
