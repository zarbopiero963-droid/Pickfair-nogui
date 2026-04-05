from __future__ import annotations

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

SEVERITY_RANK = {
    "INFO": 10,
    "WARNING": 20,
    "ERROR": 30,
    "CRITICAL": 40,
}


class TelegramAlertsService:
    """
    Notificatore alert verso Telegram.

    Dipendenze:
    - settings_service: SettingsService reale del repo
    - telegram_sender: sender già esistente del progetto

    Settings supportati:
    - alerts_enabled: bool
    - alerts_chat_id: str | int
    - alerts_chat_name: str
    - min_alert_severity: INFO | WARNING | ERROR | CRITICAL
    """

    def __init__(
        self,
        *,
        settings_service: Any,
        telegram_sender: Any,
    ) -> None:
        self.settings_service = settings_service
        self.telegram_sender = telegram_sender

    def notify_alert(self, alert: Dict[str, Any]) -> None:
        try:
            settings = self._get_telegram_settings()

            if not bool(settings.get("alerts_enabled", False)):
                return

            chat_id = settings.get("alerts_chat_id")
            if chat_id in (None, "", 0, "0"):
                logger.debug("Telegram alert skipped: alerts_chat_id missing")
                return

            min_severity = str(settings.get("min_alert_severity", "WARNING") or "WARNING").upper()
            severity = str(alert.get("severity", "WARNING") or "WARNING").upper()

            if not self._should_send(severity, min_severity):
                return

            text = self._format_alert_text(alert, settings)
            self._send_message(chat_id=chat_id, text=text)

        except Exception:
            logger.exception("TelegramAlertsService.notify_alert failed")

    def _get_telegram_settings(self) -> Dict[str, Any]:
        loader = getattr(self.settings_service, "load_telegram_config_row", None)
        if callable(loader):
            data = loader()
            if isinstance(data, dict):
                return data

        getter = getattr(self.settings_service, "get_all_settings", None)
        if callable(getter):
            try:
                data = getter() or {}
                return {
                    "alerts_enabled": bool(data.get("telegram.alerts_enabled", False)),
                    "alerts_chat_id": data.get("telegram.alerts_chat_id"),
                    "alerts_chat_name": data.get("telegram.alerts_chat_name", ""),
                    "min_alert_severity": data.get("telegram.min_alert_severity", "WARNING"),
                }
            except Exception:
                logger.exception("get_all_settings failed while reading telegram alert settings")

        return {}

    def _should_send(self, severity: str, min_severity: str) -> bool:
        return SEVERITY_RANK.get(severity, 0) >= SEVERITY_RANK.get(min_severity, 20)

    def _format_alert_text(self, alert: Dict[str, Any], settings: Dict[str, Any]) -> str:
        severity = str(alert.get("severity", "WARNING") or "WARNING").upper()
        code = str(alert.get("code", "UNKNOWN_ALERT") or "UNKNOWN_ALERT")
        title = str(
            alert.get("title")
            or alert.get("message")
            or code
        )
        source = str(alert.get("source", "system") or "system")
        description = alert.get("description")
        details = alert.get("details") or alert.get("payload") or {}

        chat_name = str(settings.get("alerts_chat_name", "") or "").strip()

        icon = {
            "INFO": "ℹ️",
            "WARNING": "⚠️",
            "ERROR": "❌",
            "CRITICAL": "🚨",
        }.get(severity, "⚠️")

        lines = [
            f"{icon} ALERT PICKFAIR",
            f"Severity: {severity}",
            f"Code: {code}",
            f"Title: {title}",
            f"Source: {source}",
        ]

        if chat_name:
            lines.append(f"Channel: {chat_name}")

        if description:
            lines.append(f"Description: {description}")

        if details:
            lines.append(f"Details: {details}")

        return "\n".join(lines)

    def _send_message(self, *, chat_id: Any, text: str) -> None:
        """
        Adattatore verso telegram_sender reale.
        Prova più firme compatibili.
        """
        send_alert_message = getattr(self.telegram_sender, "send_alert_message", None)
        if callable(send_alert_message):
            try:
                send_alert_message(chat_id, text)
                return
            except TypeError:
                try:
                    send_alert_message(chat_id=chat_id, text=text)
                    return
                except TypeError:
                    pass

        send_message = getattr(self.telegram_sender, "send_message", None)
        if callable(send_message):
            try:
                send_message(chat_id, text)
                return
            except TypeError:
                try:
                    send_message(chat_id=chat_id, text=text)
                    return
                except TypeError:
                    pass

        enqueue_message = getattr(self.telegram_sender, "enqueue_message", None)
        if callable(enqueue_message):
            try:
                enqueue_message(chat_id=chat_id, text=text, message_type="ALERT")
                return
            except TypeError:
                pass

        send = getattr(self.telegram_sender, "send", None)
        if callable(send):
            try:
                send(chat_id=chat_id, text=text)
                return
            except TypeError:
                pass

        raise RuntimeError("NO_VALID_TELEGRAM_SENDER_METHOD_FOR_ALERTS")
