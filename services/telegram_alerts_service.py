from __future__ import annotations

import logging
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)

SEVERITY_RANK = {
    "INFO": 10,
    "WARNING": 20,
    "ERROR": 30,
    "HIGH": 35,
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
    - min_alert_severity: INFO | WARNING | ERROR | HIGH | CRITICAL
    """

    def __init__(
        self,
        *,
        settings_service: Any,
        telegram_sender: Any,
    ) -> None:
        self.settings_service = settings_service
        self.telegram_sender = telegram_sender
        self._last_sent_at: Dict[str, float] = {}
        self.last_delivery_error: str = ""
        self.last_delivery_ok: bool = False

    def notify_alert(self, alert: Dict[str, Any]) -> Dict[str, Any]:
        settings = self._get_telegram_settings()
        availability = self.availability_status(settings=settings)

        if not availability["alerts_enabled"]:
            self.last_delivery_ok = False
            self.last_delivery_error = "alerts_disabled"
            return {**availability, "delivered": False, "reason": "alerts_disabled"}

        if not availability["sender_available"]:
            self.last_delivery_ok = False
            self.last_delivery_error = "sender_unavailable"
            logger.warning("Telegram alert skipped: sender unavailable while alerts are enabled")
            return {**availability, "delivered": False, "reason": "sender_unavailable"}

        chat_id = settings.get("alerts_chat_id")
        if chat_id in (None, "", 0, "0"):
            self.last_delivery_ok = False
            self.last_delivery_error = "alerts_chat_id_missing"
            logger.warning("Telegram alert skipped: alerts_chat_id missing while alerts are enabled")
            return {**availability, "delivered": False, "reason": "alerts_chat_id_missing"}

        min_severity = str(settings.get("min_alert_severity", "WARNING") or "WARNING").upper()
        severity = str(alert.get("severity", "WARNING") or "WARNING").upper()

        if not self._should_send(severity, min_severity):
            self.last_delivery_ok = False
            self.last_delivery_error = "below_min_severity"
            return {**availability, "delivered": False, "reason": "below_min_severity"}

        dedup_enabled = bool(settings.get("alert_dedup_enabled", True))
        cooldown_sec = int(settings.get("alert_cooldown_sec", 300) or 300)
        dedup_key = self._dedup_key(alert)
        now = time.time()
        if dedup_enabled and cooldown_sec > 0:
            last_sent = float(self._last_sent_at.get(dedup_key, 0.0) or 0.0)
            if (now - last_sent) < cooldown_sec:
                self.last_delivery_ok = False
                self.last_delivery_error = "dedup_cooldown"
                return {**availability, "delivered": False, "reason": "dedup_cooldown"}

        text = self._format_alert_text(alert, settings)

        try:
            self._send_message(chat_id=chat_id, text=text)
            self._last_sent_at[dedup_key] = now
            self.last_delivery_ok = True
            self.last_delivery_error = ""
            return {**availability, "delivered": True, "reason": "sent"}
        except Exception as exc:
            self.last_delivery_ok = False
            self.last_delivery_error = str(exc)
            logger.exception("TelegramAlertsService.notify_alert failed")
            return {**availability, "delivered": False, "reason": "send_failed", "error": str(exc)}

    def availability_status(self, settings: Dict[str, Any] | None = None) -> Dict[str, Any]:
        settings = settings or self._get_telegram_settings()
        alerts_enabled = bool(settings.get("alerts_enabled", False))
        sender_available = self._sender_has_supported_method(self.telegram_sender)
        deliverable = alerts_enabled and sender_available and settings.get("alerts_chat_id") not in (None, "", 0, "0")

        reason = None
        status = "DISABLED"
        if alerts_enabled and not sender_available:
            reason = "sender_unavailable"
            status = "DEGRADED"
        elif alerts_enabled and not deliverable:
            reason = "alerts_chat_id_missing"
            status = "DEGRADED"
        elif alerts_enabled and deliverable:
            status = "READY"

        return {
            "alerts_enabled": alerts_enabled,
            "sender_available": sender_available,
            "deliverable": bool(deliverable),
            "status": status,
            "reason": reason,
            "last_delivery_ok": self.last_delivery_ok,
            "last_delivery_error": self.last_delivery_error,
        }

    def _sender_has_supported_method(self, sender: Any) -> bool:
        return sender is not None and any(
            callable(getattr(sender, name, None))
            for name in ("send_alert_message", "send_message", "enqueue_message", "send")
        )

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
                    "alert_cooldown_sec": data.get("telegram.alert_cooldown_sec", 300),
                    "alert_dedup_enabled": bool(data.get("telegram.alert_dedup_enabled", True)),
                    "alert_format_rich": bool(data.get("telegram.alert_format_rich", True)),
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

        lifecycle = str(alert.get("lifecycle") or "").upper()
        if lifecycle not in {"OPEN", "UPDATED", "RESOLVED"}:
            lifecycle = ""

        icon = {
            "INFO": "ℹ️",
            "WARNING": "⚠️",
            "ERROR": "❌",
            "HIGH": "🔥",
            "CRITICAL": "🚨",
        }.get(severity, "⚠️")

        lines = [
            f"{icon} ALERT PICKFAIR",
            f"Severity: {severity}",
            f"Code: {code}",
            f"Title: {title}",
            f"Source: {source}",
        ]

        if lifecycle:
            lines.append(f"Lifecycle: {lifecycle}")

        if chat_name:
            lines.append(f"Channel: {chat_name}")

        if description:
            lines.append(f"Description: {description}")

        if details:
            if bool(settings.get("alert_format_rich", True)):
                rendered = ", ".join(f"{k}={v}" for k, v in sorted(dict(details).items()))
                lines.append(f"Details: {rendered}")
            else:
                lines.append(f"Details: {details}")

        return "\n".join(lines)

    def _dedup_key(self, alert: Dict[str, Any]) -> str:
        code = str(alert.get("code", "UNKNOWN_ALERT") or "UNKNOWN_ALERT")
        severity = str(alert.get("severity", "WARNING") or "WARNING").upper()
        message = str(alert.get("message") or alert.get("title") or code)
        return f"{code}|{severity}|{message}"

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
