from __future__ import annotations

import logging
import time
from collections import Counter
from typing import Any, Dict

logger = logging.getLogger(__name__)

SEVERITY_RANK = {
    "INFO": 10,
    "WARNING": 20,
    "ERROR": 30,
    "HIGH": 35,
    "CRITICAL": 40,
}



_REDACTED = "[REDACTED]"
_TELEGRAM_SENSITIVE_KEYS = {
    "token",
    "auth_token",
    "access_token",
    "bearer",
    "user_session",
    "session",
    "session_token",
    "api_key",
    "secret",
    "password",
    "authorization",
}


def _sanitize_telegram_payload(value: Any) -> Any:
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            key = str(k)
            if key.lower() in _TELEGRAM_SENSITIVE_KEYS:
                out[k] = _REDACTED
            else:
                out[k] = _sanitize_telegram_payload(v)
        return out
    if isinstance(value, list):
        return [_sanitize_telegram_payload(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_telegram_payload(v) for v in value)
    return value

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
        self._aggregation_state: Dict[str, Any] = {
            "window_start": 0.0,
            "count": 0,
            "severity_counter": Counter(),
            "code_counter": Counter(),
            "summary_sent": False,
        }
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

        chat_id = settings.get("telegram_alert_chat_id") or settings.get("alerts_chat_id")
        if chat_id in (None, "", 0, "0"):
            self.last_delivery_ok = False
            self.last_delivery_error = "alerts_chat_id_missing"
            logger.warning("Telegram alert skipped: alerts_chat_id missing while alerts are enabled")
            return {**availability, "delivered": False, "reason": "alerts_chat_id_missing"}

        min_severity = str(settings.get("telegram_alert_min_severity") or settings.get("min_alert_severity", "WARNING") or "WARNING").upper()
        if min_severity not in SEVERITY_RANK:
            min_severity = "WARNING"
        severity = str(alert.get("severity", "WARNING") or "WARNING").upper()

        if not self._should_send(severity, min_severity):
            self.last_delivery_ok = False
            self.last_delivery_error = "below_min_severity"
            return {**availability, "delivered": False, "reason": "below_min_severity"}

        dedup_enabled = bool(settings.get("alert_dedup_enabled", True))
        cooldown_sec = int(settings.get("telegram_alert_cooldown_sec") or settings.get("alert_cooldown_sec", 300) or 300)
        dedup_key = self._dedup_key(alert)
        now = time.time()
        if dedup_enabled and cooldown_sec > 0:
            last_sent = float(self._last_sent_at.get(dedup_key, 0.0) or 0.0)
            if (now - last_sent) < cooldown_sec:
                self.last_delivery_ok = False
                self.last_delivery_error = "dedup_cooldown"
                return {**availability, "delivered": False, "reason": "dedup_cooldown"}

        aggregated = self._maybe_aggregate(alert=alert, settings=settings, now=now)
        if aggregated["suppress"]:
            self.last_delivery_ok = False
            self.last_delivery_error = "alert_aggregation_suppressed"
            return {**availability, "delivered": False, "reason": "alert_aggregation_suppressed"}

        text = aggregated["text"] or self._format_alert_text(alert, settings)

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

    def notify_anomaly_alert(self, anomaly: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "code": anomaly.get("code", "UNKNOWN_ANOMALY"),
            "severity": anomaly.get("severity", "warning"),
            "source": anomaly.get("source", "watchdog_service"),
            "description": anomaly.get("description") or anomaly.get("message") or anomaly.get("code", "anomaly"),
            "details": anomaly.get("details") or {},
            "type": "anomaly",
            "title": anomaly.get("title") or anomaly.get("code") or "Anomaly detected",
            "message": anomaly.get("message") or anomaly.get("description") or anomaly.get("code") or "Anomaly detected",
        }
        return self.notify_alert(payload)
    def _maybe_aggregate(self, *, alert: Dict[str, Any], settings: Dict[str, Any], now: float) -> Dict[str, Any]:
        enabled = bool(settings.get("alert_aggregation_enabled", True))
        if not enabled:
            return {"text": None, "suppress": False}

        threshold = int(settings.get("alert_aggregation_threshold", 5) or 5)
        window_sec = int(settings.get("alert_aggregation_window_sec", 10) or 10)
        if threshold < 2 or window_sec <= 0:
            return {"text": None, "suppress": False}

        state = self._aggregation_state
        if state["window_start"] <= 0 or (now - float(state["window_start"])) > window_sec:
            state["window_start"] = now
            state["count"] = 0
            state["severity_counter"] = Counter()
            state["code_counter"] = Counter()
            state["summary_sent"] = False

        severity = str(alert.get("severity", "WARNING") or "WARNING").upper()
        code = str(alert.get("code", "UNKNOWN_ALERT") or "UNKNOWN_ALERT")
        state["count"] += 1
        state["severity_counter"][severity] += 1
        state["code_counter"][code] += 1

        if state["count"] < threshold:
            return {"text": None, "suppress": False}

        if state["summary_sent"]:
            return {"text": None, "suppress": True}

        state["summary_sent"] = True
        top_codes = ", ".join(
            f"{code_name}={count}" for code_name, count in state["code_counter"].most_common(3)
        )
        severity_mix = ", ".join(
            f"{sev}={count}" for sev, count in state["severity_counter"].most_common()
        )
        summary_text = (
            "🚨 Pickfair Alert Burst Detected\n"
            f"• Alerts in {window_sec}s: {state['count']} (threshold={threshold})\n"
            f"• Severity mix: {severity_mix}\n"
            f"• Top codes: {top_codes}\n"
            "• Individual alerts temporarily aggregated to reduce Telegram flood risk."
        )
        return {"text": summary_text, "suppress": False}

    def availability_status(self, settings: Dict[str, Any] | None = None) -> Dict[str, Any]:
        settings = settings or self._get_telegram_settings()
        alerts_enabled = bool(settings.get("telegram_alerts_enabled", settings.get("alerts_enabled", False)))
        sender_available = self._sender_has_supported_method(self.telegram_sender)
        chat_id = settings.get("telegram_alert_chat_id") or settings.get("alerts_chat_id")
        deliverable = alerts_enabled and sender_available and chat_id not in (None, "", 0, "0")

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
                return self._normalize_telegram_settings(data)

        getter = getattr(self.settings_service, "get_all_settings", None)
        if callable(getter):
            try:
                data = getter() or {}
                return self._normalize_telegram_settings(data)
            except Exception:
                logger.exception("get_all_settings failed while reading telegram alert settings")

        return {}

    def _normalize_telegram_settings(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "alerts_enabled": bool(data.get("alerts_enabled", data.get("telegram.alerts_enabled", data.get("telegram_alerts_enabled", False)))),
            "alerts_chat_id": data.get("alerts_chat_id", data.get("telegram.alerts_chat_id", data.get("telegram_alert_chat_id"))),
            "alerts_chat_name": data.get("alerts_chat_name", data.get("telegram.alerts_chat_name", data.get("telegram_alert_name", ""))),
            "min_alert_severity": data.get("min_alert_severity", data.get("telegram.min_alert_severity", data.get("telegram_alert_min_severity", "WARNING"))),
            "alert_cooldown_sec": data.get("alert_cooldown_sec", data.get("telegram.alert_cooldown_sec", data.get("telegram_alert_cooldown_sec", 300))),
            "telegram_alerts_enabled": bool(data.get("telegram_alerts_enabled", data.get("alerts_enabled", data.get("telegram.alerts_enabled", False)))),
            "telegram_alert_chat_id": data.get("telegram_alert_chat_id", data.get("alerts_chat_id", data.get("telegram.alerts_chat_id"))),
            "telegram_alert_name": data.get("telegram_alert_name", data.get("alerts_chat_name", data.get("telegram.alerts_chat_name", ""))),
            "telegram_alert_min_severity": data.get("telegram_alert_min_severity", data.get("min_alert_severity", data.get("telegram.min_alert_severity", "WARNING"))),
            "telegram_alert_cooldown_sec": data.get("telegram_alert_cooldown_sec", data.get("alert_cooldown_sec", data.get("telegram.alert_cooldown_sec", 300))),
            "alert_dedup_enabled": bool(data.get("alert_dedup_enabled", data.get("telegram.alert_dedup_enabled", True))),
            "alert_format_rich": bool(data.get("alert_format_rich", data.get("telegram.alert_format_rich", True))),
            "alert_aggregation_enabled": bool(data.get("alert_aggregation_enabled", True)),
            "alert_aggregation_threshold": int(data.get("alert_aggregation_threshold", 5) or 5),
            "alert_aggregation_window_sec": int(data.get("alert_aggregation_window_sec", 10) or 10),
        }

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
        alert_type = str(alert.get("type", "") or "").lower()

        chat_name = str(settings.get("telegram_alert_name") or settings.get("alerts_chat_name", "") or "").strip()
        timestamp = str(alert.get("timestamp") or alert.get("ts") or "").strip()

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
            f"{icon} Pickfair Alert",
            f"• Severity: {severity}",
            f"• Code: {code}",
            f"• Title: {title}",
            f"• Source: {source}",
        ]

        if lifecycle:
            lines.append(f"Lifecycle: {lifecycle}")

        if chat_name:
            lines.append(f"• Channel: {chat_name}")
        if timestamp:
            lines.append(f"• Time: {timestamp}")

        if description:
            lines.append(f"• Description: {description}")

        if alert_type == "anomaly":
            lines.append("• Category: ANOMALY")

        if details:
            try:
                from observability.sanitizers import sanitize_value as _san
                safe_details = _sanitize_telegram_payload(_san(dict(details)))
            except Exception:
                safe_details = {"details": "***REDACTED_ERROR***"}
            safe_details = _sanitize_telegram_payload(safe_details)
            if bool(settings.get("alert_format_rich", True)):
                rendered = ", ".join(f"{k}={v}" for k, v in sorted(safe_details.items()))
                lines.append(f"• Details: {rendered}")
            else:
                lines.append(f"• Details: {safe_details}")

        incident_class = details.get("incident_class") if isinstance(details, dict) else None
        normalized = details.get("normalized_severity") if isinstance(details, dict) else None
        why_it_matters = details.get("why_it_matters") if isinstance(details, dict) else None
        recommended = details.get("recommended_action") if isinstance(details, dict) else None
        resolution_reason = details.get("resolution_reason") if isinstance(details, dict) else None
        if incident_class:
            lines.append(f"• Incident Class: {incident_class}")
        if normalized:
            lines.append(f"• Normalized Severity: {str(normalized).upper()}")
        if why_it_matters:
            lines.append(f"• Why it matters: {why_it_matters}")
        if recommended:
            lines.append(f"• Recommended action: {recommended}")
        suggested_action = alert.get("suggested_action") or (details.get("suggested_action") if isinstance(details, dict) else None)
        if suggested_action:
            lines.append(f"• Suggested action: {suggested_action}")
        if resolution_reason and lifecycle == "RESOLVED":
            lines.append(f"• Resolution: {resolution_reason}")

        return "\n".join(lines)

    def _dedup_key(self, alert: Dict[str, Any]) -> str:
        code = str(alert.get("code", "UNKNOWN_ALERT") or "UNKNOWN_ALERT")
        severity = str(alert.get("severity", "WARNING") or "WARNING").upper()
        message = str(alert.get("message") or alert.get("title") or code)
        source = str(alert.get("source", "system") or "system")
        return f"{source}|{code}|{severity}|{message}"

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
