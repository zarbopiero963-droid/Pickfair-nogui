from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from telegram_listener import TelegramListener

logger = logging.getLogger(__name__)


class TelegramService:
    def __init__(self, settings_service, db, bus):
        self.settings_service = settings_service
        self.db = db
        self.bus = bus
        self.listener: Optional[TelegramListener] = None
        self.connected = False
        self.last_error = ""

    def _handle_signal(self, signal: dict) -> None:
        signal = dict(signal or {})
        signal["received_at"] = datetime.utcnow().isoformat()

        if hasattr(self.db, "save_received_signal"):
            try:
                self.db.save_received_signal(signal)
            except Exception as exc:
                logger.warning("save_received_signal fallita: %s", exc)

        self.bus.publish("SIGNAL_RECEIVED", signal)

    def _handle_status(self, status_message) -> None:
        self.bus.publish("TELEGRAM_STATUS", {"message": str(status_message or "")})

    def start(self) -> dict:
        cfg = self.settings_service.load_telegram_config()
        if not cfg.enabled:
            return {"started": False, "reason": "telegram_disabled"}

        if not cfg.api_id or not cfg.api_hash:
            raise RuntimeError("Configurazione Telegram incompleta")

        self.listener = TelegramListener(
            api_id=int(cfg.api_id),
            api_hash=cfg.api_hash,
            session_string=cfg.session_string or None,
        )
        self.listener.set_database(self.db)
        self.listener.set_monitored_chats(cfg.monitored_chat_ids)
        self.listener.set_callbacks(
            on_signal=self._handle_signal,
            on_status=self._handle_status,
        )
        self.listener.start()
        self.connected = True
        self.last_error = ""

        return {
            "started": True,
            "chat_count": len(cfg.monitored_chat_ids),
        }

    def stop(self) -> None:
        if self.listener:
            try:
                self.listener.stop()
            except Exception as exc:
                logger.warning("Errore stop Telegram listener: %s", exc)
        self.listener = None
        self.connected = False

    def status(self) -> dict:
        running = bool(self.listener and getattr(self.listener, "running", False))
        return {
            "connected": self.connected and running,
            "running": running,
            "last_error": self.last_error,
        }
