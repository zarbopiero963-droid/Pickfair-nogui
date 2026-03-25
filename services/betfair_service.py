from __future__ import annotations

import logging
from typing import Optional

from betfair_client import BetfairClient

logger = logging.getLogger(__name__)


class BetfairService:
    def __init__(self, settings_service):
        self.settings_service = settings_service
        self.client: Optional[BetfairClient] = None
        self.connected = False
        self.last_error = ""

    def connect(self, password: str | None = None, force: bool = False) -> dict:
        if self.connected and self.client and not force:
            return {"connected": True, "reason": "already_connected"}

        cfg = self.settings_service.load_betfair_config()
        if not cfg.username or not cfg.app_key or not cfg.certificate or not cfg.private_key:
            raise RuntimeError("Configurazione Betfair incompleta")

        password = password if password is not None else self.settings_service.load_password()
        if not password:
            raise RuntimeError("Password Betfair mancante")

        client = BetfairClient(
            username=cfg.username,
            app_key=cfg.app_key,
            cert_pem=cfg.certificate,
            key_pem=cfg.private_key,
        )

        session_info = client.login(password=password)
        self.client = client
        self.connected = True
        self.last_error = ""

        if hasattr(self.settings_service.db, "save_session"):
            self.settings_service.db.save_session(
                session_info.get("session_token"),
                session_info.get("expiry"),
            )

        return {
            "connected": True,
            "session": session_info,
        }

    def disconnect(self) -> None:
        if self.client:
            try:
                self.client.logout()
            except Exception as exc:
                logger.warning("Errore logout Betfair: %s", exc)
        self.client = None
        self.connected = False

        if hasattr(self.settings_service.db, "clear_session"):
            try:
                self.settings_service.db.clear_session()
            except Exception:
                pass

    def get_client(self) -> Optional[BetfairClient]:
        return self.client

    def get_account_funds(self) -> dict:
        if not self.client:
            return {"available": 0.0, "exposure": 0.0, "total": 0.0}
        return self.client.get_account_funds()

    def status(self) -> dict:
        return {
            "connected": self.connected and self.client is not None,
            "last_error": self.last_error,
            "has_client": self.client is not None,
        }
