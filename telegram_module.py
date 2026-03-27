from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class TelegramModule:
    """
    Mixin Telegram completo.

    Risolve:
    - wiring listener → handler
    - salvataggio DB
    - invio REQ_QUICK_BET
    """

    # =========================================================
    # START LISTENER
    # =========================================================
    def _start_telegram_listener(self):
        settings = self.db.get_telegram_settings()

        if not settings or not settings.get("api_id") or not settings.get("api_hash"):
            return

        try:
            from telegram_listener import TelegramListener

            self.telegram_listener = TelegramListener(
                api_id=int(settings["api_id"]),
                api_hash=settings["api_hash"].strip(),
                session_string=settings.get("session_string"),
                db=self.db,
            )

            # 🔥 QUI STA IL FIX VERO
            self.telegram_listener.set_callbacks(
                on_signal=self._handle_telegram_signal,  # ✅ NON più publish diretto
                on_status=self._update_telegram_status,
            )

            chats = self.db.get_telegram_chats() or []
            active_chats = [c["chat_id"] for c in chats if c.get("is_active")]

            self.telegram_listener.start(monitored_chats=active_chats)

        except Exception:
            logger.exception("Errore start telegram listener")

    # =========================================================
    # STOP
    # =========================================================
    def _stop_telegram_listener(self):
        try:
            if getattr(self, "telegram_listener", None):
                self.telegram_listener.stop()
        except Exception:
            logger.exception("Errore stop telegram listener")

    # =========================================================
    # HANDLE SEGNALE (CUORE DEL SISTEMA)
    # =========================================================
    def _handle_telegram_signal(self, signal: dict):
        """
        Questo è IL punto critico:
        da qui parte tutto verso il trading.
        """

        try:
            if not isinstance(signal, dict):
                return

            # =========================================================
            # SALVA SU DB (fix corretto)
            # =========================================================
            try:
                if hasattr(self.db, "save_received_signal"):
                    self.db.save_received_signal(signal)
            except Exception:
                logger.exception("Errore salvataggio segnale DB")

            # =========================================================
            # VALIDAZIONE BASE
            # =========================================================
            if not signal.get("market_id") or not signal.get("selection_id"):
                logger.warning("Segnale incompleto: %s", signal)
                return

            # =========================================================
            # NORMALIZZAZIONE
            # =========================================================
            payload = {
                "market_id": str(signal.get("market_id")),
                "selection_id": int(signal.get("selection_id")),
                "bet_type": str(signal.get("bet_type", "BACK")).upper(),
                "price": float(signal.get("price") or 0.0),
                "stake": float(signal.get("stake") or 2.0),
                "event_name": signal.get("event_name", ""),
                "market_name": signal.get("market_name", ""),
                "runner_name": signal.get("selection_name", ""),
                "source": "telegram",
            }

            # =========================================================
            # INVIO AL RUNTIME
            # =========================================================
            self.bus.publish("SIGNAL_RECEIVED", payload)

        except Exception:
            logger.exception("Errore handle telegram signal")

    # =========================================================
    # STATUS UI
    # =========================================================
    def _update_telegram_status(self, status: dict):
        try:
            self.bus.publish("TELEGRAM_STATUS", status)
        except Exception:
            logger.exception("Errore update telegram status")