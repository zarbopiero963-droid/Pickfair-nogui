from __future__ import annotations

import asyncio
import logging
import re
import threading
from typing import Any, Callable, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


class TelegramListener:
    """
    Telegram listener headless.

    Responsabilità:
    - connessione Telethon
    - ascolto nuovi messaggi
    - filtro chat monitorate
    - parsing minimo del segnale
    - callback verso il resto del sistema

    Callback supportate:
    - on_signal(signal_dict)
    - on_message(message_dict)
    - on_status(status, message)

    Note:
    - non contiene logica di trading
    - non contiene GUI
    - non dipende dal runtime controller
    """

    def __init__(
        self,
        *,
        api_id: int,
        api_hash: str,
        session_string: Optional[str] = None,
    ):
        self.api_id = int(api_id)
        self.api_hash = str(api_hash or "").strip()
        self.session_string = session_string

        self.running = False
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._client = None

        self._db = None
        self._monitored_chats: set[int] = set()

        self._on_signal: Optional[Callable[[Dict[str, Any]], None]] = None
        self._on_message: Optional[Callable[[Dict[str, Any]], None]] = None
        self._on_status: Optional[Callable[..., None]] = None

    # =========================================================
    # PUBLIC CONFIG
    # =========================================================
    def set_database(self, db) -> None:
        self._db = db

    def set_monitored_chats(self, chats: Iterable[Any]) -> None:
        monitored: set[int] = set()
        for item in chats or []:
            try:
                monitored.add(int(item))
            except Exception:
                logger.warning("Chat id non valido ignorato: %s", item)
        self._monitored_chats = monitored

    def set_callbacks(
        self,
        *,
        on_signal: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_message: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_status: Optional[Callable[..., None]] = None,
    ) -> None:
        self._on_signal = on_signal
        self._on_message = on_message
        self._on_status = on_status

    # =========================================================
    # STATUS
    # =========================================================
    def _emit_status(self, status: str, message: str) -> None:
        logger.info("[TelegramListener] %s - %s", status, message)
        cb = self._on_status
        if not cb:
            return
        try:
            cb(status, message)
        except TypeError:
            try:
                cb(message)
            except Exception:
                logger.exception("Errore callback on_status")
        except Exception:
            logger.exception("Errore callback on_status")

    def _emit_signal(self, signal: Dict[str, Any]) -> None:
        cb = self._on_signal
        if not cb:
            return
        try:
            cb(signal)
        except Exception:
            logger.exception("Errore callback on_signal")

    def _emit_message(self, message: Dict[str, Any]) -> None:
        cb = self._on_message
        if not cb:
            return
        try:
            cb(message)
        except Exception:
            logger.exception("Errore callback on_message")

    # =========================================================
    # LIFECYCLE
    # =========================================================
    def start(self) -> None:
        if self.running:
            return

        self.running = True
        self._thread = threading.Thread(
            target=self._run_thread,
            daemon=True,
            name="TelegramListenerThread",
        )
        self._thread.start()

    def stop(self) -> None:
        self.running = False

        loop = self._loop
        if loop and loop.is_running():
            try:
                loop.call_soon_threadsafe(lambda: None)
            except Exception:
                pass

            client = self._client
            if client is not None:
                try:
                    asyncio.run_coroutine_threadsafe(client.disconnect(), loop)
                except Exception:
                    pass

            try:
                loop.call_soon_threadsafe(loop.stop)
            except Exception:
                pass

        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=5.0)

        self._thread = None
        self._loop = None
        self._client = None

    def _run_thread(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        try:
            self._loop.run_until_complete(self._main())
        except Exception as exc:
            logger.exception("Errore thread Telegram listener: %s", exc)
            self._emit_status("ERROR", str(exc))
        finally:
            try:
                pending = asyncio.all_tasks(self._loop)
                for task in pending:
                    task.cancel()
            except Exception:
                pass

            try:
                self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            except Exception:
                pass

            try:
                self._loop.close()
            except Exception:
                pass

            self.running = False

    async def _main(self) -> None:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        from telethon import events

        if self.session_string:
            session = StringSession(self.session_string)
        else:
            session = "pickfair_telegram_session"

        client = TelegramClient(session, self.api_id, self.api_hash)
        self._client = client

        await client.connect()

        if not await client.is_user_authorized():
            await client.disconnect()
            raise RuntimeError("Sessione Telegram non autorizzata")

        @client.on(events.NewMessage)
        async def _handler(event):
            try:
                await self._handle_event(event)
            except Exception:
                logger.exception("Errore gestione messaggio Telegram")

        self._emit_status("LISTENING", "Listener Telegram avviato")
        await client.run_until_disconnected()

    # =========================================================
    # EVENT HANDLING
    # =========================================================
    async def _handle_event(self, event) -> None:
        if not self.running:
            return

        try:
            chat_id = int(event.chat_id)
        except Exception:
            chat_id = None

        if self._monitored_chats and chat_id not in self._monitored_chats:
            return

        raw_text = self._extract_text(event)
        if not raw_text:
            return

        message_payload = {
            "chat_id": chat_id,
            "message_id": getattr(event, "id", None),
            "text": raw_text,
            "raw_text": raw_text,
            "simulation_mode": False,
        }
        self._emit_message(message_payload)

        signal = self._parse_signal(raw_text)
        if not signal:
            return

        signal["chat_id"] = chat_id
        signal["message_id"] = getattr(event, "id", None)
        signal["raw_text"] = raw_text
        signal["simulation_mode"] = bool(signal.get("simulation_mode", False))

        self._emit_signal(signal)

    def _extract_text(self, event) -> str:
        text = getattr(event, "raw_text", None) or getattr(event, "text", None) or ""
        text = str(text).strip()
        return text

    # =========================================================
    # SIGNAL PARSING
    # =========================================================
    def _parse_signal(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Parser minimo robusto.

        Supporta:
        - market_id / selection_id se presenti nel testo
        - BACK / LAY
        - quota
        - stake opzionale
        - event / selection opzionali
        """
        normalized = " ".join(str(text or "").split())
        if not normalized:
            return None

        market_id = self._extract_market_id(normalized)
        selection_id = self._extract_selection_id(normalized)
        action = self._extract_action(normalized)
        price = self._extract_price(normalized)
        stake = self._extract_stake(normalized)
        selection_name = self._extract_selection_name(normalized)
        event_name = self._extract_event_name(normalized)

        # minimo indispensabile per inoltrare al processor
        if market_id is None or selection_id is None or price is None:
            return None

        return {
            "market_id": market_id,
            "selection_id": selection_id,
            "action": action,
            "price": price,
            "stake": stake if stake is not None else 1.0,
            "selection": selection_name or "",
            "event": event_name or "",
            "source": "TELEGRAM_LISTENER",
        }

    def _extract_market_id(self, text: str) -> Optional[str]:
        patterns = [
            r"\b(1\.\d{6,})\b",
            r"market[_\s-]*id[:=\s]+(1\.\d{6,})",
            r"\bmarketId[:=\s]+(1\.\d{6,})\b",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                return str(m.group(1)).strip()
        return None

    def _extract_selection_id(self, text: str) -> Optional[int]:
        patterns = [
            r"\bselection[_\s-]*id[:=\s]+(\d+)\b",
            r"\bselectionId[:=\s]+(\d+)\b",
            r"\bsel[_\s-]*id[:=\s]+(\d+)\b",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                try:
                    return int(m.group(1))
                except Exception:
                    return None
        return None

    def _extract_action(self, text: str) -> str:
        m = re.search(r"\b(BACK|LAY)\b", text, flags=re.IGNORECASE)
        if m:
            return str(m.group(1)).upper()
        return "BACK"

    def _extract_price(self, text: str) -> Optional[float]:
        patterns = [
            r"\bquota[:=\s]+(\d+(?:[.,]\d+)?)\b",
            r"\bodds[:=\s]+(\d+(?:[.,]\d+)?)\b",
            r"@(\d+(?:[.,]\d+)?)",
            r"\bprice[:=\s]+(\d+(?:[.,]\d+)?)\b",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                try:
                    return float(str(m.group(1)).replace(",", "."))
                except Exception:
                    return None
        return None

    def _extract_stake(self, text: str) -> Optional[float]:
        patterns = [
            r"\bstake[:=\s]+(\d+(?:[.,]\d+)?)\b",
            r"\bsize[:=\s]+(\d+(?:[.,]\d+)?)\b",
            r"\bimporto[:=\s]+(\d+(?:[.,]\d+)?)\b",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                try:
                    return float(str(m.group(1)).replace(",", "."))
                except Exception:
                    return None
        return None

    def _extract_selection_name(self, text: str) -> str:
        patterns = [
            r"\bselection[:=\s]+([^\|,;]+)",
            r"\brunner[_\s-]*name[:=\s]+([^\|,;]+)",
            r"\bteam[:=\s]+([^\|,;]+)",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                return str(m.group(1)).strip()
        return ""

    def _extract_event_name(self, text: str) -> str:
        patterns = [
            r"\bevent[:=\s]+([^\|;]+)",
            r"\bmatch[:=\s]+([^\|;]+)",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                return str(m.group(1)).strip()
        return ""