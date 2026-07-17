"""Trasporto Telegram basato su **Bot API HTTP** (getUpdates long-poll).

PR-1 dell'epica «migrazione userbot -> Bot API» (issue #374). Sostituisce, sul
solo livello di TRASPORTO, il client Telethon userbot (api_id/api_hash + session
string) con l'HTTP Bot API: serve **solo** un `bot_token` (da BotFather) e la
lista dei `chat_id` da ascoltare (chat proprie in cui il bot e' admin/membro).

Confine di responsabilita': questo modulo si occupa ESCLUSIVAMENTE di ricevere i
messaggi da Telegram e consegnarli, tramite una callback, al contratto gia'
esistente `TelegramListener.handle_incoming(text, chat_id, message_date)` — da
li' in giu' (parser, bus, trading) tutto resta invariato e riusato.

Caratteristiche PR-1:
- `getUpdates` long-poll con gestione dell'`offset` (ack dei messaggi consumati).
- Fetch HTTP **iniettabile** (default via `urllib`), cosi' i test non toccano la
  rete e sono deterministici.
- **Fail-open** sul singolo update malformato: viene saltato ma l'offset avanza
  comunque (un update rotto non deve incastrare il loop).
- **Fail-safe** sul loop: un errore di rete non uccide il thread (backoff).
- **Fail-closed** sui messaggi: allow-list dei `chat_id` (difesa in profondita')
  e data del messaggio obbligatoria e valida (anti-replay), coerente con la
  guardia anti-stale di `handle_incoming`.
- **Nessun segreto nei log**: il `bot_token` (che compare nell'URL getUpdates)
  non viene MAI loggato; ogni testo d'errore e' redatto.

NON e' ancora agganciato al runtime di produzione: e' opt-in e inerte finche' una
PR successiva non lo seleziona come trasporto attivo. Il path userbot resta
invariato.
"""
from __future__ import annotations

import json
import logging
import threading
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional

logger = logging.getLogger(__name__)

# Tipo della callback di consegna: coincide con TelegramListener.handle_incoming.
OnMessage = Callable[[str, int, Optional[datetime]], Any]
# Tipo del fetcher iniettabile: dato l'offset, ritorna il payload getUpdates.
Fetch = Callable[[int], Any]

_DEFAULT_API_BASE = "https://api.telegram.org"


class BotApiPollError(Exception):
    """getUpdates ha risposto senza `ok: true`. Sollevata da poll_once cosi' che
    il loop (run) attivi il backoff invece di ripollare stretto (hot-loop)."""


class TelegramBotApiTransport:
    """Long-poll `getUpdates` di un singolo bot, con consegna a una callback."""

    def __init__(
        self,
        bot_token: str,
        chat_ids: Iterable[int],
        on_message: OnMessage,
        *,
        fetch: Optional[Fetch] = None,
        api_base: str = _DEFAULT_API_BASE,
        long_poll_timeout: int = 25,
        base_backoff_sec: float = 1.0,
        max_backoff_sec: float = 30.0,
    ) -> None:
        if not bot_token or not isinstance(bot_token, str):
            raise ValueError("bot_token obbligatorio")
        self._bot_token = bot_token
        # Allow-list dei chat_id (difesa in profondita', come il filtro Telethon).
        # OBBLIGATORIA e NON VUOTA (fail-closed): senza allow-list il trasporto
        # consegnerebbe segnali da qualunque chat al pipeline di trading.
        self.chat_ids = {int(c) for c in (chat_ids or [])}
        if not self.chat_ids:
            raise ValueError("chat_ids (allow-list) obbligatoria e non vuota")
        # Solo schemi http(s): urlopen aprirebbe anche file://, ftp:// (l'URL e'
        # costruito da api_base -> vincolo esplicito, fail-closed).
        if not isinstance(api_base, str) or not api_base.startswith(("http://", "https://")):
            raise ValueError("api_base deve usare schema http:// o https://")
        self._on_message = on_message
        self._fetch: Fetch = fetch if fetch is not None else self._default_fetch
        self._api_base = api_base.rstrip("/")
        self._long_poll_timeout = int(long_poll_timeout)
        self._base_backoff = float(base_backoff_sec)
        self._max_backoff = float(max_backoff_sec)

        self._offset = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Redazione segreti
    # ------------------------------------------------------------------
    def _redact(self, text: Any) -> str:
        """Rimuove il bot_token da qualunque stringa destinata ai log (l'URL
        getUpdates lo contiene). Fail-safe: non solleva mai."""
        out = str(text)
        try:
            if self._bot_token and self._bot_token in out:
                out = out.replace(self._bot_token, "[REDACTED]")
        except Exception:  # pragma: no cover - la redazione non deve rompere il log
            return "[REDACTION_ERROR]"
        return out

    # ------------------------------------------------------------------
    # Estrazione / dispatch (puri, testabili senza rete ne' thread)
    # ------------------------------------------------------------------
    @staticmethod
    def _extract(update: Any) -> Optional[tuple[str, int, datetime]]:
        """Da un update Bot API estrae (text, chat_id, message_date).

        Ritorna None (skip fail-open) se l'update non e' un messaggio testuale
        utilizzabile. Gestisce sia `message` (gruppi) sia `channel_post` (canali
        dove il bot e' admin). La **data e' obbligatoria e valida** (anti-replay
        fail-closed): un messaggio senza data confrontabile viene scartato.
        """
        if not isinstance(update, dict):
            return None
        msg = update.get("message")
        if not isinstance(msg, dict):
            msg = update.get("channel_post")
        if not isinstance(msg, dict):
            return None
        text = msg.get("text")
        if not isinstance(text, str) or not text:
            # Media con didascalia: usa la caption come testo.
            caption = msg.get("caption")
            text = caption if isinstance(caption, str) and caption else None
        if not text:
            return None
        chat = msg.get("chat")
        if not isinstance(chat, dict):
            return None
        chat_id = chat.get("id")
        if not isinstance(chat_id, int) or isinstance(chat_id, bool):
            return None
        date_raw = msg.get("date")
        if not isinstance(date_raw, (int, float)) or isinstance(date_raw, bool):
            return None
        try:
            message_date = datetime.fromtimestamp(float(date_raw), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
        return text, chat_id, message_date

    def _dispatch(self, update: Any) -> None:
        """Applica allow-list e consegna alla callback (fail-open)."""
        extracted = self._extract(update)
        if extracted is None:
            return
        text, chat_id, message_date = extracted
        # Fail-closed: allow-list VUOTA => scarta tutto (una config mancante non
        # deve consegnare segnali da qualunque chat al parser/trading).
        if not self.chat_ids or chat_id not in self.chat_ids:
            logger.warning(
                "[TelegramBotTransport] messaggio da chat non autorizzata %s scartato "
                "(allow-list %s)",
                chat_id,
                "vuota" if not self.chat_ids else "attiva",
            )
            return
        try:
            self._on_message(text, chat_id, message_date)
        except Exception:
            logger.exception("[TelegramBotTransport] callback on_message fallita")

    # ------------------------------------------------------------------
    # Poll
    # ------------------------------------------------------------------
    def poll_once(self, offset: int) -> int:
        """Esegue una getUpdates a partire da `offset`, consegna i messaggi e
        ritorna il PROSSIMO offset. L'offset avanza oltre OGNI update consumato
        (anche malformato): un update rotto non deve reincastrare il loop."""
        payload = self._fetch(offset)
        if not isinstance(payload, dict) or not payload.get("ok"):
            # Solleva (non ritorna) cosi' run() applica il backoff: un ok=False
            # persistente NON deve degenerare in polling stretto verso Telegram.
            desc = (payload or {}).get("description") if isinstance(payload, dict) else payload
            raise BotApiPollError(self._redact(f"getUpdates non ok: {desc}"))
        results = payload.get("result")
        if not isinstance(results, list) or not results:
            return offset
        next_offset = offset
        for update in results:
            uid = update.get("update_id") if isinstance(update, dict) else None
            if isinstance(uid, int) and not isinstance(uid, bool):
                next_offset = max(next_offset, uid + 1)
            try:
                self._dispatch(update)
            except Exception:  # pragma: no cover - _dispatch e' gia' fail-open
                logger.exception("[TelegramBotTransport] dispatch update fallito")
        return next_offset

    def run(self) -> None:
        """Loop long-poll fail-safe fino a stop(). Backoff esponenziale sugli
        errori di rete, reset sul primo successo."""
        backoff = self._base_backoff
        while not self._stop.is_set():
            try:
                self._offset = self.poll_once(self._offset)
                backoff = self._base_backoff
            except Exception as exc:
                logger.error(
                    "[TelegramBotTransport] poll fallito (%s); backoff %.1fs",
                    self._redact(type(exc).__name__),
                    backoff,
                )
                self._stop.wait(backoff)
                backoff = min(backoff * 2.0, self._max_backoff)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self.run, name="telegram-bot-transport", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)

    # ------------------------------------------------------------------
    # Fetch HTTP di default (produzione; i test iniettano un fake)
    # ------------------------------------------------------------------
    def _default_fetch(self, offset: int) -> Any:
        params = {
            "offset": offset,
            "timeout": self._long_poll_timeout,
            "allowed_updates": json.dumps(["message", "channel_post"]),
        }
        # ATTENZIONE: l'URL contiene il bot_token -> non va MAI loggato.
        url = f"{self._api_base}/bot{self._bot_token}/getUpdates?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=self._long_poll_timeout + 10) as resp:
            return json.loads(resp.read().decode("utf-8"))
