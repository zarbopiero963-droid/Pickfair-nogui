"""Runtime adapter Bot API (epica #374 PR-5a — wiring runtime multi-bot, step 1).

Presenta la superficie-listener attesa da `TelegramService` (`state`, `running`,
`_runtime_thread`, `set_*`, `start`/`stop`/`status`/`runtime_snapshot`) ma
alimentata da un `TelegramBotApiTransport` (HTTP getUpdates) invece del client
Telethon: **nessun api_id/api_hash richiesto**. I messaggi ricevuti dal transport
passano per `TelegramListener.handle_incoming` (RIUSO integrale della pipeline:
allow-list, guardia anti-stale, parse, emit) verso gli stessi callback
`on_signal`/`on_status` del path Telethon. La semantica di parsing NON è
modificata: il listener è usato solo come SINK (mai avviato — nessun Telethon).

SOLO wiring di UN bot (PR-5a). Rimandati alle PR successive:
- orchestrazione multi-bot (N transport + fan-in);
- autoheal/health per-bot (qui il transport non ha un vero health-surface: la
  liveness deriva dal thread vivo e dall'ultimo messaggio processato dal sink);
- validazione avanzata dei chat_id (qui: coercizione a int per l'allow-list).

Il `bot_token` è un SEGRETO: mai loggato (il transport redige; qui non lo si
stampa mai). Isolato dal path Telethon esistente, che resta invariato.
"""
from __future__ import annotations

from typing import Any, Callable, List, Optional

from telegram_bot_transport import TelegramBotApiTransport
from telegram_listener import TelegramListener


class TelegramBotApiRuntime:
    """Adapter listener-compatibile che ascolta via Bot API (un bot)."""

    def __init__(
        self,
        *,
        bot_token: str,
        chat_ids: List[Any],
        db=None,
        on_signal: Optional[Callable] = None,
        on_status: Optional[Callable] = None,
        transport_factory: Optional[Callable[[str, List[int], Callable], Any]] = None,
        allow_insecure_http: bool = False,
        max_message_age_seconds: float = 300.0,
    ):
        # Allow-list chat come int (lo schema salva chat_id TEXT). Fail-closed:
        # senza almeno una chat il transport non saprebbe cosa ascoltare.
        ids: List[int] = []
        for c in chat_ids or []:
            s = str(c).strip()
            if not s:
                continue
            ids.append(int(s))
        if not ids:
            raise ValueError("TelegramBotApiRuntime richiede almeno una chat allow-list")
        self._chat_ids = ids

        # Listener come SINK di parsing: mai avviato (nessun Telethon). Riusa
        # handle_incoming (allow-list, anti-stale, parse, emit) e la liveness.
        self._sink = TelegramListener(
            api_id=0,
            api_hash="",
            bot_token=bot_token,
            max_message_age_seconds=max_message_age_seconds,
        )
        self._sink.set_database(db)
        self._sink.set_monitored_chats(ids)
        self._sink.set_callbacks(on_signal=on_signal, on_status=on_status)

        self._bot_token = bot_token
        self._allow_insecure_http = bool(allow_insecure_http)
        self._transport_factory = transport_factory
        self._transport: Any = None
        self._started = False
        self.intentional_stop = False
        self.state = "CREATED"

    # ---- superficie compatibile con TelegramService.self.listener ----
    @property
    def running(self) -> bool:
        return bool(self._started and self._transport_alive())

    @property
    def _runtime_thread(self):
        return getattr(self._transport, "_thread", None)

    def handle_incoming(self, *args, **kwargs):
        # Delega al sink (usato come on_message del transport, ma esposto anche
        # qui per compatibilità con eventuali chiamanti che usano il listener).
        return self._sink.handle_incoming(*args, **kwargs)

    def _transport_alive(self) -> bool:
        t = self._transport
        if t is None:
            return False
        thread = getattr(t, "_thread", None)
        if thread is None:
            # Transport senza thread introspezionabile (es. fake nei test):
            # considerato vivo se è stato avviato e non fermato.
            return self._started
        return bool(thread.is_alive())

    def _build_transport(self):
        on_message = self._sink.handle_incoming
        if self._transport_factory is not None:
            return self._transport_factory(self._bot_token, self._chat_ids, on_message)
        return TelegramBotApiTransport(
            self._bot_token,
            self._chat_ids,
            on_message=on_message,
            allow_insecure_http=self._allow_insecure_http,
        )

    def start(self) -> dict:
        if self._started and self._transport_alive():
            return {"started": True, "reason": "already_running"}
        self.intentional_stop = False
        self._transport = self._build_transport()
        self._transport.start()
        self._started = True
        self.state = "CONNECTED"
        return {"started": True}

    def stop(self) -> dict:
        self.intentional_stop = True
        if self._transport is not None:
            try:
                self._transport.stop()
            except Exception as exc:  # pragma: no cover - stop best-effort
                self._started = False
                self.state = "STOPPED"
                return {"stopped": True, "warning": str(exc)}
        self._started = False
        self.state = "STOPPED"
        return {"stopped": True}

    def status(self) -> dict:
        inner = self._sink.status()
        alive = self.running
        return {
            "state": self.state,
            "running": alive,
            "intentional_stop": bool(self.intentional_stop),
            "reconnect_attempts": 0,
            "reconnect_in_progress": False,
            "last_error": "",
            "last_successful_message_ts": inner.get("last_successful_message_ts"),
            "listener_started": bool(self._started),
            # UN transport attivo = UN handler: soddisfa l'invariant guard
            # ("CONNECTED richiede esattamente 1 handler").
            "handlers_registered": 1 if alive else 0,
            "active_network_resources": 1 if alive else 0,
            "monitored_chat_count": len(self._chat_ids),
        }

    def runtime_snapshot(self) -> dict:
        st = self.status()
        return {
            "state": st["state"],
            "running": st["running"],
            "listener_started": st["listener_started"],
            "client_alive": st["running"],
            "handlers_registered": st["handlers_registered"],
            "reconnect_in_progress": False,
            "reconnect_attempts": 0,
            "active_network_resources": st["active_network_resources"],
            "intentional_stop": st["intentional_stop"],
            "retry_loop_active": False,
            "last_error": st["last_error"],
            "last_successful_message_ts": st["last_successful_message_ts"],
        }
