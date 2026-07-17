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
        # Allow-list chat come int (lo schema salva chat_id TEXT). getUpdates
        # restituisce chat.id NUMERICO: un chat_id non numerico (es. @canale) non
        # è ascoltabile per questa via -> scartato (non fa crashare start()).
        # Fail-closed: senza almeno una chat numerica valida il transport non
        # saprebbe cosa ascoltare.
        ids: List[int] = []
        for c in chat_ids or []:
            s = str(c).strip()
            if not s:
                continue
            try:
                ids.append(int(s))
            except (TypeError, ValueError):
                continue
        if not ids:
            raise ValueError(
                "TelegramBotApiRuntime richiede almeno una chat_id numerica valida"
            )
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
        # Stato "base" (CREATED/CONNECTED/STOPPED). Lo `state` effettivo (property)
        # deriva anche dalla liveness reale del thread getUpdates: se il thread
        # muore in modo NON intenzionale, lo stato diventa FAILED (vedi property).
        self._state_base = "CREATED"

    # ---- superficie compatibile con TelegramService.self.listener ----
    @property
    def state(self) -> str:
        """Stato effettivo. Avviato ma con thread getUpdates morto (non stop
        intenzionale) => FAILED: così l'invariant guard ('CONNECTED richiede 1
        handler') e l'autoheal esistenti rilevano la perdita di ingestione,
        invece di restare 'CONNECTED' per sempre in silenzio."""
        if self._state_base != "CONNECTED":
            return self._state_base
        return "CONNECTED" if self._transport_alive() else "FAILED"

    @property
    def running(self) -> bool:
        return bool(self._started and self._transport_alive())

    @property
    def _runtime_thread(self):
        if self._transport is None:
            return None
        return getattr(self._transport, "_thread", None)

    def _thread_dead_unexpectedly(self) -> bool:
        return self._state_base == "CONNECTED" and not self._transport_alive()

    def handle_incoming(self, *args, **kwargs):
        # Delega al sink (usato come on_message del transport, ma esposto anche
        # qui per compatibilità con eventuali chiamanti che usano il listener).
        return self._sink.handle_incoming(*args, **kwargs)

    def _transport_alive(self) -> bool:
        t = self._transport
        if t is None:
            return False
        thread = getattr(t, "_thread", None)
        if thread is not None:
            return bool(thread.is_alive())
        # Transport senza thread introspezionabile (es. fake nei test): usa il
        # flag `stopped` se esposto (liveness reale), altrimenti considera vivo
        # se avviato.
        if hasattr(t, "stopped"):
            return not bool(getattr(t, "stopped"))
        return self._started

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
        # Avvio del transport fail-safe: se costruzione/start sollevano, NON si
        # dichiara CONNECTED (contratto duck-typed come TelegramListener.start,
        # che ritorna un esito invece di propagare).
        try:
            self._transport = self._build_transport()
            self._transport.start()
        except Exception as exc:
            self._started = False
            self._state_base = "FAILED"
            return {"started": False, "error": str(exc)}
        self._started = True
        self._state_base = "CONNECTED"
        return {"started": True}

    def stop(self) -> dict:
        self.intentional_stop = True
        if self._transport is not None:
            try:
                self._transport.stop()
            except Exception as exc:  # pragma: no cover - stop best-effort
                # Stop sollevato: se il thread è ancora vivo => fail-closed
                # (il service NON deve staccare il listener).
                if self._transport_alive():
                    return {"stopped": False, "error": f"bot_transport_stop_failed: {exc}"}
            # Contratto fail-closed come il path Telethon: se il thread del
            # transport è ancora vivo dopo lo stop, NON dichiarare stopped —
            # altrimenti il service stacca il listener e un restart creerebbe un
            # SECONDO getUpdates sullo stesso token (Telegram 409 Conflict).
            if self._transport_alive():
                return {"stopped": False, "error": "bot_transport_thread_still_alive"}
        self._started = False
        self._state_base = "STOPPED"
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
            # Thread getUpdates morto in modo non intenzionale => errore esplicito
            # (l'autoheal/invariant guard vedono FAILED + last_error, non un
            # 'CONNECTED' muto).
            "last_error": "bot_transport_thread_dead" if self._thread_dead_unexpectedly() else "",
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
