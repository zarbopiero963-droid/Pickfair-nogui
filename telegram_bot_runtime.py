"""Runtime adapter Bot API (epica #374 PR-5a — wiring runtime multi-bot, step 1).

Presenta la superficie-listener attesa da `TelegramService` (`state`, `running`,
`_runtime_thread`, `set_*`, `start`/`stop`/`status`/`runtime_snapshot`) ma
alimentata da un `TelegramBotApiTransport` (HTTP getUpdates) invece del client
Telethon: **nessun api_id/api_hash richiesto**. I messaggi ricevuti dal transport
passano per `TelegramListener.handle_incoming` (RIUSO integrale della pipeline:
allow-list, guardia anti-stale, parse, emit) verso gli stessi callback
`on_signal`/`on_status` del path Telethon. La semantica di parsing NON è
modificata: il listener è usato solo come SINK (mai avviato — nessun Telethon).

PR-5a wira UN bot (`TelegramBotApiRuntime`). PR-5b aggiunge
`TelegramMultiBotRuntime`: orchestrazione N-bot (N transport indipendenti +
fan-in) con la stessa superficie listener-compatibile e semantica AGGREGATA
fail-closed. Rimandati alle PR successive:
- autoheal/health PER-BOT (PR-5c): oggi l'aggregato è fail-closed (se un bot
  degrada l'intero runtime va FAILED => autoheal service-level restart-all);
- validazione avanzata dei chat_id / risoluzione `@username` (qui: coercizione a
  int per l'allow-list; un bot con sole chat non numeriche è non-usable).

Il `bot_token` è un SEGRETO: mai loggato (il transport redige; qui non lo si
stampa mai). Isolato dal path Telethon esistente, che resta invariato.
"""
from __future__ import annotations

from typing import Any, Callable, List, Optional

from telegram_bot_transport import TelegramBotApiTransport
from telegram_listener import TelegramListener


class TelegramBotApiRuntime:
    """Adapter listener-compatibile che ascolta via Bot API (un bot).

    Concorrenza: come il path Telethon (`TelegramListener`), i metodi di lifecycle
    (`start`/`stop`) sono invocati dal lifecycle SERIALIZZATO del `TelegramService`
    (start al boot; `restart` fa stop→start in sequenza; il watchdog chiama
    `run_autoheal_once`→`restart`) — non in parallelo. Non si introduce quindi un
    lock qui; una sincronizzazione esplicita sarà valutata solo se/quando il
    runtime multi-bot renderà i lifecycle concorrenti (rimandato alla PR N-bot).
    """

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

    # Soglia di fallimenti getUpdates consecutivi oltre la quale il transport è
    # considerato in ingestione morta (thread vivo ma nessun poll riuscito, es.
    # bot_token 401 o 409 Conflict permanente). ~5 poll con backoff esponenziale
    # ≈ decine di secondi di fallimento continuo prima di degradare.
    _MAX_CONSECUTIVE_FAILURES = 5

    # ---- superficie compatibile con TelegramService.self.listener ----
    def _health_read(self) -> tuple:
        """Ritorna (alive, degraded) da UNA lettura ATOMICA del transport quando
        disponibile (`health_snapshot()` sotto lock del transport → la coppia
        thread-vivo / fallimenti-consecutivi è coerente, niente stato torn
        cross-thread). Fallback a letture separate per transport senza il metodo
        (fake nei test). `degraded` = thread vivo ma troppi getUpdates falliti
        consecutivi (ingestione morta → fail-open da chiudere)."""
        t = self._transport
        if t is None:
            return False, False
        snap = getattr(t, "health_snapshot", None)
        if callable(snap):
            alive, failures = snap()
        else:  # pragma: no cover - solo fake di test senza health_snapshot
            alive = self._transport_alive()
            try:
                failures = int(getattr(t, "_consecutive_failures", 0) or 0)
            except (TypeError, ValueError):
                failures = 0
        degraded = bool(
            self._state_base == "CONNECTED"
            and alive
            and int(failures) >= self._MAX_CONSECUTIVE_FAILURES
        )
        return bool(alive), degraded

    @property
    def state(self) -> str:
        """Stato effettivo. Avviato ma con thread getUpdates morto, o vivo ma in
        fallimento PERMANENTE (401/409) => FAILED: così l'invariant guard
        ('CONNECTED richiede 1 handler') e l'autoheal esistenti rilevano la perdita
        di ingestione invece di un 'CONNECTED' muto (fail-open). Derivato da una
        singola lettura atomica (`_health_read`) => coerente."""
        if self._state_base != "CONNECTED":
            return self._state_base
        alive, degraded = self._health_read()
        return "CONNECTED" if (alive and not degraded) else "FAILED"

    @property
    def running(self) -> bool:
        return bool(self._started and self._transport_alive())

    @property
    def _runtime_thread(self):
        if self._transport is None:
            return None
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
        # Non costruire un SECONDO transport se ne esiste già uno VIVO: sarebbe un
        # doppio getUpdates sullo stesso token (Telegram 409). Vale sia per un
        # runtime già avviato (already_running) sia per un transport ORFANO
        # rimasto vivo dopo uno start fallito — in tal caso fail-closed: il
        # chiamante deve fare stop() prima di ritentare (difesa dell'adapter,
        # oltre al guard previous_runtime_still_alive del service).
        if self._transport_alive():
            if self._started:
                return {"started": True, "reason": "already_running"}
            return {"started": False, "error": "bot_transport_orphan_alive"}
        self.intentional_stop = False
        # Avvio del transport fail-safe: se costruzione/start sollevano, NON si
        # dichiara CONNECTED (contratto duck-typed come TelegramListener.start,
        # che ritorna un esito invece di propagare).
        try:
            self._transport = self._build_transport()
            self._transport.start()
        except Exception as exc:
            # Il transport potrebbe aver GIA' avviato un thread prima di sollevare:
            # fermalo per non lasciare un getUpdates orfano (un retry ne aprirebbe
            # un secondo sullo stesso token -> Telegram 409 Conflict).
            try:
                if self._transport is not None:
                    self._transport.stop()
            except Exception:  # pragma: no cover - cleanup best-effort
                pass
            self._started = False
            self._state_base = "FAILED"
            # Thread MORTO => azzera il riferimento. Thread ANCORA VIVO => MANTIENILO:
            # così il guard del service (_runtime_thread) vede il thread e rifiuta
            # di avviare un SECONDO getUpdates sullo stesso token (409 Conflict);
            # il thread residuo verrà ritentato dallo stop fail-closed.
            if not self._transport_alive():
                self._transport = None
            # Simmetria col fail-closed di stop(): uno start fallito NON è uno stop
            # intenzionale (il valore è già False dall'inizio di start(); esplicito
            # per chiarezza), così il recovery non viene soppresso.
            self.intentional_stop = False
            # SOLO il tipo dell'eccezione: il messaggio potrebbe (in teoria)
            # contenere dati sensibili -> mai propagarlo grezzo dal path col
            # bot_token.
            return {"started": False, "error": type(exc).__name__}
        self._started = True
        self._state_base = "CONNECTED"
        return {"started": True}

    def stop(self) -> dict:
        self.intentional_stop = True
        if self._transport is not None:
            stop_warning = ""
            try:
                self._transport.stop()
            except Exception as exc:  # pragma: no cover - stop best-effort
                if self._transport_alive():
                    # Stop sollevato E thread ancora vivo => fail-closed: lo stato
                    # runtime segue il service (FAILED), il listener NON va staccato.
                    # `intentional_stop=False`: lo stop NON è avvenuto, quindi il
                    # runtime NON è intenzionalmente fermo (recovery non soppresso).
                    self.intentional_stop = False
                    self._state_base = "FAILED"
                    return {"stopped": False, "error": f"bot_transport_stop_failed:{type(exc).__name__}"}
                # thread già morto ma stop ha sollevato: preserva la diagnostica.
                stop_warning = type(exc).__name__
            # Contratto fail-closed come il path Telethon: se il thread del
            # transport è ancora vivo dopo lo stop, NON dichiarare stopped —
            # altrimenti il service stacca il listener e un restart creerebbe un
            # SECONDO getUpdates sullo stesso token (Telegram 409 Conflict).
            if self._transport_alive():
                self.intentional_stop = False
                self._state_base = "FAILED"
                return {"stopped": False, "error": "bot_transport_thread_still_alive"}
            # Stop riuscito: azzera stato E riferimento (nessun transport morto
            # appeso -> uno stop successivo è pulito, niente FAILED spurio).
            self._transport = None
            self._started = False
            self._state_base = "STOPPED"
            return {"stopped": True, "warning": stop_warning} if stop_warning else {"stopped": True}
        self._started = False
        self._state_base = "STOPPED"
        return {"stopped": True}

    def status(self) -> dict:
        inner = self._sink.status()
        # Snapshot ATOMICO: liveness e fallimenti da UNA lettura (`_health_read`,
        # coppia coerente sotto lock del transport). Da questi derivano TUTTI i
        # campi (state/running/handlers/last_error): mai incoerenti tra loro (es.
        # state="FAILED" con handlers=1), invariant guard "CONNECTED => 1 handler"
        # sempre rispettato.
        alive, degraded = self._health_read()
        if self._state_base != "CONNECTED":
            state = self._state_base
        elif not alive or degraded:
            state = "FAILED"
        else:
            state = "CONNECTED"
        # "Sano" = avviato, thread vivo E non in fallimento permanente. Solo un
        # transport sano conta come 1 handler (coerente con state CONNECTED).
        healthy = bool(self._started and alive and not degraded)
        running = bool(self._started and alive)
        if self._state_base == "CONNECTED" and not alive:
            last_error = "bot_transport_thread_dead"
        elif degraded:
            last_error = "bot_transport_persistent_poll_failure"
        else:
            last_error = ""
        return {
            "state": state,
            "running": running,
            "intentional_stop": bool(self.intentional_stop),
            "reconnect_attempts": 0,
            "reconnect_in_progress": False,
            "last_error": last_error,
            "last_successful_message_ts": inner.get("last_successful_message_ts"),
            "listener_started": bool(self._started),
            # UN transport SANO = UN handler: soddisfa l'invariant guard
            # ("CONNECTED richiede esattamente 1 handler"); degradato/morto => 0.
            "handlers_registered": 1 if healthy else 0,
            "active_network_resources": 1 if healthy else 0,
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
            # Un singolo bot ATTESO = 1 handler quando CONNECTED (invariant guard).
            "expected_handlers": 1,
            "reconnect_in_progress": False,
            "reconnect_attempts": 0,
            "active_network_resources": st["active_network_resources"],
            "intentional_stop": st["intentional_stop"],
            "retry_loop_active": False,
            "last_error": st["last_error"],
            "last_successful_message_ts": st["last_successful_message_ts"],
        }


def _child_thread_alive(runtime: Any) -> bool:
    """True se il runtime child ha un thread runtime VIVO. Fail-closed: se la
    verifica di liveness solleva, assume VIVO (non nascondere un orfano: il guard
    del service deve poterlo vedere e bloccare un retry -> anti-409)."""
    thread = getattr(runtime, "_runtime_thread", None)
    if thread is None:
        return False
    try:
        return bool(thread.is_alive())
    except Exception:  # pragma: no cover - is_alive difensivo
        return True


class _AnyAliveThread:
    """Proxy `_runtime_thread` per l'orchestratore: `is_alive()` è True se un
    QUALSIASI transport child è vivo. Serve al guard `previous_runtime_still_alive`
    e all'anti-orfano del service, che leggono `listener._runtime_thread` e ne
    chiamano `is_alive()`: con N bot un solo thread vivo deve bastare a bloccare un
    secondo avvio (doppio getUpdates/409)."""

    def __init__(self, runtimes: List[Any]):
        self._runtimes = runtimes

    def is_alive(self) -> bool:
        return any(_child_thread_alive(r) for r in self._runtimes)


class TelegramMultiBotRuntime:
    """Orchestratore N-bot (epica #374 PR-5b): compone N `TelegramBotApiRuntime`
    indipendenti (uno per bot attivo *usable*) presentando la STESSA superficie
    duck-typed attesa da `TelegramService.self.listener`
    (`state`/`running`/`_runtime_thread`/`start`/`stop`/`status`/`runtime_snapshot`).

    Ogni child mantiene i propri invarianti per-token: transport getUpdates
    dedicato, thread proprio, sink di parsing PRIVATO (mai condiviso — `handle_
    incoming` muta stato non sotto lock, quindi N thread su un sink solo sarebbe una
    race). Il punto di fan-in condiviso è a valle, in `TelegramService._handle_
    signal` (serializzato lì con un lock in PR-5b).

    Semantica AGGREGATA fail-closed:
    - `state`: CONNECTED solo se TUTTI i child sono CONNECTED; qualsiasi child non
      CONNECTED => FAILED (in PR-5b l'autoheal service-level fa restart-all;
      l'autoheal per-bot arriva in PR-5c). STOPPED/CREATED solo se TUTTI lo sono.
    - `running`: True solo se TUTTI i child sono running.
    - `_runtime_thread`: proxy any-alive (un solo thread child vivo blocca un retry).
    - `stop()`: fail-closed se un QUALSIASI thread child sopravvive (anti-409).
    - `handlers_registered`/`active_network_resources`/`monitored_chat_count`:
      SOMMA sui child. `expected_handlers` = numero di bot (per l'invariant guard
      generalizzato). `healthy_bot_count` = child con >=1 handler.
    """

    def __init__(self, runtimes: List[TelegramBotApiRuntime]):
        if not runtimes:
            raise ValueError("TelegramMultiBotRuntime richiede almeno un runtime")
        self._runtimes: List[Any] = list(runtimes)
        self.intentional_stop = False
        self._any_alive = _AnyAliveThread(self._runtimes)

    # ---- stato aggregato ----
    @property
    def state(self) -> str:
        states = [r.state for r in self._runtimes]
        if all(s == "CONNECTED" for s in states):
            return "CONNECTED"
        if all(s == "STOPPED" for s in states):
            return "STOPPED"
        if all(s == "CREATED" for s in states):
            return "CREATED"
        # Qualsiasi mix (parziale/degradato) => FAILED (fail-closed): l'autoheal
        # service-level riavvia l'intero set (per-bot in PR-5c).
        return "FAILED"

    @property
    def running(self) -> bool:
        return bool(self._runtimes) and all(bool(r.running) for r in self._runtimes)

    @property
    def _runtime_thread(self):
        return self._any_alive

    def start(self) -> dict:
        """Avvia (fan-out) tutti i child. Aggregato: `started=True` solo se TUTTI
        partono. Su fallimento parziale/totale => `started=False` con gli errori
        aggregati; i child già avviati NON vengono fermati qui (il loro thread resta
        visibile via `_runtime_thread` any-alive => il guard blocca un retry, e
        l'autoheal service-level farà restart-all)."""
        self.intentional_stop = False
        results = []
        for r in self._runtimes:
            try:
                res = r.start() or {}
            except Exception as exc:  # pragma: no cover - start dei child è già fail-safe
                res = {"started": False, "error": type(exc).__name__}
            results.append(res)
        if all(bool(x.get("started")) for x in results):
            return {"started": True}
        errors = [str(x.get("error") or "") for x in results if not x.get("started")]
        detail = ",".join(e for e in errors if e) or "multibot_start_failed"
        return {"started": False, "error": "multibot_partial_start:" + detail}

    def stop(self) -> dict:
        """Ferma (fan-out) tutti i child. Fail-closed: se un QUALSIASI thread child
        sopravvive => `stopped=False` (anti-409: un restart aprirebbe un secondo
        getUpdates sullo stesso token)."""
        self.intentional_stop = True
        results = []
        for r in self._runtimes:
            try:
                res = r.stop() or {}
            except Exception as exc:  # pragma: no cover - stop dei child è già best-effort
                res = {"stopped": False, "error": type(exc).__name__}
            results.append(res)
        if any(_child_thread_alive(r) for r in self._runtimes):
            self.intentional_stop = False
            return {"stopped": False, "error": "multibot_thread_still_alive"}
        if all(bool(x.get("stopped")) for x in results):
            return {"stopped": True}
        errors = [str(x.get("error") or "") for x in results if not x.get("stopped")]
        detail = ",".join(e for e in errors if e) or "multibot_stop_failed"
        return {"stopped": False, "error": "multibot_stop_failed:" + detail}

    def status(self) -> dict:
        child = [r.status() for r in self._runtimes]
        handlers = sum(int(c.get("handlers_registered", 0) or 0) for c in child)
        net = sum(int(c.get("active_network_resources", 0) or 0) for c in child)
        chats = sum(int(c.get("monitored_chat_count", 0) or 0) for c in child)
        running = bool(child) and all(bool(c.get("running")) for c in child)
        last_error = next((str(c.get("last_error")) for c in child if c.get("last_error")), "")
        ts_values = [
            c.get("last_successful_message_ts")
            for c in child
            if c.get("last_successful_message_ts") is not None
        ]
        last_ts = max(ts_values) if ts_values else None
        return {
            "state": self.state,
            "running": running,
            "intentional_stop": bool(self.intentional_stop),
            "reconnect_attempts": 0,
            "reconnect_in_progress": False,
            "last_error": last_error,
            "last_successful_message_ts": last_ts,
            "listener_started": any(bool(c.get("listener_started")) for c in child),
            "handlers_registered": handlers,
            "active_network_resources": net,
            "monitored_chat_count": chats,
            # Per l'invariant guard generalizzato (CONNECTED => handlers == expected).
            "expected_handlers": len(self._runtimes),
            "bot_count": len(self._runtimes),
            "healthy_bot_count": sum(
                1 for c in child if int(c.get("handlers_registered", 0) or 0) >= 1
            ),
        }

    def runtime_snapshot(self) -> dict:
        st = self.status()
        return {
            "state": st["state"],
            "running": st["running"],
            "listener_started": st["listener_started"],
            "client_alive": st["running"],
            "handlers_registered": st["handlers_registered"],
            "expected_handlers": st["expected_handlers"],
            "reconnect_in_progress": False,
            "reconnect_attempts": 0,
            "active_network_resources": st["active_network_resources"],
            "intentional_stop": st["intentional_stop"],
            "retry_loop_active": False,
            "last_error": st["last_error"],
            "last_successful_message_ts": st["last_successful_message_ts"],
        }
