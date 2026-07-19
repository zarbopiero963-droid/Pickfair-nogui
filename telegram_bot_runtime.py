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
fail-closed. PR-5c aggiunge l'autoheal PER-BOT (`run_perbot_autoheal_once`):
un bot degradato viene riavviato da solo (budget/lockout per-child), senza
restart-all e senza toccare i bot sani; l'aggregato resta FAILED finché quel bot
non è guarito. Rimandati alle PR successive:
- validazione avanzata dei chat_id / risoluzione `@username` (qui: coercizione a
  int per l'allow-list; un bot con sole chat non numeriche è non-usable);
- terminazione per-bot più rapida della finestra cooperativa di stop.

Il `bot_token` è un SEGRETO: mai loggato (il transport redige; qui non lo si
stampa mai). Isolato dal path Telethon esistente, che resta invariato.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Callable, List, Optional

from recovery.telegram_autoheal import (
    TelegramAutohealAction,
    TelegramAutohealHistory,
    TelegramAutohealPolicy,
    TelegramAutohealSnapshot,
)
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

    def restart(self) -> dict:
        """Riavvio di QUESTO bot (stop -> start), per l'autoheal PER-BOT (PR-5c).
        Anti-409 preservato: se il thread transport sopravvive allo stop, `start()`
        è orphan-guarded e NON riapre un secondo getUpdates (ritorna
        `bot_transport_orphan_alive`); il retry riparte al ciclo successivo quando
        il thread è morto. NON è uno stop operatore: `stop()` setta transitoriamente
        `intentional_stop=True`, ma `start()` (o il ramo fail-closed di `stop()`) lo
        riporta a False, così il recovery non resta soppresso."""
        stop_res = self.stop() or {}
        if not stop_res.get("stopped", False):
            # stop() NON ha fermato il transport (thread ancora vivo): NON chiamare
            # start() — con `_started` residuo True risponderebbe "already_running"
            # mascherando un restart mai avvenuto (rilievo CodeRabbit/Greptile).
            # Anti-409 preservato: nessun secondo transport aperto; il thread si
            # spegne e il retry riparte al ciclo successivo.
            return {"restarted": False, "stop": stop_res, "start": {}}
        start_res = self.start() or {}
        return {
            "restarted": bool(start_res.get("started")),
            "stop": stop_res,
            "start": start_res,
        }

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


def _sum_int_field(dicts: List[dict], key: str) -> int:
    """Somma robusta di un campo intero across gli status dei child."""
    return sum(int(d.get(key, 0) or 0) for d in dicts)


def _parse_iso_utc(value):
    """Parse ISO-8601 → datetime normalizzata a UTC (None se non valido)."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _latest_iso_ts(values):
    """Timestamp ISO CRONOLOGICAMENTE più recente tra `values`. Confronta le
    datetime NORMALIZZATE a UTC, NON le stringhe: il `max()` lessicografico
    sbaglierebbe con offset timezone diversi tra i child (es. `+05:00` vs `+00:00`)
    → falsi STALE_RUNTIME nel guard. Fail-safe: valori non parsabili ignorati;
    None se nessuno è parsabile."""
    best_str, best_dt = None, None
    for v in values:
        dt = _parse_iso_utc(v)
        if dt is None:
            continue
        if best_dt is None or dt > best_dt:
            best_dt, best_str = dt, v
    return best_str


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

    name = "telegram-multibot-any-alive"

    def __init__(self, runtimes: List[Any]):
        self._runtimes = runtimes

    def is_alive(self) -> bool:
        return any(_child_thread_alive(r) for r in self._runtimes)

    def join(self, timeout=None) -> None:
        """Fan-out join sui thread child (difensivo: se un chiamante tratta il proxy
        come un Thread reale, es. shutdown/autoheal, NON deve sollevare
        AttributeError e saltare l'anti-409). Best-effort, non propaga."""
        for r in self._runtimes:
            t = getattr(r, "_runtime_thread", None)
            joiner = getattr(t, "join", None)
            if callable(joiner):
                try:
                    joiner(timeout=timeout)
                except Exception:  # pragma: no cover - join difensivo
                    pass


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

    def __init__(
        self,
        runtimes: List[TelegramBotApiRuntime],
        *,
        autoheal_policy: Optional[TelegramAutohealPolicy] = None,
    ):
        if not runtimes:
            raise ValueError("TelegramMultiBotRuntime richiede almeno un runtime")
        self._runtimes: List[Any] = list(runtimes)
        self.intentional_stop = False
        self._any_alive = _AnyAliveThread(self._runtimes)
        # True SOLO durante il fan-out di start(): distingue un mix TRANSITORIO
        # (CONNECTED+CREATED mentre i child partono in sequenza) da una degradazione
        # PERSISTENTE (un child giù mentre altri su). Bool = read/write atomico:
        # l'autoheal può leggerlo concorrentemente senza lock.
        self._starting = False
        # Autoheal PER-BOT (PR-5c): la policy è STATELESS (decide da snapshot+history),
        # quindi una sola istanza valuta ogni child. Lo stato per-child (budget/lockout)
        # vive qui, PARALLELO a `_runtimes` (indice = child): timestamp dei restart
        # nella finestra, istante di ingresso in lockout, totale restart.
        self._autoheal_policy = autoheal_policy or TelegramAutohealPolicy()
        self._child_restart_ts: List[List[float]] = [[] for _ in self._runtimes]
        self._child_lockout_since: List[Optional[float]] = [None for _ in self._runtimes]
        self._child_restart_total: List[int] = [0 for _ in self._runtimes]
        # Deferral CONSECUTIVI (stop che non cicla il transport, thread vivo): un
        # wind-down transitorio non consuma il budget restart, ma un thread
        # PERMANENTEMENTE bloccato non deve generare retry infiniti (rilievo GPT-5.6
        # Terra) => dopo `max_restarts_in_window` deferral consecutivi => lockout stuck.
        self._child_deferred_consecutive: List[int] = [0 for _ in self._runtimes]
        # Guardia per-child: True mentre `child.restart()` è in corso (fuori dal lock).
        # Impedisce che due cicli autoheal concorrenti riavviino lo STESSO transport in
        # parallelo (doppio getUpdates/orphan, budget incoerente) — rilievo GPT/Fable/Fugu.
        self._child_restart_in_progress: List[bool] = [False for _ in self._runtimes]
        # Serializza lo stato autoheal per-child (liste `_child_*`). L'autoheal gira
        # sul singolo thread watchdog (`run_autoheal_once`, lifecycle serializzato),
        # ma il lock difende comunque contro letture concorrenti da
        # `status()`/`locked_out_bot_count` (probe) e da eventuali chiamate concorrenti
        # future — così budget/lockout non vengono calcolati due volte sullo stesso bot.
        self._perbot_heal_lock = threading.Lock()

    # ---- stato aggregato ----
    @property
    def state(self) -> str:
        states = [r.state for r in self._runtimes]
        # Un child DEGRADATO (FAILED) domina l'aggregato (fail-closed, onesto: un bot
        # è giù). Il recovery NON è più restart-all: il service delega ad
        # `run_perbot_autoheal_once` (PR-5c), che riavvia il SOLO bot giù; l'aggregato
        # torna CONNECTED quando il bot è guarito.
        if any(s == "FAILED" for s in states):
            return "FAILED"
        if all(s == "CONNECTED" for s in states):
            return "CONNECTED"
        if all(s == "STOPPED" for s in states):
            return "STOPPED"
        if all(s == "CREATED" for s in states):
            return "CREATED"
        # Mix senza FAILED. Distingue TRANSITORIO vs PERSISTENTE (rilievo convergente
        # GPT-5.6 Terra / Fable 5 / Fugu Ultra):
        # - DURANTE il fan-out di start() (`_starting`): il mix CONNECTED+CREATED è
        #   normale (i child partono in sequenza) => CONNECTING, NON FAILED, così un
        #   autoheal concorrente non fa restart-all spurio (409).
        # - FUORI dalla finestra di start(): un mix che NON converge a all-CONNECTED
        #   (es. un child rimasto CREATED/STOPPED per orphan/thread morto senza flag
        #   FAILED) è una DEGRADAZIONE PERSISTENTE => FAILED, così l'autoheal
        #   service-level riavvia il bot giù (niente mascheramento indefinito).
        return "CONNECTING" if self._starting else "FAILED"

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
        # `_starting`=True per l'INTERO fan-out: uno stato aggregato letto in questa
        # finestra (mix CONNECTED+CREATED transitorio) resta CONNECTING, non FAILED.
        self._starting = True
        try:
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
        finally:
            self._starting = False

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
            # Thread child SUPERSTITE (zombie) dopo uno stop OPERATORE. PRESERVA
            # intentional_stop=True (rilievo Fugu Ultra, coerente col ramo sotto):
            # un restart-all per "ripulire" lo zombie aprirebbe un secondo getUpdates
            # sullo stesso bot (Telegram 409 Conflict / doppio consumo). Il fail-closed è NON
            # riavviare dopo uno stop operatore; lo zombie è SURFACED (aggregato
            # not-stopped + thread vivo => anti-409 guard) per intervento manuale /
            # autoheal per-bot (PR-5c), mai per restart-all automatico.
            return {"stopped": False, "error": "multibot_thread_still_alive"}
        if all(bool(x.get("stopped")) for x in results):
            return {"stopped": True}
        # Nessun thread child vivo ma uno stop() figlio ha riportato stopped=False.
        # PRESERVA intentional_stop (rilievo Fable 5): stop() è invocato per intento
        # OPERATORE; azzerare il flag qui farebbe RIAVVIARE il listener dall'autoheal
        # service-level DOPO uno shutdown voluto (ripresa consumo segnali/piazzamenti
        # contro l'intento operatore = rischio safety). Senza thread vivi non c'è
        # orfano né consumo in corso: lasciare intentional_stop=True è sicuro e non
        # incastra nulla (nessun thread da guarire). [Supera il precedente rilievo
        # Greptile P2, che non distingueva stop-operatore da stop-di-healing.]
        errors = [str(x.get("error") or "") for x in results if not x.get("stopped")]
        detail = ",".join(e for e in errors if e) or "multibot_stop_failed"
        return {"stopped": False, "error": "multibot_stop_failed:" + detail}

    def run_perbot_autoheal_once(self, now_ts: Optional[float] = None) -> dict:
        """Autoheal PER-BOT (PR-5c): valuta OGNI child e riavvia SOLO i bot non-sani.

        Con budget/lockout PER-CHILD, senza toccare i bot sani (sink/transport
        isolati): rimpiazza il restart-ALL service-level. La policy è la stessa del
        service (stateless), applicata per-child con history dedicata. Fail-closed:
        budget esaurito nella finestra => lockout del SOLO bot (niente restart storm);
        un bot tornato CONNECTED azzera il budget. Idempotente sui bot sani.

        Osservabilità onesta: `healed` conta i restart REALMENTE riusciti (transport
        ciclato + start ok), NON i tentativi; `restart_failed` i tentativi reali non
        riusciti; `restart_deferred` i casi in cui lo stop non ha ciclato il transport
        (thread ancora vivo => anti-409, NON consuma budget); `locked_out` le NUOVE
        transizioni in lockout e `locked_out_now` i bot ATTUALMENTE in lockout (così il
        service può loggare la degradazione persistente, non solo la transizione).
        Resiliente: un'eccezione su un child NON aborta il ciclo degli altri.
        """
        now_ts = float(now_ts if now_ts is not None else self._autoheal_policy.now())
        actions: List[dict] = []
        for i, child in enumerate(self._runtimes):
            try:
                actions.append(self._heal_one_child(i, child, now_ts))
            except Exception as exc:  # resilienza: un bot che solleva non blocca gli altri
                actions.append({"index": i, "action": "error", "error": type(exc).__name__})
        with self._perbot_heal_lock:
            locked_out_now = self._locked_out_count_locked(now_ts)
        healed = sum(1 for a in actions if a["action"] == "restart")
        locked_out = sum(1 for a in actions if a["action"] == "lockout")
        restart_failed = sum(1 for a in actions if a["action"] in ("restart_failed", "restart_deferred"))
        errors = sum(1 for a in actions if a["action"] == "error")
        return {
            "healed": healed, "locked_out": locked_out, "locked_out_now": locked_out_now,
            "restart_failed": restart_failed, "errors": errors, "actions": actions,
        }

    def _heal_one_child(self, i: int, child: Any, now_ts: float) -> dict:
        """Valuta e cura il child `i`.

        Il lock protegge SOLO decisione e mutazioni dello stato per-child (brevi);
        `child.restart()` (stop con `join` fino a ~5s) gira **fuori** dal lock, così
        `status()`/`locked_out_bot_count()` (probe/watchdog) non si bloccano durante
        un restart storm (rilievo Fable 5 / Fugu Ultra). Il caller isola le eccezioni.
        """
        # Snapshot fuori dal lock (potenziale I/O).
        snap = child.runtime_snapshot() or {}
        state = str(snap.get("state") or "")
        with self._perbot_heal_lock:
            if state == "CONNECTED":  # bot SANO: azzera budget/lockout/deferred (recovery pulito)
                self._child_restart_ts[i] = []
                self._child_lockout_since[i] = None
                self._child_deferred_consecutive[i] = 0
                return {"index": i, "action": "healthy"}
            self._prune_child_heal_state_locked(i, now_ts)
            decision = self._autoheal_policy.evaluate(
                self._child_ah_snapshot_locked(i, snap, now_ts),
                self._child_ah_history_locked(i),
            )
            if decision.action == TelegramAutohealAction.ENTER_FAILED_LOCKOUT:
                if self._child_lockout_since[i] is None:
                    self._child_lockout_since[i] = now_ts
                return {"index": i, "action": "lockout", "reason": decision.reason}
            if decision.action != TelegramAutohealAction.SCHEDULE_RESTART:
                return {"index": i, "action": decision.action.value, "reason": decision.reason}
            # Guardia atomica: un altro ciclo sta già riavviando questo child => skip
            # (niente doppio restart dello stesso transport, rilievo GPT/Fable/Fugu).
            if self._child_restart_in_progress[i]:
                return {"index": i, "action": "restart_in_progress"}
            self._child_restart_in_progress[i] = True
        # SCHEDULE_RESTART: `restart()` FUORI dal lock (I/O lento). La guardia
        # `_child_restart_in_progress[i]` va SEMPRE azzerata (`finally`): un `restart()`
        # che solleva O un esito MALFORMATO (`stop`/`start` non-dict) non deve escludere
        # il child per sempre (rilievo GPT-5.6 Terra). Il parsing è difensivo
        # (`isinstance`), così un esito non conforme diventa un restart_deferred, non
        # un'eccezione.
        try:
            res = child.restart()
            res = res if isinstance(res, dict) else {}
            stop_res = res.get("stop")
            start_res = res.get("start")
            stopped_ok = isinstance(stop_res, dict) and stop_res.get("stopped") is True
            started_ok = bool(isinstance(start_res, dict) and start_res.get("started"))
            with self._perbot_heal_lock:
                return self._record_restart_result_locked(i, now_ts, stopped_ok, started_ok, decision.reason)
        finally:
            with self._perbot_heal_lock:
                self._child_restart_in_progress[i] = False

    def _record_restart_result_locked(
        self, i: int, now_ts: float, stopped_ok: bool, started_ok: bool, reason: str
    ) -> dict:
        """Registra l'esito di `restart()` e ritorna l'action dict. Sotto lock.

        Budget consumato SOLO se il transport è stato realmente ciclato (`stopped_ok`);
        uno stop non riuscito è `restart_deferred` (non consuma budget), ma dopo
        `max_restarts_in_window` deferral CONSECUTIVI diventa lockout "stuck"
        (fail-closed, niente loop infinito su un thread permanentemente bloccato).
        """
        if not stopped_ok:
            self._child_deferred_consecutive[i] += 1
            if self._child_deferred_consecutive[i] >= self._autoheal_policy.max_restarts_in_window:
                self._child_deferred_consecutive[i] = 0  # assorbito dal lockout
                if self._child_lockout_since[i] is None:
                    self._child_lockout_since[i] = now_ts
                return {"index": i, "action": "lockout", "reason": "restart_deferred_stuck"}
            return {"index": i, "action": "restart_deferred", "restarted": False, "reason": reason}
        self._child_deferred_consecutive[i] = 0
        self._child_restart_ts[i].append(now_ts)
        self._child_restart_total[i] += 1
        if started_ok:
            return {"index": i, "action": "restart", "restarted": True, "reason": reason}
        return {"index": i, "action": "restart_failed", "restarted": False, "reason": reason}

    def _prune_child_heal_state_locked(self, i: int, now_ts: float) -> None:
        """Prune dei restart fuori finestra + scadenza lockout. Sotto lock."""
        window = self._autoheal_policy.restart_window_sec
        self._child_restart_ts[i] = [t for t in self._child_restart_ts[i] if (now_ts - t) <= window]
        since = self._child_lockout_since[i]
        if since is not None and (now_ts - since) >= self._autoheal_policy.lockout_sec:
            self._child_lockout_since[i] = None

    def _child_ah_history_locked(self, i: int) -> TelegramAutohealHistory:
        return TelegramAutohealHistory(
            restart_timestamps=tuple(self._child_restart_ts[i]),
            lockout_since_ts=self._child_lockout_since[i],
        )

    def _child_ah_snapshot_locked(self, i: int, snap: dict, now_ts: float) -> TelegramAutohealSnapshot:
        since = self._child_lockout_since[i]
        lockout_active = since is not None and (now_ts - since) < self._autoheal_policy.lockout_sec
        return TelegramAutohealSnapshot(
            state=str(snap.get("state") or ""),
            invariant_ok=True,
            active_alert_codes=(),
            reconnect_attempts=len(self._child_restart_ts[i]),
            restart_attempts_total=int(self._child_restart_total[i]),
            restart_in_progress=False,
            intentional_stop=bool(snap.get("intentional_stop")),
            startup_grace_active=False,
            reconnect_grace_active=False,
            lockout_active=lockout_active,
            last_error_category=str(snap.get("last_error") or ""),
            failure_escalated=False,
            listener_stale=False,
            now_ts=now_ts,
        )

    def _locked_out_count_locked(self, now_ts: float) -> int:
        lockout_sec = self._autoheal_policy.lockout_sec
        return sum(
            1 for since in self._child_lockout_since
            if since is not None and (now_ts - since) < lockout_sec
        )

    def locked_out_bot_count(self, now_ts: Optional[float] = None) -> int:
        """Numero di bot attualmente in lockout per-bot (budget restart esaurito)."""
        now_ts = float(now_ts if now_ts is not None else self._autoheal_policy.now())
        with self._perbot_heal_lock:
            return self._locked_out_count_locked(now_ts)

    def status(self) -> dict:
        child = [r.status() for r in self._runtimes]
        last_error = next((str(c.get("last_error")) for c in child if c.get("last_error")), "")
        # Letture per-bot sotto lock (coerenti con le mutazioni in
        # run_perbot_autoheal_once): un'unica sezione critica per entrambi i campi.
        with self._perbot_heal_lock:
            locked_out_bot_count = self._locked_out_count_locked(self._autoheal_policy.now())
            perbot_restart_total = sum(int(t) for t in self._child_restart_total)
        return {
            "state": self.state,
            "running": bool(child) and all(bool(c.get("running")) for c in child),
            "intentional_stop": bool(self.intentional_stop),
            "reconnect_attempts": 0,
            "reconnect_in_progress": False,
            "last_error": last_error,
            # Confronto CRONOLOGICO (UTC), non lessicografico sulle stringhe ISO.
            "last_successful_message_ts": _latest_iso_ts(
                [c.get("last_successful_message_ts") for c in child]
            ),
            "listener_started": any(bool(c.get("listener_started")) for c in child),
            "handlers_registered": _sum_int_field(child, "handlers_registered"),
            "active_network_resources": _sum_int_field(child, "active_network_resources"),
            "monitored_chat_count": _sum_int_field(child, "monitored_chat_count"),
            # Per l'invariant guard generalizzato (CONNECTED => handlers == expected).
            "expected_handlers": len(self._runtimes),
            "bot_count": len(self._runtimes),
            "healthy_bot_count": sum(
                1 for c in child if int(c.get("handlers_registered", 0) or 0) >= 1
            ),
            # Autoheal PER-BOT (PR-5c): bot in lockout (budget restart esaurito) e
            # totale restart per-bot cumulativo — SURFACED per l'osservabilità.
            "locked_out_bot_count": locked_out_bot_count,
            "perbot_restart_total": perbot_restart_total,
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
