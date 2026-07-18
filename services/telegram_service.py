from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Optional

from observability.telegram_health_probe import TelegramHealthProbe
from observability.telegram_invariant_guard import TelegramInvariantSnapshot
from recovery.telegram_autoheal import (
    TelegramAutohealAction,
    TelegramAutohealDecision,
    TelegramAutohealHistory,
    TelegramAutohealPolicy,
    TelegramAutohealSnapshot,
)
from telegram_bot_runtime import TelegramBotApiRuntime, TelegramMultiBotRuntime
from telegram_listener import TelegramListener

logger = logging.getLogger(__name__)


class TelegramService:
    """
    Service Telegram runtime-safe.

    Responsabilità:
    - avvio / stop listener
    - inoltro segnali al bus
    - preserva simulation_mode nei payload
    - non contiene logica di trading
    """

    def __init__(self, settings_service, db, bus, client_factory=None, connect_timeout: float = 10.0, bot_transport_factory=None):
        self.settings_service = settings_service
        self.db = db
        self.bus = bus
        # Factory iniettabile del client Telegram (test/diagnostica);
        # None = client Telethon reale costruito dal listener.
        self._client_factory = client_factory
        # Factory iniettabile del transport Bot API (test/diagnostica);
        # None = TelegramBotApiTransport reale costruito dall'adapter.
        # Firma: (bot_token, chat_ids, on_message) -> transport.
        self._bot_transport_factory = bot_transport_factory
        self._connect_timeout = float(connect_timeout)
        # Il listener può essere il TelegramListener (path Telethon userbot),
        # l'adapter Bot API single-bot (PR-5a) o l'orchestratore N-bot (PR-5b):
        # stessa superficie runtime duck-typed.
        self.listener: Optional[
            TelegramListener | TelegramBotApiRuntime | TelegramMultiBotRuntime
        ] = None
        self.connected = False
        self.last_error = ""
        self.state = "CREATED"
        self.intentional_stop = False
        self.reconnect_attempts = 0
        self.reconnect_in_progress = False
        self.last_successful_message_ts: str | None = None
        self.listener_started = False
        self.handlers_registered = 0
        self.active_network_resources = 0
        self._health_probe = TelegramHealthProbe()
        self._autoheal_policy = TelegramAutohealPolicy()
        self._restart_attempts_total = 0
        self._restart_timestamps: list[float] = []
        self._lockout_active = False
        self._lockout_since_ts: float | None = None
        self._lockout_reason = ""
        self._last_restart_ts: float | None = None
        self._last_autoheal_action = TelegramAutohealAction.NO_ACTION.value
        self._last_autoheal_decision_reason = "not_evaluated"
        self._restart_in_progress = False
        # Fan-in N-bot (PR-5b): con l'orchestratore multi-bot, N thread transport
        # consegnano segnali/status CONCORRENTEMENTE a _handle_signal/_handle_status
        # (prima single-writer). Il lock serializza la sezione critica (mutazione di
        # last_successful_message_ts + save_received_signal + bus.publish) così le
        # consegne non si intrecciano. Sink di parsing restano per-bot (non condivisi).
        self._signal_fanin_lock = threading.Lock()
        # Bot ATTIVI ma non-usable (sole chat non numeriche / @username rimandato):
        # SURFACED in status (visibile), non droppati in silenzio (PR-5b).
        self._unusable_active_bot_count = 0

    def _set_state(self, new_state: str) -> None:
        allowed_states = {"CREATED", "CONNECTING", "CONNECTED", "RECONNECTING", "STOPPED", "FAILED"}
        if new_state not in allowed_states:
            raise ValueError(f"Invalid Telegram service state: {new_state}")
        self.state = new_state

    # =========================================================
    # INTERNAL CALLBACKS
    # =========================================================
    def _handle_signal(self, signal: dict) -> None:
        signal = dict(signal or {})
        # Preserva il timestamp di RICEZIONE messo dal listener; qui si
        # genera solo come fallback (l'ora di processing non e' la ricezione).
        received_at = signal.get("received_at") or datetime.now(timezone.utc).isoformat()
        signal["received_at"] = received_at
        # conserva eventuale flag simulation_mode già presente
        signal["simulation_mode"] = bool(signal.get("simulation_mode", False))

        # Fan-in serializzato (PR-5b): con N bot, thread transport distinti chiamano
        # qui in parallelo. Il lock evita interleaving su last_successful_message_ts
        # e sulla coppia save_received_signal→bus.publish (consegna coerente).
        with self._signal_fanin_lock:
            self.last_successful_message_ts = received_at
            if hasattr(self.db, "save_received_signal"):
                try:
                    self.db.save_received_signal(signal)
                except Exception as exc:
                    logger.warning("save_received_signal fallita: %s", exc)
            self.bus.publish("SIGNAL_RECEIVED", signal)

    def _handle_status(self, *args) -> None:
        """
        Compatibile con callback:
        - on_status(message)
        - on_status(status, message)
        """
        if len(args) >= 2:
            status = str(args[0] or "")
            message = str(args[1] or "")
        elif len(args) == 1:
            status = "INFO"
            message = str(args[0] or "")
        else:
            status = "INFO"
            message = ""

        with self._signal_fanin_lock:
            self.bus.publish(
                "TELEGRAM_STATUS",
                {
                    "status": status,
                    "message": message,
                },
            )

    def _stop_partial_listener_on_start_failure(self) -> bool:
        """Best-effort stop del listener dopo un'eccezione in start() (avvio
        parziale). Ritorna True se un thread runtime SOPRAVVIVE comunque (orfano da
        NON nascondere: il chiamante tiene il riferimento perché il guard
        previous_runtime_still_alive lo veda e blocchi un retry, evitando un secondo
        getUpdates/409 e segnali di betting duplicati). Fail-safe: eccezioni nello
        stop non si propagano (siamo già sul path di errore)."""
        lst = self.listener
        if lst is None:
            return False
        stopper = getattr(lst, "stop", None)
        if callable(stopper):
            try:
                stopper()
            except Exception:  # pragma: no cover - lo stop non deve mascherare l'errore originale
                pass
        thread = getattr(lst, "_runtime_thread", None)
        if thread is None:
            return False  # nessun thread runtime => morto (safe: azzera il riferimento)
        try:
            return bool(thread.is_alive())
        except Exception:
            # Incertezza sulla liveness (is_alive solleva) => FAIL-CLOSED: assumi il
            # thread VIVO (tieni il listener, blocca il retry). Un fail-open qui
            # riaprirebbe la duplicazione dei segnali di betting.
            return True

    def _refresh_runtime_truth_from_listener(self) -> None:
        status_getter = getattr(self.listener, "status", None) if self.listener else None
        if callable(status_getter):
            snap = status_getter() or {}
            self.state = str(snap.get("state") or self.state)
            listener_reconnect_attempts = int(snap.get("reconnect_attempts", 0) or 0)
            self.reconnect_attempts = max(self.reconnect_attempts, listener_reconnect_attempts)
            self.reconnect_in_progress = bool(snap.get("reconnect_in_progress", self.reconnect_in_progress))
            self.listener_started = bool(snap.get("listener_started", self.listener_started))
            self.handlers_registered = int(snap.get("handlers_registered", self.handlers_registered) or 0)
            self.active_network_resources = int(snap.get("active_network_resources", self.active_network_resources) or 0)
            if not self.last_error:
                self.last_error = str(snap.get("last_error") or "")
            # Liveness: il listener è la fonte di verità runtime (si aggiorna
            # su ogni messaggio, anche non-segnale); il cache del service
            # resta solo come fallback quando il listener non ha un valore.
            listener_ts = snap.get("last_successful_message_ts")
            if listener_ts is not None:
                self.last_successful_message_ts = listener_ts
        self.connected = self.state == "CONNECTED"

    # =========================================================
    # LIFECYCLE
    # =========================================================
    @staticmethod
    def _numeric_active_chat_ids(chats: list) -> list:
        """chat_id NUMERICI e attivi di un bot. getUpdates restituisce chat.id
        numerico: un chat_id non numerico (es. `@canale`) non è ascoltabile per
        questa via -> scartato (la risoluzione di `@username` è rimandata). Fa
        anche da validazione fail-closed: un bot con sole chat non numeriche
        risulta senza chat utilizzabili (=> non usable)."""
        out = []
        for c in chats or []:
            if not c.get("is_active"):
                continue
            s = str(c.get("chat_id") or "").strip()
            if not s:
                continue
            try:
                int(s)
            except (TypeError, ValueError):
                continue
            out.append(s)
        return out

    def _select_bot_api_source(self) -> tuple:
        """Legge i bot Bot API in UNA sola lettura e ritorna `(active_count, usable)`
        (epica #374 PR-5a):
        - `active_count`: numero di bot ATTIVI con token — il gate multi-bot fa
          fail-closed su >1 attivo (niente drop silenzioso di una sorgente attiva),
          A PRESCINDERE dall'usabilità delle chat;
        - `usable`: lista `(bot_dict, [chat_id_str])` dei bot attivi con >=1 chat
          NUMERICA attiva (per la selezione della sorgente).

        Read-only. Gli errori DB **PROPAGANO** (nessun degrado a 0/[] che aprirebbe
        un buco nel fail-closed: un conteggio azzerato per errore riavvierebbe un
        singolo bot droppando il 2°): `start()` cattura e fa fail-closed
        'telegram_bot_config_read_error'. Un'UNICA lettura evita incoerenze tra il
        conteggio (gate) e la selezione (sorgente) — due letture separate potrebbero
        divergere. Il token viene letto perché serve comunque a costruire il runtime
        del bot selezionato (nessuna estrazione extra di segreti solo per contare).

        Un DB senza supporto Bot API (metodo `get_telegram_bots` ASSENTE, es. legacy/
        minimal) NON è un errore di lettura: significa "nessuna sorgente bot" =>
        `(0, [])` => `start()` cade nel fail-closed 'Configurazione incompleta'. La
        propagazione riguarda solo gli errori REALI del metodo quando esiste."""
        getter = getattr(self.db, "get_telegram_bots", None)
        if not callable(getter):
            return 0, []
        bots = getter(include_token=True) or []
        active_count = 0
        usable: list = []
        for bot in bots:
            if not bot.get("is_active") or not bot.get("bot_token"):
                continue
            active_count += 1
            chats = self.db.get_telegram_bot_chats(bot["id"]) or []
            chat_ids = self._numeric_active_chat_ids(chats)
            if chat_ids:
                usable.append((bot, chat_ids))
        return active_count, usable

    def _running_chat_count(self) -> int:
        """Numero di chat monitorate dal listener IN ESECUZIONE (Telethon o Bot
        API), per le risposte `already_running` — evita di rivalutare il gate/la
        selezione su un runtime sano. Fail-safe: 0 se non leggibile."""
        try:
            snap = self.listener.status() if self.listener else {}
            return int(snap.get("monitored_chat_count", 0) or 0)
        except Exception:  # pragma: no cover - degrado difensivo
            return 0

    def _build_botapi_runtime(self, bot: dict, chat_ids: list) -> TelegramBotApiRuntime:
        """Costruisce l'adapter Bot API (path bot_token, PR-5a). Estratto da
        start() per tenerne bassa la complessità."""
        return TelegramBotApiRuntime(
            bot_token=bot["bot_token"],
            chat_ids=chat_ids,
            db=self.db,
            on_signal=self._handle_signal,
            on_status=self._handle_status,
            transport_factory=self._bot_transport_factory,
        )

    def _build_multibot_runtime(self, usable: list) -> TelegramMultiBotRuntime:
        """Costruisce l'orchestratore N-bot (PR-5b): un TelegramBotApiRuntime per
        ciascun bot usable, tutti con fan-in verso gli STESSI callback
        on_signal/on_status (serializzati da _signal_fanin_lock). Ogni child ha il
        proprio transport getUpdates, thread e sink di parsing PRIVATO."""
        return TelegramMultiBotRuntime(
            [self._build_botapi_runtime(bot, chat_ids) for bot, chat_ids in usable]
        )

    def _build_telethon_listener(self, cfg) -> TelegramListener:
        """Costruisce e configura il listener Telethon userbot (path invariato).
        Estratto da start() per tenerne bassa la complessità."""
        listener = TelegramListener(
            api_id=int(cfg.api_id),
            api_hash=cfg.api_hash,
            session_string=cfg.session_string or None,
            bot_token=getattr(cfg, "bot_token", None),
            client_factory=self._client_factory,
            connect_timeout=self._connect_timeout,
        )
        listener.set_database(self.db)
        listener.set_monitored_chats(cfg.monitored_chat_ids)
        listener.set_callbacks(
            on_signal=self._handle_signal,
            on_status=self._handle_status,
        )
        return listener

    def start(self) -> dict:
        cfg = self.settings_service.load_telegram_config()

        if not cfg.enabled:
            self.intentional_stop = True
            self._set_state("STOPPED")
            self.connected = False
            return {
                "started": False,
                "reason": "telegram_disabled",
                "state": self.state,
            }

        # Riallinea lo stato-cache col runtime REALE prima dell'idempotenza: uno
        # `self.state` stale "CONNECTED" (dopo morte o degrado permanente del
        # transport, non ancora rinfrescato) NON deve far tornare already_running
        # su un runtime in realtà morto, impedendo il recovery. Il listener/adapter
        # riporta lo stato effettivo (FAILED su ingestione morta).
        if self.listener is not None:
            self._refresh_runtime_truth_from_listener()

        # Idempotenza PRIMA della selezione sorgente (epica #374 PR-5a): un
        # servizio GIÀ in esecuzione NON deve rivalutare il gate Bot API. Se la
        # config DB cambiasse a runtime (2° bot attivato, bot disattivato), una
        # start() ridondante che rivaluta il gate solleverebbe e forzerebbe FAILED
        # su un runtime SANO (transport vivo), innescando un autoheal inutile con
        # possibile perdita d'ingestione. Il conteggio chat qui viene dal listener
        # in esecuzione (non da una nuova selezione).
        if self.state in {"CONNECTING", "CONNECTED", "RECONNECTING"}:
            return {
                "started": True,
                "reason": "already_running",
                "chat_count": self._running_chat_count(),
                "state": self.state,
            }

        if self.listener and str(getattr(self.listener, "state", "")) in {"CONNECTING", "CONNECTED", "RECONNECTING"}:
            return {
                "started": True,
                "reason": "already_running",
                "chat_count": self._running_chat_count(),
                "state": self.state,
            }

        old_thread = getattr(self.listener, "_runtime_thread", None) if self.listener else None
        if old_thread is not None and old_thread.is_alive():
            self.last_error = "previous_runtime_still_alive"
            self._set_state("FAILED")
            self.connected = False
            return {"started": False, "reason": "previous_runtime_still_alive", "state": self.state}

        # Selezione sorgente di ingestione. Il path Telethon userbot
        # (api_id/api_hash) resta prioritario e INVARIATO. Solo quando le
        # credenziali userbot mancano si tenta il path Bot API (bot_token), che
        # NON richiede api_id/api_hash: è questo il gate rilassato. Eseguito DOPO
        # l'idempotenza => non fa mai fallire un runtime già attivo.
        bot_api_usable = None
        if not cfg.api_id or not cfg.api_hash:
            try:
                # UNA lettura => (active_count, usable) coerenti. Errori DB PROPAGANO.
                active_count, usable = self._select_bot_api_source()
            except Exception as exc:
                # Errore DB nel determinare il set di bot: NON avviare un set
                # parziale (fail-closed). Meglio non partire e ritentare che
                # droppare silenziosamente una sorgente.
                logger.warning("[TelegramService] selezione bot Bot API fallita: %s", exc)
                self.last_error = "telegram_bot_config_read_error"
                self.intentional_stop = False
                self._set_state("FAILED")
                raise RuntimeError(self.last_error)
            # Bot ATTIVI ma non-usable (sole chat non numeriche, @username rimandato):
            # SURFACED (visibile in status), non droppati in silenzio né bloccanti.
            self._unusable_active_bot_count = max(0, int(active_count) - len(usable))
            if len(usable) >= 1:
                # PR-5b: uno o PIÙ bot usable => si avvia l'ingestione (runtime
                # singolo o orchestratore N-bot). Rimpiazza il fail-closed
                # 'multi_bot_runtime_not_yet_supported' di PR-5a.
                bot_api_usable = usable
            else:
                # Nessun bot usable (0 attivi, o attivi senza chat numeriche):
                # fail-closed come prima.
                self.last_error = "Configurazione Telegram incompleta"
                self.intentional_stop = False
                self._set_state("FAILED")
                raise RuntimeError(self.last_error)

        # Conteggio chat della sorgente selezionata per la risposta di avvio:
        # sul path Bot API le chat vivono nei bot usable (somma su N), NON in
        # cfg.monitored_chat_ids (lista userbot, vuota qui).
        active_chat_count = (
            sum(len(chat_ids) for _bot, chat_ids in bot_api_usable)
            if bot_api_usable is not None
            else len(cfg.monitored_chat_ids)
        )

        try:
            self.intentional_stop = False
            self.reconnect_in_progress = False
            self._set_state("CONNECTING")
            if bot_api_usable is not None:
                # Path Bot API: 1 bot => runtime singolo (PR-5a); >1 bot =>
                # orchestratore N-bot (PR-5b). Nessun Telethon/api_id/api_hash.
                if len(bot_api_usable) == 1:
                    bot, chat_ids = bot_api_usable[0]
                    self.listener = self._build_botapi_runtime(bot, chat_ids)
                    self.handlers_registered = 1
                else:
                    self.listener = self._build_multibot_runtime(bot_api_usable)
                    # N bot usable = N handler runtime (uno per transport sano).
                    self.handlers_registered = len(bot_api_usable)
            else:
                self.listener = self._build_telethon_listener(cfg)
                self.handlers_registered = sum(
                    1 for cb in (self._handle_signal, self._handle_status) if callable(cb)
                )

            start_result = self.listener.start()
            started_ok = bool(start_result.get("started", False))
            self.last_error = str(start_result.get("error") or "")
            self._refresh_runtime_truth_from_listener()
            if self.state == "CREATED":
                self._set_state("STOPPED")
            if self.last_error:
                self._set_state("FAILED")
                self.connected = False

            return {
                # Esito dal risultato di start, NON dal flag runtime
                # listener_started (che il refresh riallinea al listener,
                # dove resta True anche per una startup fallita).
                "started": started_ok and not self.last_error,
                "chat_count": active_chat_count,
                "state": self.state,
                "connected": self.connected,
            }

        except Exception as exc:
            self.connected = False
            # handlers_registered è impostato PRIMA di listener.start() (1 sul path
            # Bot API, 2 sul Telethon). Se start() ha (parzialmente) avviato un
            # thread e poi ha sollevato, NON basta perdere il riferimento: un thread
            # orfano resterebbe vivo e un retry creerebbe un SECONDO listener (doppio
            # getUpdates/409, segnali di betting duplicati). Best-effort stop, poi:
            # - thread MORTO dopo lo stop => azzera listener+handler (snapshot pulito
            #   e coerente: FAILED => 0 handler);
            # - thread ANCORA VIVO => TIENI il riferimento (il guard
            #   previous_runtime_still_alive lo vede e blocca il retry) e mantieni
            #   handlers_registered=1 (residuo NON nascosto).
            residual_alive = self._stop_partial_listener_on_start_failure()
            if not residual_alive:
                self.listener = None
                self.handlers_registered = 0
            self.last_error = str(exc)
            self.intentional_stop = False
            self.reconnect_in_progress = False
            self._set_state("FAILED")
            logger.exception("Errore start Telegram listener: %s", exc)
            raise

    def stop(self) -> dict:
        if self.state == "STOPPED" and not self.listener:
            self.connected = False
            return {"stopped": True, "reason": "already_stopped", "state": self.state}

        self.intentional_stop = True
        self.reconnect_in_progress = False
        if self.listener:
            stop_result = {}
            try:
                stop_result = self.listener.stop() or {}
            except Exception as exc:
                logger.warning("Errore stop Telegram listener: %s", exc)
            # Se il runtime del listener non e' davvero uscito, NON va
            # dichiarato STOPPED ne' staccato il listener: un restart
            # creerebbe un secondo runtime sopra quello ancora vivo.
            if stop_result.get("stopped") is False:
                self.connected = False
                self.last_error = str(stop_result.get("error") or "listener_stop_failed")
                self._set_state("FAILED")
                return {"stopped": False, "error": self.last_error, "state": self.state}

        self.listener = None
        self.connected = False
        self._set_state("STOPPED")
        self.active_network_resources = 0
        return {"stopped": True, "state": self.state}

    def restart(self) -> dict:
        if self.intentional_stop:
            return {"started": False, "reason": "intentional_stop", "state": self.state}
        if self._restart_in_progress:
            return {"started": False, "reason": "restart_in_progress", "state": self.state}
        if self.state in {"CONNECTING", "RECONNECTING"}:
            return {"started": False, "reason": "connection_in_progress", "state": self.state}

        self._restart_in_progress = True
        self.intentional_stop = False
        self.reconnect_in_progress = True
        self.reconnect_attempts += 1
        self._restart_attempts_total += 1
        now_ts = self._autoheal_policy.now()
        self._last_restart_ts = now_ts
        self._restart_timestamps.append(now_ts)
        self._restart_timestamps = [
            ts for ts in self._restart_timestamps if (now_ts - ts) <= self._autoheal_policy.restart_window_sec
        ]
        self._set_state("RECONNECTING")
        try:
            stop_result = self.stop() or {}
            self.intentional_stop = False
            self.reconnect_in_progress = False
            # Stop non riuscito (runtime ancora vivo): NON avviare un secondo
            # listener sopra quello esistente; resta FAILED, riprovera' l'autoheal.
            if stop_result.get("stopped") is False:
                return {
                    "started": False,
                    "recovered": False,
                    "reason": "listener_stop_failed",
                    "state": self.state,
                }
            result = self.start()
            if not bool(result.get("started", False)):
                result["recovered"] = False
            return result
        finally:
            self._restart_in_progress = False

    # =========================================================
    # STATUS
    # =========================================================
    def status(self) -> dict:
        listener_state = str(getattr(self.listener, "state", "")) if self.listener else ""
        if listener_state:
            self.state = listener_state
        running = bool(self.listener and getattr(self.listener, "running", False))
        self.connected = self.state == "CONNECTED"
        if self.listener:
            self._refresh_runtime_truth_from_listener()
        return {
            "connected": bool(self.connected),
            "running": running,
            "state": self.state,
            "intentional_stop": bool(self.intentional_stop),
            "reconnect_attempts": int(self.reconnect_attempts),
            "reconnect_in_progress": bool(self.reconnect_in_progress),
            "last_error": self.last_error,
            "last_successful_message_ts": self.last_successful_message_ts,
            "listener_started": bool(self.listener_started),
            "handlers_registered": int(self.handlers_registered),
            "active_network_resources": int(self.active_network_resources),
            "restart_attempts_total": int(self._restart_attempts_total),
            "restart_attempts_in_window": len(self._restart_timestamps),
            "last_restart_ts": self._last_restart_ts,
            "lockout_active": bool(self._lockout_active),
            "lockout_reason": self._lockout_reason,
            "last_autoheal_action": self._last_autoheal_action,
            "last_autoheal_decision_reason": self._last_autoheal_decision_reason,
            "recovery_allowed": bool(not self._lockout_active and not self.intentional_stop),
            # Bot ATTIVI ma non-usable (sole chat non numeriche / @username
            # rimandato): SURFACED (visibile), non droppati in silenzio (PR-5b).
            "unusable_active_bot_count": int(self._unusable_active_bot_count),
        }

    def runtime_snapshot(self) -> dict:
        listener_snapshot = {}
        if self.listener and callable(getattr(self.listener, "runtime_snapshot", None)):
            listener_snapshot = self.listener.runtime_snapshot() or {}
        status = self.status()
        return {
            "state": str(status["state"]),
            "running": bool(status["running"]),
            "listener_started": bool(status["listener_started"]),
            "client_alive": bool(listener_snapshot.get("client_alive", False)),
            # Verita' runtime dal listener (handler Telethon, 0 o 1): il guard
            # richiede esattamente 1 handler quando CONNECTED; i callback
            # applicativi restano conteggiati in status().
            "handlers_registered": int(
                listener_snapshot.get("handlers_registered", status["handlers_registered"])
            ),
            # Handler ATTESI quando CONNECTED: 1 per single-bot/Telethon, N per
            # l'orchestratore N-bot (PR-5b). Alimenta l'invariant guard generalizzato.
            "expected_handlers": int(listener_snapshot.get("expected_handlers", 1)),
            "reconnect_in_progress": bool(status["reconnect_in_progress"]),
            "reconnect_attempts": int(status["reconnect_attempts"]),
            "active_network_resources": int(status["active_network_resources"]),
            "intentional_stop": bool(status["intentional_stop"]),
            "retry_loop_active": bool(status["reconnect_in_progress"]),
            "last_error": str(status["last_error"] or ""),
            # Liveness dal listener: il valore cache del service si aggiorna
            # solo via callback segnale, i messaggi non-segnale no.
            "last_successful_message_ts": (
                listener_snapshot.get("last_successful_message_ts")
                or status["last_successful_message_ts"]
            ),
        }

    def health_status(self, *, checked_at: str | None = None) -> dict:
        # now_ts e' obbligatorio per il guard: senza, ogni stato operativo
        # verrebbe marcato STALE_RUNTIME_NO_TIMESTAMP (e l'autoheal
        # riavvierebbe un listener sano). Default fail-safe: adesso.
        checked_at = checked_at or datetime.now(timezone.utc).isoformat()
        snap = self.runtime_snapshot()
        invariant_snapshot = TelegramInvariantSnapshot(
            state=str(snap["state"]),
            listener_started=bool(snap["listener_started"]),
            client_alive=bool(snap["client_alive"]),
            handlers_registered=int(snap["handlers_registered"]),
            reconnect_in_progress=bool(snap["reconnect_in_progress"]),
            reconnect_attempts=int(snap["reconnect_attempts"]),
            active_network_resources=int(snap["active_network_resources"]),
            intentional_stop=bool(snap["intentional_stop"]),
            retry_loop_active=bool(snap["retry_loop_active"]),
            running=bool(snap["running"]),
            last_error=str(snap["last_error"] or ""),
            last_successful_message_ts=snap["last_successful_message_ts"],
            now_ts=checked_at,
            expected_handlers=int(snap.get("expected_handlers", 1)),
        )
        health = self._health_probe.evaluate(invariant_snapshot, checked_at=checked_at)
        return {
            "state": health.state,
            "healthy": health.healthy,
            "degraded": health.degraded,
            "failed": health.failed,
            "last_error": health.last_error,
            "reconnect_attempts": health.reconnect_attempts,
            "reconnect_in_progress": health.reconnect_in_progress,
            "last_successful_message_ts": health.last_successful_message_ts,
            "handlers_registered": health.handlers_registered,
            "client_alive": health.client_alive,
            "intentional_stop": health.intentional_stop,
            "invariant_ok": health.invariant_ok,
            "active_alert_codes": list(health.active_alert_codes),
            "checked_at": health.checked_at,
        }

    def get_sender(self):
        sender = getattr(self, "sender", None)
        if sender is not None:
            return sender
        if callable(getattr(self, "send_alert_message", None)):
            return self
        return None

    def evaluate_autoheal(
        self,
        *,
        checked_at_ts: float | None,
        startup_grace_active: bool,
        reconnect_grace_active: bool,
        failure_escalated: bool,
    ) -> TelegramAutohealDecision:
        now_ts = float(checked_at_ts if checked_at_ts is not None else self._autoheal_policy.now())
        health = self.health_status(
            checked_at=datetime.fromtimestamp(now_ts, tz=timezone.utc).isoformat()
        )
        snapshot = TelegramAutohealSnapshot(
            state=str(health.get("state") or self.state),
            invariant_ok=bool(health.get("invariant_ok", True)),
            active_alert_codes=tuple(str(c) for c in (health.get("active_alert_codes") or [])),
            reconnect_attempts=int(health.get("reconnect_attempts", 0) or 0),
            restart_attempts_total=int(self._restart_attempts_total),
            restart_in_progress=bool(self._restart_in_progress),
            intentional_stop=bool(health.get("intentional_stop", self.intentional_stop)),
            startup_grace_active=bool(startup_grace_active),
            reconnect_grace_active=bool(reconnect_grace_active),
            lockout_active=bool(self._lockout_active),
            last_error_category=str(health.get("last_error") or ""),
            failure_escalated=bool(failure_escalated),
            listener_stale="STALE_RUNTIME" in set(health.get("active_alert_codes") or []),
            now_ts=now_ts,
        )
        history = TelegramAutohealHistory(
            restart_timestamps=tuple(self._restart_timestamps),
            lockout_since_ts=self._lockout_since_ts,
        )
        decision = self._autoheal_policy.evaluate(snapshot, history)
        self._last_autoheal_action = decision.action.value
        self._last_autoheal_decision_reason = decision.reason
        if decision.action == TelegramAutohealAction.ENTER_FAILED_LOCKOUT:
            self._lockout_active = True
            self._lockout_since_ts = now_ts
            self._lockout_reason = decision.reason
            # Diagnostica: l'ingresso in lockout SOSPENDE il recovery (niente piu'
            # restart automatici). Prima era invisibile: Telegram restava giu' e
            # nei log non risultava perche' non si riprendeva.
            logger.error(
                "[TelegramService] autoheal ENTER_FAILED_LOCKOUT: recovery sospeso "
                "(reason=%s, failure_class=%s)",
                decision.reason,
                decision.failure_class.value,
            )
        elif self._lockout_active and self._lockout_since_ts is not None:
            if (now_ts - self._lockout_since_ts) >= self._autoheal_policy.lockout_sec:
                self._lockout_active = False
                self._lockout_since_ts = None
                self._lockout_reason = ""
                logger.info("[TelegramService] autoheal lockout scaduto: recovery riabilitato")
        return decision

    def run_autoheal_once(
        self,
        *,
        checked_at_ts: float | None,
        startup_grace_active: bool,
        reconnect_grace_active: bool,
        failure_escalated: bool,
    ) -> dict:
        decision = self.evaluate_autoheal(
            checked_at_ts=checked_at_ts,
            startup_grace_active=startup_grace_active,
            reconnect_grace_active=reconnect_grace_active,
            failure_escalated=failure_escalated,
        )
        if decision.action == TelegramAutohealAction.SCHEDULE_RESTART:
            # Diagnostica: logga la DECISIONE PRIMA di restart(), cosi' resta
            # tracciata anche se restart() solleva (altrimenti il fallimento
            # tornerebbe silenzioso — proprio il problema che questo log risolve).
            logger.warning(
                "[TelegramService] autoheal SCHEDULE_RESTART (reason=%s, failure_class=%s)",
                decision.reason,
                decision.failure_class.value,
            )
            restarted = self.restart()
            # restart() ritorna un dict per contratto. Se il contratto e' violato
            # NON si maschera in silenzio (si LOGGA a ERROR) ma nemmeno si fa
            # fail-hard nel path di recovery: si degrada in modo osservabile
            # (restart_result vuoto) senza uccidere il loop di autoheal.
            if not isinstance(restarted, dict):
                logger.error(
                    "[TelegramService] restart() ha restituito un tipo inatteso "
                    "(%s) nel path autoheal; degrado a esito vuoto",
                    type(restarted).__name__,
                )
                restarted = {}
            if not restarted.get("started"):
                logger.error(
                    "[TelegramService] autoheal restart NON avviato (reason=%s)",
                    decision.reason,
                )
            return {
                "action": decision.action.value,
                "reason": decision.reason,
                "failure_class": decision.failure_class.value,
                "restart_result": dict(restarted),
            }
        return {
            "action": decision.action.value,
            "reason": decision.reason,
            "failure_class": decision.failure_class.value,
        }
