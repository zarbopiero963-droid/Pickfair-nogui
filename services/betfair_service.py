from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from betfair_client import BetfairClient
from simulation_broker import SimulationBroker

logger = logging.getLogger(__name__)


class BetfairService:
    """
    Service unificato broker:
    - LIVE  -> BetfairClient
    - SIM   -> SimulationBroker

    Il broker attivo dipende da simulation_mode.
    """

    def __init__(self, settings_service):
        self.settings_service = settings_service

        self.client: Optional[BetfairClient] = None
        self.simulation_broker: Optional[SimulationBroker] = None

        self.connected = False
        self.last_error = ""
        self.simulation_mode = False

        # Session expiry state — set when SESSION_EXPIRED is detected;
        # cleared only on successful re-auth.
        self._session_invalid: bool = False
        self._session_invalid_reason: str = ""
        # Bounded re-auth: max 1 attempt per expiry event to avoid loops.
        self._reauth_attempts: int = 0
        self._MAX_REAUTH_ATTEMPTS: int = 1
        # Serializza handle_session_expiry: il keepalive thread e il path di
        # order-submission possono rilevare l'expiry insieme; senza lock due
        # recovery simultanee passerebbero entrambe il gate "1 tentativo" e
        # _connect_live(force) di una distruggerebbe il client fresco dell'altra.
        # L'epoch de-duplica: chi ottiene il lock dopo un re-auth gia' avvenuto
        # non ri-autentica di nuovo.
        self._reauth_lock = threading.Lock()
        self._reauth_epoch: int = 0

        # Betting-session keepalive (B2 / Fase 1.2): la sessione betting scade
        # ~20 min di inattivita'; un loop ~10 min tocca la sessione con
        # client.keep_alive() (getAccountFunds leggero). Lo streaming ha gia' il
        # suo keepalive separato; questo copre la sessione REST/betting.
        self._keepalive_interval: float = 600.0
        self._keepalive_thread: Optional[threading.Thread] = None
        self._keepalive_stop_event: Optional[threading.Event] = None
        self._keepalive_lock = threading.Lock()
        self._keepalive_failure_count: int = 0
        self._last_keepalive_error: str = ""
        self._keepalive_last_ok_ts: str = ""
        # Generation del worker keepalive: ogni start/stop la incrementa. Un tick
        # in volo (keep_alive puo' bloccarsi fino al timeout del client) la
        # confronta prima di toccare metriche o instradare un re-auth, cosi' un
        # worker stale che sopravvive a un disconnect/reconnect non sporca la
        # nuova sessione ne' la sua osservabilita'.
        self._keepalive_generation: int = 0

    # =========================================================
    # SESSION EXPIRY DETECTION & RECOVERY
    # =========================================================

    @property
    def is_session_invalid(self) -> bool:
        """True if session is known-expired and live operations must be blocked."""
        return self._session_invalid

    def handle_session_expiry(self, reason: str = "SESSION_EXPIRED", abort_if=None) -> dict:
        """
        Called when a SESSION_EXPIRED or INVALID_SESSION signal is detected.

        Serializzato sotto _reauth_lock (il keepalive thread e il path di
        order-submission possono rilevare l'expiry insieme). De-dup via epoch: se
        un altro thread ha gia' completato un re-auth mentre attendevamo il lock
        e la sessione e' ora valida, non ri-autentichiamo di nuovo.

        abort_if: guard opzionale RI-VALUTATO SOTTO _reauth_lock (atomico con la
        decisione di reconnect). Il keepalive lo usa per abortire il re-auth se,
        tra il rilevamento dell'expiry e l'acquisizione del lock, e' avvenuto un
        disconnect/switch-SIMULATION (un tick stale non deve ricreare una
        sessione LIVE dopo uno stop voluto).

        FAIL-CLOSED: if re-auth fails or is not possible, the service remains
        blocked and the caller must not proceed with live orders.
        """
        epoch_before = self._reauth_epoch
        with self._reauth_lock:
            if abort_if is not None and abort_if():
                return {
                    "recovered": False,
                    "reason": reason,
                    "reauth_attempted": False,
                    "aborted_stale": True,
                }
            if (
                self._reauth_epoch != epoch_before
                and self.connected
                and not self._session_invalid
            ):
                return {
                    "recovered": True,
                    "reason": reason,
                    "reauth_attempted": False,
                    "already_recovered": True,
                }
            return self._do_handle_session_expiry(reason)

    def _do_handle_session_expiry(self, reason: str = "SESSION_EXPIRED") -> dict:
        """Corpo del re-auth bounded fail-closed (eseguito sotto _reauth_lock).

        1. Marks session as invalid.
        2. Sets connected=False.
        3. Attempts one bounded re-auth if password is loadable; otherwise stays blocked.
        4. Returns a structured result dict.
        """
        self.connected = False
        self._session_invalid = True
        self._session_invalid_reason = reason
        if self.client:
            try:
                self.client.connected = False
                self.client.session_token = ""
            except Exception:
                pass

        logger.warning(
            "betfair_service: session expiry detected reason=%r; "
            "blocking live operations",
            reason,
        )

        if self._reauth_attempts >= self._MAX_REAUTH_ATTEMPTS:
            msg = (
                f"session expiry: max re-auth attempts "
                f"({self._MAX_REAUTH_ATTEMPTS}) exhausted — "
                "live operations permanently blocked until manual restart"
            )
            logger.error("betfair_service: %s", msg)
            self.last_error = msg
            return {
                "recovered": False,
                "reason": reason,
                "reauth_attempted": False,
                "error": msg,
            }

        self._reauth_attempts += 1

        # Try to load password from settings for re-auth
        try:
            password = self.settings_service.load_password()
        except Exception as exc:
            password = None
            logger.warning("betfair_service: cannot load password for re-auth: %s", exc)

        if not password:
            msg = "session expiry: no password available for re-auth — live operations blocked"
            logger.error("betfair_service: %s", msg)
            self.last_error = msg
            return {
                "recovered": False,
                "reason": reason,
                "reauth_attempted": False,
                "error": msg,
            }

        try:
            result = self._connect_live(password=password, force=True)
            self._session_invalid = False
            self._session_invalid_reason = ""
            self._reauth_attempts = 0
            self._reauth_epoch += 1  # segnala il recovery agli altri thread in attesa
            logger.info("betfair_service: re-auth successful after session expiry")
            return {
                "recovered": True,
                "reason": reason,
                "reauth_attempted": True,
                "connect_result": result,
            }
        except Exception as exc:
            msg = f"session expiry: re-auth failed: {exc}"
            logger.error("betfair_service: %s", msg)
            self.connected = False
            self.last_error = msg
            return {
                "recovered": False,
                "reason": reason,
                "reauth_attempted": True,
                "error": msg,
            }

    def is_live_usable(self) -> bool:
        """
        Returns True only if the live client is connected and session is valid.
        Simulation mode is always usable regardless of session state.
        """
        if self.simulation_mode:
            return bool(self.connected and self.simulation_broker is not None)
        return bool(
            self.connected
            and self.client is not None
            and not self._session_invalid
        )

    def ensure_stream_session_ready(self) -> dict:
        """
        Stream-specific session gate.

        Reuses handle_session_expiry() as the authoritative bounded re-auth path.
        Returns a dict with ok/reason for streaming reconnect decisions.
        """
        if self.simulation_mode:
            return {"ok": False, "reason": "simulation_mode"}

        if self.client is None:
            return {"ok": False, "reason": "no_live_client"}

        if self._session_invalid:
            recovery = self.handle_session_expiry(self._session_invalid_reason or "SESSION_INVALID_STREAM_GATE")
            return {
                "ok": bool(recovery.get("recovered", False)),
                "reason": str(recovery.get("error") or recovery.get("reason") or "session_invalid"),
                "recovery": recovery,
            }

        token = str(getattr(self.client, "session_token", "") or "").strip()
        if not token:
            recovery = self.handle_session_expiry("SESSION_TOKEN_MISSING_STREAM_GATE")
            return {
                "ok": bool(recovery.get("recovered", False)),
                "reason": str(recovery.get("error") or recovery.get("reason") or "session_token_missing"),
                "recovery": recovery,
            }

        return {"ok": True, "reason": "session_ready"}

    # =========================================================
    # MODE
    # =========================================================
    def set_simulation_mode(self, enabled: bool) -> None:
        self.simulation_mode = bool(enabled)

    def is_simulation_mode(self) -> bool:
        return bool(self.simulation_mode)

    # =========================================================
    # SESSION KEEPALIVE (B2 / Fase 1.2)
    # =========================================================
    @staticmethod
    def _is_session_expiry_error(message: str) -> bool:
        # Normalizza spazi -> underscore: Betfair (o il client) puo' propagare la
        # forma con spazi ("SESSION EXPIRED", "session expired") come API_ERROR;
        # senza normalizzare verrebbe trattata come errore generico soft e il
        # re-auth proattivo non partirebbe fino a una successiva betting call.
        text = str(message or "").upper().replace(" ", "_")
        return (
            "SESSION_EXPIRED" in text
            or "INVALID_SESSION" in text
            or "NO_SESSION" in text
            or "SESSION_TIMEOUT" in text
            or "EXPIRED_SESSION" in text
        )

    def _start_session_keepalive(self) -> None:
        """Avvia (idempotente) il loop di keepalive della sessione betting.

        Ogni avvio bumpa la generation e crea un proprio stop_event, entrambi
        passati al thread: un worker vecchio (es. lasciato dal ramo di re-auth, o
        ancora bloccato in keep_alive) si ferma sul suo evento e diventa stale per
        generation, mentre quello nuovo gira sul suo.
        """
        with self._keepalive_lock:
            if self._keepalive_thread is not None and self._keepalive_thread.is_alive():
                return
            self._keepalive_generation += 1
            generation = self._keepalive_generation
            stop_event = threading.Event()
            self._keepalive_stop_event = stop_event
            thread = threading.Thread(
                target=self._session_keepalive_loop,
                args=(stop_event, generation),
                name="betfair-session-keepalive",
                daemon=True,
            )
            self._keepalive_thread = thread
            thread.start()

    def _stop_session_keepalive(self) -> None:
        """Ferma il loop di keepalive.

        RE-ENTRANCY-SAFE: se chiamato dallo stesso thread di keepalive (via
        handle_session_expiry -> _connect_live(force) -> disconnect) NON fa
        self-join (eviterebbe un deadlock). Bumpa la generation, cosi' un worker
        ancora in volo (bloccato in keep_alive) diventa STALE e non tocchera'
        piu' metriche/recovery anche se sopravvive al join best-effort.
        """
        with self._keepalive_lock:
            stop_event = self._keepalive_stop_event
            thread = self._keepalive_thread
            self._keepalive_generation += 1
            self._keepalive_thread = None
            self._keepalive_stop_event = None
        if stop_event is not None:
            stop_event.set()
        if (
            thread is not None
            and thread.is_alive()
            and thread is not threading.current_thread()
        ):
            thread.join(timeout=5.0)

    def _session_keepalive_loop(self, stop_event: threading.Event, generation: int) -> None:
        while not stop_event.wait(self._keepalive_interval):
            if not self._keepalive_tick(stop_event, generation):
                return

    def _keepalive_tick(
        self, stop_event: Optional[threading.Event] = None, generation: Optional[int] = None
    ) -> bool:
        """Una iterazione di keepalive.

        Ritorna True se il loop deve CONTINUARE, False se deve USCIRE
        (session-expiry instradato al re-auth fail-closed, oppure worker stale).
        """
        # Worker stale (un nuovo start/stop ha bumpato la generation): esci senza
        # toccare metriche/recovery della nuova sessione.
        if generation is not None and generation != self._keepalive_generation:
            return False
        # Niente keepalive in simulazione o se la sessione e' gia' nota invalida.
        if self.simulation_mode or self._session_invalid:
            return True
        client = self.client
        if client is None:
            return True
        try:
            # self.client e' un BetfairClient (che espone keep_alive): chiamata
            # diretta (no getattr/callable) — piu' idiomatica e senza ambiguita'.
            client.keep_alive()
            # keep_alive puo' bloccarsi a lungo: se nel frattempo siamo diventati
            # stale (disconnect/reconnect), non sporcare le metriche della nuova
            # sessione.
            if generation is not None and generation != self._keepalive_generation:
                return False
            with self._keepalive_lock:
                self._last_keepalive_error = ""
                self._keepalive_last_ok_ts = datetime.utcnow().isoformat()
            return True
        except Exception as exc:
            if generation is not None and generation != self._keepalive_generation:
                return False  # stale: niente metriche/re-auth sulla nuova sessione
            return self._route_keepalive_failure(client, str(exc), stop_event, generation)

    def _route_keepalive_failure(
        self,
        client: Any,
        message: str,
        stop_event: Optional[threading.Event],
        generation: Optional[int] = None,
    ) -> bool:
        """Classifica un fallimento di keep_alive. Ritorna True (loop continua)
        per errori generici (soft); False (loop esce) per session-expiry."""
        with self._keepalive_lock:
            self._keepalive_failure_count += 1
            self._last_keepalive_error = message
        logger.warning("betfair session keep_alive failed: %s", message)
        if not self._is_session_expiry_error(message):
            return True  # errore generico: soft, il loop continua

        def _stale() -> bool:
            # Stato cambiato dopo che questo tick ha catturato il client: shutdown
            # (stop settato), switch a SIMULATION, client live diverso, o
            # generation avanzata (disconnect/reconnect). Un tick stale non deve
            # ricreare una sessione LIVE dopo uno stop/switch voluto.
            return bool(
                (stop_event is not None and stop_event.is_set())
                or self.simulation_mode
                or self.client is not client
                or (generation is not None and generation != self._keepalive_generation)
            )

        # Fast-path: se gia' stale, esci senza nemmeno entrare nel re-auth.
        if _stale():
            return False
        try:
            # _stale ri-valutato DENTRO _reauth_lock (atomico con la decisione di
            # reconnect): se un disconnect/switch si infila qui, il re-auth aborta.
            self.handle_session_expiry(reason="KEEPALIVE_SESSION_EXPIRED", abort_if=_stale)
        except Exception:
            logger.exception("betfair keepalive: handle_session_expiry raised")
        return False

    def keepalive_status(self) -> Dict[str, Any]:
        with self._keepalive_lock:
            running = bool(self._keepalive_thread is not None and self._keepalive_thread.is_alive())
            return {
                "running": running,
                "interval_seconds": float(self._keepalive_interval),
                "failure_count": int(self._keepalive_failure_count),
                "last_error": str(self._last_keepalive_error),
                "last_ok_ts": str(self._keepalive_last_ok_ts),
            }

    # =========================================================
    # BROKER GETTERS
    # =========================================================
    def get_client(self):
        if self.simulation_mode:
            return self.simulation_broker
        return self.client

    def get_live_client(self) -> Optional[BetfairClient]:
        return self.client

    def get_simulation_broker(self) -> Optional[SimulationBroker]:
        return self.simulation_broker

    # =========================================================
    # CONNECT / DISCONNECT
    # =========================================================
    def connect(
        self,
        password: str | None = None,
        force: bool = False,
        simulation_mode: bool | None = None,
    ) -> dict:
        if simulation_mode is not None:
            self.set_simulation_mode(simulation_mode)

        if self.simulation_mode:
            return self._connect_simulation(force=force)

        return self._connect_live(password=password, force=force)

    def _connect_live(self, password: str | None = None, force: bool = False) -> dict:
        if self.connected and self.client and not force and not self.simulation_mode:
            return {
                "connected": True,
                "reason": "already_connected",
                "simulated": False,
            }

        if force:
            try:
                self.disconnect()
            except Exception:
                pass

        cfg = self.settings_service.load_betfair_config()
        if (
            not cfg.username
            or not cfg.app_key
            or not cfg.certificate
            or not cfg.private_key
        ):
            self.last_error = "Configurazione Betfair incompleta"
            raise RuntimeError(self.last_error)

        password = (
            password
            if password is not None
            else self.settings_service.load_password()
        )
        if not password:
            self.last_error = "Password Betfair mancante"
            raise RuntimeError(self.last_error)

        try:
            client = BetfairClient(
                username=cfg.username,
                app_key=cfg.app_key,
                cert_pem=cfg.certificate,
                key_pem=cfg.private_key,
            )

            session_info = client.login(password=password)

            self.client = client
            self.simulation_broker = None
            self.connected = True
            self.last_error = ""
            self.simulation_mode = False

            db = getattr(self.settings_service, "db", None)
            if db and hasattr(db, "save_session"):
                db.save_session(
                    session_info.get("session_token", ""),
                    session_info.get("expiry", ""),
                )

            # Avvia il keepalive della sessione betting (idempotente: il ramo di
            # re-auth ri-entra qui e non duplica il thread).
            self._start_session_keepalive()

            return {
                "connected": True,
                "session": session_info,
                "simulated": False,
            }

        except Exception as exc:
            self.client = None
            self.connected = False
            self.last_error = str(exc)
            logger.exception("Errore connect LIVE Betfair: %s", exc)
            raise

    def _connect_simulation(self, force: bool = False) -> dict:
        if self.connected and self.simulation_broker and not force and self.simulation_mode:
            return {
                "connected": True,
                "reason": "already_connected",
                "simulated": True,
            }

        if force:
            try:
                self.disconnect()
            except Exception:
                pass

        try:
            sim_cfg = self._load_simulation_config()

            broker = SimulationBroker(
                starting_balance=float(sim_cfg.get("starting_balance", 1000.0) or 1000.0),
                commission_pct=float(sim_cfg.get("commission_pct", 4.5) or 4.5),
                partial_fill_enabled=bool(sim_cfg.get("partial_fill_enabled", True)),
                consume_liquidity=bool(sim_cfg.get("consume_liquidity", True)),
                db=getattr(self.settings_service, "db", None),
            )

            if bool(sim_cfg.get("persist_state", True)):
                persisted_state = self._load_persisted_simulation_state()
                if persisted_state:
                    try:
                        broker.state.load_from_dict(persisted_state)
                    except Exception:
                        logger.exception("Errore load persisted simulation state")

            session_info = broker.login(password="SIMULATION")

            self.simulation_broker = broker
            self.client = None
            self.connected = True
            self.last_error = ""
            self.simulation_mode = True

            return {
                "connected": True,
                "session": session_info,
                "simulated": True,
                "starting_balance": float(sim_cfg.get("starting_balance", 1000.0) or 1000.0),
                "commission_pct": float(sim_cfg.get("commission_pct", 4.5) or 4.5),
                "partial_fill_enabled": bool(sim_cfg.get("partial_fill_enabled", True)),
                "consume_liquidity": bool(sim_cfg.get("consume_liquidity", True)),
                "persist_state": bool(sim_cfg.get("persist_state", True)),
            }

        except Exception as exc:
            self.simulation_broker = None
            self.connected = False
            self.last_error = str(exc)
            logger.exception("Errore connect SIMULATION broker: %s", exc)
            raise

    def disconnect(self) -> None:
        # Ferma il keepalive PRIMA del logout (re-entrancy-safe: niente self-join
        # se invocato dal thread di keepalive via re-auth).
        self._stop_session_keepalive()

        self._persist_simulation_state_if_needed()

        if self.simulation_broker:
            try:
                self.simulation_broker.logout()
            except Exception as exc:
                logger.warning("Errore logout SimulationBroker: %s", exc)

        if self.client:
            try:
                self.client.logout()
            except Exception as exc:
                logger.warning("Errore logout Betfair: %s", exc)

        self.simulation_broker = None
        self.client = None
        self.connected = False

        db = getattr(self.settings_service, "db", None)
        if db and hasattr(db, "clear_session"):
            try:
                db.clear_session()
            except Exception:
                pass

    def ensure_connected(
        self,
        password: str | None = None,
        simulation_mode: bool | None = None,
    ):
        # If session is known-invalid and this is a live request, refuse
        live_requested = (simulation_mode is False) or (
            simulation_mode is None and not self.simulation_mode
        )
        if live_requested and self._session_invalid:
            raise RuntimeError(
                f"LIVE_BLOCKED_SESSION_INVALID: {self._session_invalid_reason}"
            )

        broker = self.get_client()
        if self.connected and broker is not None:
            if simulation_mode is None or bool(simulation_mode) == self.simulation_mode:
                return broker

        self.connect(password=password, simulation_mode=simulation_mode)
        return self.get_client()

    # =========================================================
    # ACCOUNT FUNDS / STATUS
    # =========================================================
    def get_account_funds(self) -> dict:
        broker = self.get_client()
        if not broker:
            return {
                "available": 0.0,
                "exposure": 0.0,
                "total": 0.0,
                "simulated": bool(self.simulation_mode),
            }

        try:
            funds = broker.get_account_funds() or {}
            return {
                "available": float(funds.get("available", 0.0) or 0.0),
                "exposure": float(funds.get("exposure", 0.0) or 0.0),
                "total": float(funds.get("total", 0.0) or 0.0),
                "simulated": bool(funds.get("simulated", self.simulation_mode)),
            }
        except Exception as exc:
            error_text = str(exc)
            self.last_error = error_text
            if "SESSION_EXPIRED" in error_text.upper() or "INVALID_SESSION" in error_text.upper():
                logger.warning(
                    "betfair_service: session expiry detected in get_account_funds; invoking recovery"
                )
                self.handle_session_expiry(reason=error_text)
            else:
                logger.exception("Errore get_account_funds: %s", exc)
            return {
                "available": 0.0,
                "exposure": 0.0,
                "total": 0.0,
                "simulated": bool(self.simulation_mode),
            }

    def list_current_orders(
        self, market_ids: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Return current (unmatched/active) orders as a list of order dicts.

        Used by the reconciliation engine's startup ghost-order hook (B3 /
        UFA-005). **Fail-closed in LIVE**: never returns a silent empty list to
        mask a fetch failure — a known-invalid session or a missing live client
        raises, and a SESSION_EXPIRED during the fetch routes bounded recovery
        and then re-raises. Only the simulation broker path returns its orders
        directly. Callers (state_recovery) treat the raised error as
        "ghost reconciliation REQUIRED but NOT completed", never as "no orders".

        Rows are normalized for the startup recovery store, which keys on
        snake_case ``order_id``/``bet_id`` (``state_recovery._is_missing_in_db``)
        — without it a persisted live order (raw Betfair ``betId``) would be
        treated as missing on every restart and re-flagged as a ghost.
        """
        broker = self.get_client()

        if self.simulation_mode:
            if not broker:
                return []
            orders = broker.get_current_orders(market_ids)
            return [
                self._normalize_startup_order(o)
                for o in (orders or [])
                if isinstance(o, dict)
            ]

        # LIVE — fail-closed.
        if self._session_invalid:
            raise RuntimeError(
                f"LIVE_BLOCKED_SESSION_INVALID: {self._session_invalid_reason}"
            )
        if not broker:
            raise RuntimeError("NO_LIVE_CLIENT")

        try:
            orders = broker.get_current_orders(market_ids)
        except Exception as exc:
            error_text = str(exc)
            self.last_error = error_text
            if self._is_session_expiry_error(error_text):
                logger.warning(
                    "betfair_service: session expiry detected in "
                    "list_current_orders; invoking recovery"
                )
                self.handle_session_expiry(reason=error_text)
            else:
                logger.exception("Errore list_current_orders: %s", exc)
            raise

        return [
            self._normalize_startup_order(o)
            for o in (orders or [])
            if isinstance(o, dict)
        ]

    @staticmethod
    def _normalize_startup_order(order: Dict[str, Any]) -> Dict[str, Any]:
        """Add snake_case ``bet_id``/``order_id``/``customer_ref`` to a row.

        Betfair's listCurrentOrders returns camelCase keys (``betId``,
        ``customerOrderRef``). The startup recovery store and dedup guard key on
        snake_case, so we mirror the value under both spellings (originals are
        preserved for the reconciliation engine's own camelCase-aware lookups).
        """
        row = dict(order)
        bet_id = str(row.get("bet_id") or row.get("betId") or "").strip()
        if bet_id:
            row.setdefault("bet_id", bet_id)
            row.setdefault("order_id", bet_id)
        ref = str(row.get("customer_ref") or row.get("customerOrderRef") or "").strip()
        if ref:
            row.setdefault("customer_ref", ref)
        return row

    def list_cleared_orders(
        self,
        *,
        bet_status: str = "SETTLED",
        market_ids: Optional[List[str]] = None,
        bet_ids: Optional[List[str]] = None,
        settled_after: Optional[str] = None,
        settled_before: Optional[str] = None,
        group_by: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Facade fail-closed di ``BetfairClient.list_cleared_orders``.

        Usata dal poller settlement del RuntimeController (PR3): su questo
        percorso un errore mascherato da "nessun settlement" è una perdita
        invisibile al daily-loss, quindi la postura è identica a
        ``list_current_orders``:

        - **SIM => raise** (``CLEARED_ORDERS_UNAVAILABLE_IN_SIMULATION``): la
          parity del broker simulato non è ancora cablata; una lista vuota
          silenziosa sarebbe un falso "nessun settlement";
        - **LIVE fail-closed**: sessione known-invalid o client assente =>
          raise; SESSION_EXPIRED durante il fetch => bounded recovery e
          re-raise; ogni altro errore propaga. Mai una lista vuota al posto
          di un errore.
        """
        broker = self.get_client()

        if self.simulation_mode:
            raise RuntimeError("CLEARED_ORDERS_UNAVAILABLE_IN_SIMULATION")

        # LIVE — fail-closed.
        if self._session_invalid:
            raise RuntimeError(
                f"LIVE_BLOCKED_SESSION_INVALID: {self._session_invalid_reason}"
            )
        if not broker:
            raise RuntimeError("NO_LIVE_CLIENT")

        try:
            cleared = broker.list_cleared_orders(
                bet_status=bet_status,
                market_ids=market_ids,
                bet_ids=bet_ids,
                settled_after=settled_after,
                settled_before=settled_before,
                group_by=group_by,
            )
        except Exception as exc:
            error_text = str(exc)
            self.last_error = error_text
            if self._is_session_expiry_error(error_text):
                logger.warning(
                    "betfair_service: session expiry detected in "
                    "list_cleared_orders; invoking recovery"
                )
                self.handle_session_expiry(reason=error_text)
            else:
                logger.exception("Errore list_cleared_orders: %s", exc)
            raise

        return [o for o in (cleared or []) if isinstance(o, dict)]

    def place_order(self, payload: dict) -> dict:
        """Session-aware live-order facade.

        Refuses immediately when the session is known-invalid (fail-closed).
        Detects SESSION_EXPIRED in the client's ok=False response and invokes
        bounded recovery via handle_session_expiry().

        Only applies to live orders — simulation orders bypass this entirely.
        """
        if self._session_invalid:
            return {
                "ok": False,
                "error": (
                    f"LIVE_BLOCKED_SESSION_INVALID: {self._session_invalid_reason}"
                ),
                "classification": "PERMANENT",
                "session_invalid": True,
            }

        if not self.client:
            return {
                "ok": False,
                "error": "NO_LIVE_CLIENT",
                "classification": "PERMANENT",
            }

        try:
            result = self.client.place_bet(
                market_id=payload.get("market_id"),
                selection_id=payload.get("selection_id"),
                side=payload.get("bet_type") or payload.get("side"),
                price=payload.get("price"),
                size=payload.get("stake") or payload.get("size"),
            )
        except Exception as exc:
            err = str(exc)
            if "SESSION_EXPIRED" in err.upper() or "INVALID_SESSION" in err.upper():
                logger.warning(
                    "betfair_service: session expiry detected in place_order "
                    "(exception path); invoking recovery"
                )
                self.handle_session_expiry(reason=err)
            raise

        # place_bet catches RuntimeError and returns ok=False — inspect it.
        if isinstance(result, dict) and not result.get("ok", True):
            err = str(result.get("error", ""))
            if "SESSION_EXPIRED" in err.upper() or "INVALID_SESSION" in err.upper():
                logger.warning(
                    "betfair_service: session expiry detected in place_order "
                    "(result path); invoking recovery"
                )
                self.handle_session_expiry(reason=err)

        return result

    def status(self) -> dict:
        broker = self.get_client()
        has_client = broker is not None
        simulated = bool(self.simulation_mode)

        return {
            "connected": bool(self.connected and has_client),
            "last_error": self.last_error,
            "has_client": has_client,
            "simulated": simulated,
            "simulation_mode": simulated,  # compatibilità con market_tracker e vecchio codice
            "broker_type": "SIMULATION" if simulated else "LIVE",
            "live_execution_only": not simulated,
        }

    # =========================================================
    # SIMULATION MARKET FEED
    # =========================================================
    def update_simulation_market_book(self, *args, **kwargs) -> dict:
        """
        Firma compatibile con entrambi gli stili:
        - update_simulation_market_book(market_book)
        - update_simulation_market_book(market_id, market_book)

        Questo chiude il mismatch trovato nel repository.
        """
        if not self.simulation_mode or not self.simulation_broker:
            return {
                "ok": False,
                "reason": "simulation_not_active",
                "simulated": False,
            }

        market_id = ""
        market_book: Dict[str, Any] = {}

        # stile nuovo: (market_book,)
        if len(args) == 1 and isinstance(args[0], dict):
            market_book = dict(args[0] or {})
            market_id = str(
                market_book.get("marketId")
                or market_book.get("market_id")
                or ""
            ).strip()

        # stile vecchio: (market_id, market_book)
        elif len(args) >= 2:
            market_id = str(args[0] or "").strip()
            market_book = dict(args[1] or {})

        # kwargs fallback
        if not market_book:
            market_book = dict(kwargs.get("market_book") or {})
        if not market_id:
            market_id = str(
                kwargs.get("market_id")
                or market_book.get("marketId")
                or market_book.get("market_id")
                or ""
            ).strip()

        if not market_id or not isinstance(market_book, dict):
            return {
                "ok": False,
                "reason": "invalid_market_book",
                "simulated": True,
            }

        normalized = dict(market_book)
        normalized["marketId"] = market_id
        normalized["market_id"] = market_id

        try:
            result = self.simulation_broker.update_market_book(market_id, normalized)
            self._persist_simulation_state_if_needed()
            return result
        except Exception as exc:
            self.last_error = str(exc)
            logger.exception("Errore update_simulation_market_book: %s", exc)
            return {
                "ok": False,
                "reason": str(exc),
                "simulated": True,
            }

    def simulation_snapshot(self) -> dict:
        if not self.simulation_broker:
            return {
                "connected": False,
                "simulated": True,
                "state": {},
            }

        try:
            return self.simulation_broker.snapshot()
        except Exception as exc:
            self.last_error = str(exc)
            logger.exception("Errore simulation_snapshot: %s", exc)
            return {
                "connected": False,
                "simulated": True,
                "state": {},
                "error": str(exc),
            }

    def get_market_book_snapshot(
        self, market_id: str, *, include_prices: bool = False
    ) -> Optional[Dict[str, Any]]:
        # ``include_prices`` opt-in (default False = invariato): inoltrato al
        # client LIVE per chiedere le ladder EX_BEST_OFFERS (best-price DIRECT).
        # In SIMULATION il broker restituisce gia' un book completo: il flag e'
        # ininfluente e non viene propagato.
        if self.simulation_mode:
            if not self.simulation_broker:
                return None
            try:
                return self.simulation_broker.get_market_book(str(market_id)) or None
            except Exception:
                logger.exception("Errore get_market_book_snapshot simulation")
                return None

        if not self.client:
            return None

        try:
            return self.client.get_market_book(
                str(market_id), include_prices=include_prices
            ) or None
        except Exception as exc:
            error_text = str(exc)
            if "SESSION_EXPIRED" in error_text.upper() or "INVALID_SESSION" in error_text.upper():
                logger.warning(
                    "betfair_service: session expiry detected in get_market_book_snapshot; invoking recovery"
                )
                self.handle_session_expiry(reason=error_text)
            else:
                logger.exception("Errore get_market_book_snapshot: %s", exc)
            return None

    def reset_simulation(self, starting_balance: float | None = None) -> dict:
        if not self.simulation_broker:
            return {
                "ok": False,
                "reason": "simulation_not_initialized",
                "simulated": True,
            }

        try:
            result = self.simulation_broker.reset(starting_balance=starting_balance)
            self._persist_simulation_state_if_needed(force=True)
            return result
        except Exception as exc:
            self.last_error = str(exc)
            logger.exception("Errore reset_simulation: %s", exc)
            return {
                "ok": False,
                "reason": str(exc),
                "simulated": True,
            }

    # =========================================================
    # INTERNAL SETTINGS HELPERS
    # =========================================================
    def _load_simulation_config(self) -> dict:
        if hasattr(self.settings_service, "load_simulation_config"):
            try:
                return self.settings_service.load_simulation_config() or {}
            except Exception:
                logger.exception("Errore load_simulation_config")
        return {
            "enabled": True,
            "starting_balance": 1000.0,
            "commission_pct": 4.5,
            "partial_fill_enabled": True,
            "consume_liquidity": True,
            "persist_state": True,
        }

    def _persist_simulation_state_if_needed(self, force: bool = False) -> None:
        if not self.simulation_broker:
            return

        sim_cfg = self._load_simulation_config()
        if not force and not bool(sim_cfg.get("persist_state", True)):
            return

        if hasattr(self.settings_service, "save_simulation_state"):
            try:
                self.settings_service.save_simulation_state(
                    self.simulation_broker.state.to_dict(),
                    state_key="default",
                )
            except Exception:
                logger.exception("Errore save_simulation_state")

    def _load_persisted_simulation_state(self) -> dict:
        if hasattr(self.settings_service, "load_simulation_state"):
            try:
                return self.settings_service.load_simulation_state(state_key="default") or {}
            except Exception:
                logger.exception("Errore load_simulation_state")
        return {}
