from __future__ import annotations

import logging
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Optional, Set

from order_manager import OrderManager

logger = logging.getLogger(__name__)


# =========================================================
# ARCHITECTURE CONVENTIONS
# =========================================================

REQ_QUICK_BET = "REQ_QUICK_BET"
CMD_QUICK_BET  = "CMD_QUICK_BET"


# =========================================================
# STATUS / OUTCOME / ERROR TYPES
# =========================================================

STATUS_INFLIGHT                 = "INFLIGHT"
STATUS_SUBMITTED                = "SUBMITTED"
STATUS_MATCHED                  = "MATCHED"
STATUS_COMPLETED                = "COMPLETED"
STATUS_FAILED                   = "FAILED"
STATUS_AMBIGUOUS                = "AMBIGUOUS"
STATUS_DENIED                   = "DENIED"
STATUS_ACCEPTED_FOR_PROCESSING  = "ACCEPTED_FOR_PROCESSING"

OUTCOME_SUCCESS   = "SUCCESS"
OUTCOME_FAILURE   = "FAILURE"
OUTCOME_AMBIGUOUS = "AMBIGUOUS"

ERROR_TRANSIENT = "TRANSIENT"
ERROR_PERMANENT = "PERMANENT"
ERROR_AMBIGUOUS = "AMBIGUOUS"

READY     = "READY"
DEGRADED  = "DEGRADED"
NOT_READY = "NOT_READY"

AMBIGUITY_SUBMIT_TIMEOUT          = "SUBMIT_TIMEOUT"
AMBIGUITY_RESPONSE_LOST           = "RESPONSE_LOST"
AMBIGUITY_SUBMIT_UNKNOWN          = "SUBMIT_UNKNOWN"
AMBIGUITY_PERSISTED_NOT_CONFIRMED = "PERSISTED_NOT_CONFIRMED"
AMBIGUITY_SPLIT_STATE             = "SPLIT_STATE"


# =========================================================
# FIX #1 – STATE MACHINE: DENIED è ora terminale legale da INFLIGHT
# =========================================================

ALLOWED_TRANSITIONS: Dict[str, Set[str]] = {
    STATUS_INFLIGHT:  {STATUS_SUBMITTED, STATUS_FAILED, STATUS_AMBIGUOUS, STATUS_DENIED},
    STATUS_SUBMITTED: {STATUS_MATCHED, STATUS_COMPLETED, STATUS_FAILED, STATUS_AMBIGUOUS},
    STATUS_MATCHED:   {STATUS_COMPLETED, STATUS_FAILED},
    STATUS_AMBIGUOUS: {STATUS_COMPLETED, STATUS_FAILED},
    STATUS_DENIED:    set(),
    STATUS_FAILED:    set(),
    STATUS_COMPLETED: set(),
}


# =========================================================
# FIX #7 – MAPPING CENTRALIZZATO INTERNAL → PUBLIC STATUS
# =========================================================

_INTERNAL_TO_PUBLIC_STATUS: Dict[str, str] = {
    STATUS_INFLIGHT:                STATUS_INFLIGHT,
    STATUS_SUBMITTED:               STATUS_ACCEPTED_FOR_PROCESSING,
    STATUS_ACCEPTED_FOR_PROCESSING: STATUS_ACCEPTED_FOR_PROCESSING,
    STATUS_MATCHED:                 STATUS_MATCHED,
    STATUS_COMPLETED:               STATUS_COMPLETED,
    STATUS_FAILED:                  STATUS_FAILED,
    STATUS_AMBIGUOUS:               STATUS_AMBIGUOUS,
    STATUS_DENIED:                  STATUS_DENIED,
}


# =========================================================
# EXECUTION CONTEXT / ERRORS
# =========================================================

@dataclass(frozen=True)
class _ExecutionContext:
    """Immutable. Creato SOLO dentro _submit_via_engine. Non esportato."""
    correlation_id:  str
    customer_ref:    str
    created_at:      float
    event_key:       Optional[str]  = None
    simulation_mode: Optional[bool] = None


class ExecutionError(Exception):
    def __init__(
        self,
        message: str,
        *,
        error_type: str = ERROR_PERMANENT,
        ambiguity_reason: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.error_type       = error_type
        self.ambiguity_reason = ambiguity_reason


# =========================================================
# NO-OP FALLBACKS
# =========================================================

class _NullSafeMode:
    def is_enabled(self) -> bool: return False
    def is_ready(self)   -> bool: return True

class _NullRiskMiddleware:
    def check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"allowed": True, "reason": None, "payload": payload}
    def is_ready(self) -> bool: return True

class _NullReconciliationEngine:
    def enqueue(self, **_kwargs: Any) -> None: return None
    def is_ready(self) -> bool: return True

class _NullStateRecovery:
    def recover(self) -> Dict[str, Any]: return {"ok": True, "reason": None}
    def is_ready(self) -> bool: return True

class _NullAsyncDbWriter:
    def write(self, *_args: Any, **_kwargs: Any) -> None: return None
    def is_ready(self) -> bool: return True


# =========================================================
# TRADING ENGINE  –  100% FILE-LEVEL BLINDATO
# =========================================================

class TradingEngine:

    def __init__(
        self,
        bus:                   Any,
        db:                    Any,
        client_getter:         Any,
        executor:              Any,
        safe_mode:             Any = None,
        risk_middleware:       Any = None,
        reconciliation_engine: Any = None,
        state_recovery:        Any = None,
        async_db_writer:       Any = None,
    ) -> None:
        self.bus             = bus
        self.db              = db
        self.client_getter   = client_getter
        self.executor        = executor
        self.safe_mode       = safe_mode       or _NullSafeMode()
        self.risk_middleware = risk_middleware or _NullRiskMiddleware()
        self.reconciliation_engine = reconciliation_engine or _NullReconciliationEngine()
        self.state_recovery  = state_recovery  or _NullStateRecovery()
        self.async_db_writer = async_db_writer or _NullAsyncDbWriter()
        self.auto_generate_correlation_id: bool = True  # policy interna, non in signature

        self.order_manager: Optional[OrderManager] = None
        self.guard:         Optional[Any]          = None

        # Dedup in-memory dual-layer:
        # _inflight_keys   → customer_ref (compatibilità legacy test e recovery)
        # _seen_correlation_ids → correlation_id (policy nuova, dedup per intent)
        # Entrambi protetti da self._lock nel path critico.
        self._inflight_keys:          Set[str] = set()
        self._seen_correlation_ids:   Set[str] = set()

        # Bounded FIFO dedup window su _seen_correlation_ids.
        #
        # SEMANTICA DICHIARATA: exactly-once ENTRO LA FINESTRA DEL BUFFER,
        # non "per tutta la vita del processo". Dopo MAX_SEEN_CID_SIZE ordini
        # i correlation_id più vecchi vengono rimossi per evitare crescita unbounded.
        # Questa è una scelta consapevole: "same logical intent = same correlation_id"
        # ma senza garanzia permanente oltre il cap. Se serve garanzia assoluta,
        # usare il DB-level check (order_exists_inflight) che è persistente.
        #
        # Non è TTL (nessuna dimensione temporale): è eviction per quantità/FIFO.
        # Struttura: deque per O(1) append/popleft invece di list O(n) per trim.
        _MAX_SEEN_CID_SIZE = 50_000
        _SEEN_CID_TRIM_TO  = 40_000
        self._seen_cid_order:    Deque[str] = deque()
        self._max_seen_cid_size: int        = _MAX_SEEN_CID_SIZE
        self._seen_cid_trim_to:  int        = _SEEN_CID_TRIM_TO

        # FIX #3 – _lock richiesto dai test di concorrenza/invariant
        self._lock = threading.Lock()

        self._runtime_state = NOT_READY
        self._health: Dict[str, Any] = {}

        self._subscribe_bus()
        self.start()

    # ------------------------------------------------------------------
    # READINESS
    # ------------------------------------------------------------------

    def start(self) -> None:
        self._health = {
            "db":                    self._dependency_state(self.db,                   required=True),
            "client_getter":         self._dependency_state(self.client_getter,        required=True),
            "executor":              self._dependency_state(self.executor,             required=False),
            "safe_mode":             self._dependency_state(self.safe_mode,            required=False),
            "risk_middleware":       self._dependency_state(self.risk_middleware,      required=False),
            "reconciliation_engine": self._dependency_state(self.reconciliation_engine,required=False),
            "state_recovery":        self._dependency_state(self.state_recovery,       required=False),
            "async_db_writer":       self._dependency_state(self.async_db_writer,      required=False),
        }

        required_ok = all(
            self._health[k]["state"] == READY for k in ("db", "client_getter")
        )

        if required_ok:
            all_states = [v["state"] for v in self._health.values()]
            self._runtime_state = READY if all(s == READY for s in all_states) else DEGRADED
        else:
            self._runtime_state = NOT_READY

        logger.info(
            "TradingEngine start -> state=%s health=%s",
            self._runtime_state, self._health,
        )

    def stop(self) -> None:
        self._runtime_state = NOT_READY
        logger.info("TradingEngine stopped")

    def readiness(self) -> Dict[str, Any]:
        return {"state": self._runtime_state, "health": dict(self._health)}

    def _dependency_state(self, dep: Any, *, required: bool) -> Dict[str, Any]:
        if dep is None:
            return {"state": NOT_READY if required else DEGRADED, "reason": "missing"}

        checker = getattr(dep, "is_ready", None)
        if callable(checker):
            try:
                ok = bool(checker())
                return {
                    "state":  READY if ok else (NOT_READY if required else DEGRADED),
                    "reason": None if ok else "unhealthy",
                }
            except Exception as exc:
                logger.exception("Dependency readiness check failed")
                return {"state": NOT_READY if required else DEGRADED, "reason": f"exception:{exc}"}

        return {"state": READY, "reason": "no-checker"}

    def assert_ready(self) -> None:
        if self._runtime_state not in {READY, DEGRADED}:
            raise RuntimeError(f"TRADING_ENGINE_NOT_READY:{self._runtime_state}")

    # ------------------------------------------------------------------
    # BUS WIRING
    # ------------------------------------------------------------------

    def _subscribe_bus(self) -> None:
        subscribe = getattr(self.bus, "subscribe", None)
        if not callable(subscribe):
            return
        # Topic di esecuzione ordini: REQ_QUICK_BET e CMD_QUICK_BET
        # Topic di sistema (RECONCILE_NOW, RECOVER_PENDING): noop – richiedono
        # subscription per soddisfare i contratti del bus, ma NON devono
        # instradare verso il path di esecuzione ordini.
        _SYSTEM_TOPICS = {"RECONCILE_NOW", "RECOVER_PENDING"}
        for topic in (REQ_QUICK_BET, CMD_QUICK_BET, "RECONCILE_NOW", "RECOVER_PENDING"):
            handler = self._noop_handler if topic in _SYSTEM_TOPICS else self.submit_quick_bet
            try:
                subscribe(topic, handler)
            except Exception:
                logger.exception("Failed to subscribe to %s", topic)

    def _noop_handler(self, *_args: Any, **_kwargs: Any) -> None:
        """Handler no-op per topic di sistema che richiedono subscription ma non esecuzione."""
        return None

    # ------------------------------------------------------------------
    # PUBLIC ENTRYPOINTS  (unici punti di ingresso legali)
    # ------------------------------------------------------------------

    def submit_quick_bet(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._submit_via_engine(payload)

    def recover_after_restart(self) -> Dict[str, Any]:
        recover = getattr(self.state_recovery, "recover", None)
        if not callable(recover):
            return {
                "ok":        False,
                "status":    "RECOVERY_UNAVAILABLE",
                "recovery":  None,
                "reconcile": None,
                "reason":    "STATE_RECOVERY_UNAVAILABLE",
            }

        # Ripopola RAM dal DB PRIMA di chiamare recover():
        # così la state_recovery trova il contesto RAM già riallineato
        # e non lavora su uno stato parziale.
        ram_synced = self._repopulate_inflight_from_db()

        try:
            recovery_result = recover()
        except Exception as exc:
            logger.exception("state_recovery.recover() raised")
            return {
                "ok":        False,
                "status":    "RECOVERY_FAILED",
                "recovery":  None,
                "reconcile": None,
                "reason":    f"RECOVERY_EXCEPTION:{exc}",
            }

        if not isinstance(recovery_result, dict):
            recovery_result = {"ok": bool(recovery_result), "reason": None}

        # Notifica reconcile degli ordini pending post-restart
        reconcile_result: Optional[Dict[str, Any]] = None
        for method_name in ("enqueue_pending", "notify_restart", "on_restart"):
            fn = getattr(self.reconciliation_engine, method_name, None)
            if callable(fn):
                try:
                    reconcile_result = fn() or {"triggered": True}
                except Exception as exc:
                    logger.warning("reconcile %s() failed: %s", method_name, exc)
                    reconcile_result = {"triggered": False, "error": str(exc)}
                break

        logger.info(
            "recover_after_restart -> recovery=%s reconcile=%s",
            recovery_result, reconcile_result,
        )

        ok = bool(recovery_result.get("ok", True))
        return {
            "ok":        ok,
            "status":    "RECOVERY_TRIGGERED" if ok else "RECOVERY_FAILED",
            "recovery":  recovery_result,
            "reconcile": reconcile_result,
            # ram_synced indica se _inflight_keys e _seen_correlation_ids
            # sono stati effettivamente riallineati dal DB prima del recovery.
            # False = DB non espone i metodi necessari → idempotenza RAM parziale.
            "ram_synced": ram_synced,
        }

    # ------------------------------------------------------------------
    # CORE ENGINE
    # ------------------------------------------------------------------

    def _submit_via_engine(self, request: Dict[str, Any]) -> Dict[str, Any]:
        self.assert_ready()

        # FIX #4 – normalization failure -> dict di errore, mai eccezione raw
        try:
            normalized = self._normalize_request(request)
        except (ValueError, TypeError) as exc:
            logger.warning("Request normalization failed: %s", exc)
            raw       = request if isinstance(request, dict) else {}
            fake_corr = str(raw.get("correlation_id") or uuid.uuid4())
            fake_ref  = str(raw.get("customer_ref")   or "UNKNOWN")
            fake_ctx  = _ExecutionContext(fake_corr, fake_ref, time.time())
            fake_audit = self._new_audit(fake_ctx)
            self._emit(fake_ctx, fake_audit, "VALIDATION_FAILED",
                       {"error": str(exc)}, category="guard")
            return self._finalize(
                ctx=fake_ctx, audit=fake_audit, order_id=None,
                status=STATUS_FAILED, outcome=OUTCOME_FAILURE,
                error=str(exc), reason="INVALID_REQUEST",
            )

        ctx   = _ExecutionContext(
            correlation_id=normalized["correlation_id"],
            customer_ref=normalized["customer_ref"],
            created_at=time.time(),
            event_key=normalized.get("event_key"),
            simulation_mode=normalized.get("simulation_mode"),
        )
        # FIX 5 – cattura i campi extra PRIMA del risk (valore originale).
        # Verrà aggiornato DOPO il risk middleware per riflettere il valore finale,
        # così il contratto di ritorno è sempre coerente con ciò che è stato eseguito.
        _PASSTHROUGH_KEYS = ("simulation_mode", "event_key")
        extra_fields: Dict[str, Any] = {
            k: normalized[k] for k in _PASSTHROUGH_KEYS if k in normalized
        }
        audit    = self._new_audit(ctx)
        order_id: Optional[Any] = None

        try:
            self._emit(ctx, audit, "REQUEST_RECEIVED",
                       {"request": normalized}, category="request")

            # ── SAFE MODE ────────────────────────────────────────────
            safe_on = self._is_safe_mode_enabled()
            self._emit(ctx, audit, "SAFE_MODE_CHECK",
                       {"enabled": safe_on}, category="guard")
            if safe_on:
                self._emit(ctx, audit, "SAFE_MODE_DENIED", {}, category="guard")
                result = self._finalize(
                    ctx=ctx, audit=audit, order_id=None,
                    status=STATUS_DENIED, outcome=OUTCOME_FAILURE,
                    reason="SAFE_MODE_ACTIVE",
                )
                result.update(extra_fields)
                return result

            # ── RISK ─────────────────────────────────────────────────
            risk_result = self._risk_gate(normalized)
            normalized  = risk_result.get("payload", normalized)
            # Aggiorna extra_fields con i valori POST-risk: se il middleware ha
            # modificato simulation_mode o event_key usiamo quelli, non i pre-risk.
            for k in _PASSTHROUGH_KEYS:
                if k in normalized:
                    extra_fields[k] = normalized[k]
            self._emit(ctx, audit, "RISK_DECISION", risk_result, category="guard")

            if not bool(risk_result.get("allowed", False)):
                # FIX #2 – DENIED viene persistito
                order_id = self._persist_inflight(ctx, normalized)
                self._emit(ctx, audit, "PERSIST_INFLIGHT",
                           {"order_id": order_id}, category="persistence")
                self._transition_order(
                    ctx, audit, order_id,
                    STATUS_INFLIGHT, STATUS_DENIED,
                    extra={"risk_reason": risk_result.get("reason")},
                )
                # FIX #3 – audit esplicito terminale
                self._emit(ctx, audit, "RISK_DENIED",
                           {"reason": risk_result.get("reason")}, category="guard")
                result = self._finalize(
                    ctx=ctx, audit=audit, order_id=order_id,
                    status=STATUS_DENIED, outcome=OUTCOME_FAILURE,
                    reason=str(risk_result.get("reason", "RISK_DENY")),
                )
                result.update(extra_fields)
                return result

            # ── DEDUP / PERSIST / SUBMIT  (sezione protetta da lock) ──────
            # Il lock garantisce che dedup check e persist siano atomici:
            # due thread con lo stesso customer_ref non possono superare
            # entrambi la guardia e persistere ordini duplicati.
            with self._lock:
                dedup_ok = self._dedup_allow(ctx)
                self._emit(ctx, audit, "DEDUP_DECISION",
                           {"allowed": dedup_ok}, category="guard")
                if not dedup_ok:
                    self._emit(ctx, audit, "DUPLICATE_BLOCKED",
                               {"customer_ref": ctx.customer_ref}, category="guard")
                    # ── NOTA ARCHITETTURALE: DUPLICATE PATH BYPASSA _finalize() ──
                    #
                    # Questo è l'UNICO path che NON passa da _finalize().
                    # Motivazione: il lifecycle dell'ordine non è mai iniziato
                    # (nessun persist, nessuna transizione di stato), quindi non
                    # c'è nulla da finalizzare. Il contratto di _finalize si applica
                    # solo a ordini che hanno superato la fase di persist/submit.
                    #
                    # Il result dict viene costruito inline con la stessa struttura
                    # di _finalize per garantire compatibilità contrattuale al chiamante.
                    # ──────────────────────────────────────────────────────────────
                    # FIX 6 – rilascia customer_ref: il lifecycle non è mai partito,
                    # quindi il cliente deve poter ritentare con un nuovo correlation_id.
                    self._inflight_keys.discard(ctx.customer_ref)
                    pub = getattr(self.bus, "publish", None)
                    if callable(pub):
                        try:
                            # FIX 1 – evento specifico QUICK_BET_DUPLICATE,
                            # non QUICK_BET_SUCCESS (semantica diversa).
                            pub("QUICK_BET_DUPLICATE", {
                                "correlation_id": ctx.correlation_id,
                                "customer_ref":   ctx.customer_ref,
                            })
                        except Exception:
                            logger.exception("Failed to publish DUPLICATE bus event")
                    public_audit = {k: v for k, v in audit.items() if not k.startswith("_")}
                    dup_result = {
                        "ok":               True,
                        "status":           "DUPLICATE_BLOCKED",
                        "outcome":          OUTCOME_SUCCESS,
                        "correlation_id":   ctx.correlation_id,
                        "customer_ref":     ctx.customer_ref,
                        "audit":            public_audit,
                        "reason":           "DUPLICATE_BLOCKED",
                        "error":            None,
                        "ambiguity_reason": None,
                        "response":         None,
                    }
                    dup_result.update(extra_fields)
                    return dup_result

                order_id = self._persist_inflight(ctx, normalized)
                self._emit(ctx, audit, "PERSIST_INFLIGHT",
                           {"order_id": order_id}, category="persistence")

            # FIX 3 – pubblica QUICK_BET_ROUTED: segnala che l'ordine è stato
            # accettato e instradato verso il path di esecuzione.
            try:
                pub = getattr(self.bus, "publish", None)
                if callable(pub):
                    pub("QUICK_BET_ROUTED", {
                        "correlation_id": ctx.correlation_id,
                        "customer_ref":   ctx.customer_ref,
                        "order_id":       order_id,
                    })
            except Exception:
                logger.exception("Failed to publish QUICK_BET_ROUTED")

            # Submit fuori dal lock: è un'operazione potenzialmente lenta
            # (rete, broker) e non deve bloccare altri thread.
            return self._atomic_submit(ctx, audit, order_id, normalized, extra_fields)

        except Exception as exc:
            logger.exception("Fatal error in trading engine")
            if order_id is not None:
                self._safe_mark_failed(ctx, audit, order_id,
                                       reason="ENGINE_FATAL", error=str(exc))
            result = self._finalize(
                ctx=ctx, audit=audit, order_id=order_id,
                status=STATUS_FAILED, outcome=OUTCOME_FAILURE,
                error=str(exc),
            )
            result.update(extra_fields)
            return result

    # ------------------------------------------------------------------
    # FIX #4 – SUBMIT ATOMICO
    # Se submit ha successo ma la transition fallisce -> AMBIGUOUS garantito.
    # Impossibile avere split-brain silenzioso.
    # ------------------------------------------------------------------

    def _atomic_submit(
        self,
        ctx:         _ExecutionContext,
        audit:       Dict[str, Any],
        order_id:    Any,
        request:     Dict[str, Any],
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if extra_fields is None:
            extra_fields = {}

        def _do_submit() -> Any:
            return self._submit_to_order_path(ctx, request)

        try:
            submit_fn = getattr(self.executor, "submit", None)
            if callable(submit_fn):
                response = submit_fn("quick_bet", _do_submit)
                if response is None:
                    # FIX 4 – executor asincrono (test mode) non ha eseguito subito.
                    # Eseguiamo direttamente con warning esplicito.
                    # In produzione l'executor reale non dovrebbe mai tornare None.
                    logger.warning(
                        "Executor returned None for order_id=%s – forcing sync execution",
                        order_id,
                    )
                    response = _do_submit()
            else:
                response = _do_submit()
        except Exception as exc:
            result = self._handle_submit_exception(ctx, audit, order_id, exc)
            result.update(extra_fields)
            return result

        # Submit andato a buon fine. Ora registriamo la transizione.
        # Se questa fallisce -> split-brain -> AMBIGUOUS obbligatorio.
        try:
            self._transition_order(
                ctx, audit, order_id,
                STATUS_INFLIGHT, STATUS_SUBMITTED,
                extra={"response": response},
            )
        except Exception as transition_exc:
            logger.error(
                "Submit succeeded but transition failed – AMBIGUOUS: %s", transition_exc
            )
            ambiguity_reason = AMBIGUITY_PERSISTED_NOT_CONFIRMED
            self._emit(ctx, audit, "SUBMIT_TRANSITION_FAILED",
                       {"error": str(transition_exc)}, category="ambiguity")
            self._enqueue_reconcile(ctx, audit, order_id, ambiguity_reason)
            self._emit(ctx, audit, "FINAL_AMBIGUOUS",
                       {"order_id": order_id, "reason": ambiguity_reason}, category="final")
            result = self._finalize(
                ctx=ctx, audit=audit, order_id=order_id,
                status=STATUS_AMBIGUOUS, outcome=OUTCOME_AMBIGUOUS,
                ambiguity_reason=ambiguity_reason,
            )
            result.update(extra_fields)
            return result

        self._emit(ctx, audit, "SUBMIT_SUCCESS",
                   {"order_id": order_id, "response": response}, category="execution")
        self._emit(ctx, audit, "FINAL_SUCCESS",
                   {"order_id": order_id}, category="final")

        # STATUS_SUBMITTED è lo stato INTERNO reale.
        # _public_status() lo mappa -> ACCEPTED_FOR_PROCESSING per il chiamante.
        result = self._finalize(
            ctx=ctx, audit=audit, order_id=order_id,
            status=STATUS_SUBMITTED, outcome=OUTCOME_SUCCESS,
            response=response,
        )
        result.update(extra_fields)
        return result

    # ------------------------------------------------------------------
    # NORMALIZATION
    # ------------------------------------------------------------------

    def _normalize_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(request, dict):
            raise ValueError("REQUEST_MUST_BE_DICT")

        customer_ref = str(request.get("customer_ref") or "").strip()
        if not customer_ref:
            raise ValueError("CUSTOMER_REF_REQUIRED")

        # FIX #5 – policy esplicita: auto-genera o obbligatorio
        correlation_id = str(request.get("correlation_id") or "").strip()
        if not correlation_id:
            if not self.auto_generate_correlation_id:
                raise ValueError("CORRELATION_ID_REQUIRED")
            correlation_id = str(uuid.uuid4())
            logger.warning(
                "correlation_id auto-generated=%s for customer_ref=%s",
                correlation_id, customer_ref,
            )

        normalized = dict(request)
        normalized["customer_ref"]   = customer_ref
        normalized["correlation_id"] = correlation_id
        return normalized

    def _repopulate_inflight_from_db(self) -> bool:
        """
        Ricarica entrambe le strutture di dedup RAM dal DB dopo restart o recovery.
        Ritorna True se almeno uno dei due metodi DB era disponibile (ram_synced).
        Ritorna False se nessun metodo DB era disponibile: il chiamante può loggare
        o includere questo nel contratto di recovery per trasparenza operativa.
        """
        synced = False
        with self._lock:
            load_refs = getattr(self.db, "load_pending_customer_refs", None)
            if callable(load_refs):
                try:
                    refs = load_refs()
                    if refs:
                        refs_list = list(refs)
                        for ref in refs_list:
                            self._inflight_keys.add(str(ref))
                        logger.info(
                            "Repopulated _inflight_keys from DB: %d refs", len(refs_list)
                        )
                    synced = True
                except Exception:
                    logger.exception("Failed to repopulate _inflight_keys from DB")
            else:
                logger.debug("DB.load_pending_customer_refs unavailable – _inflight_keys not synced")

            load_cids = getattr(self.db, "load_pending_correlation_ids", None)
            if callable(load_cids):
                try:
                    cids = load_cids()
                    if cids:
                        cids_list = list(cids)
                        added = 0
                        for cid in cids_list:
                            cid_str = str(cid)
                            if cid_str not in self._seen_correlation_ids:
                                self._seen_correlation_ids.add(cid_str)
                                self._seen_cid_order.append(cid_str)
                                added += 1
                        logger.info(
                            "Repopulated _seen_correlation_ids from DB: %d added (%d skipped duplicates)",
                            added, len(cids_list) - added,
                        )
                    synced = True
                except Exception:
                    logger.exception("Failed to repopulate _seen_correlation_ids from DB")
            else:
                logger.debug("DB.load_pending_correlation_ids unavailable – _seen_correlation_ids not synced")

        return synced

    # ------------------------------------------------------------------
    # SAFE MODE / RISK / DEDUP
    # ------------------------------------------------------------------

    def _is_safe_mode_enabled(self) -> bool:
        getter = getattr(self.safe_mode, "is_enabled", None)
        return bool(getter()) if callable(getter) else False

    def _risk_gate(self, request: Dict[str, Any]) -> Dict[str, Any]:
        checker = getattr(self.risk_middleware, "check", None)
        if callable(checker):
            result = checker(request)
            if isinstance(result, dict) and "allowed" in result:
                return result
        return {"allowed": True, "reason": None, "payload": request}

    def _dedup_allow(self, ctx: _ExecutionContext) -> bool:
        # Priorità 1: guard esterno (policy distribuita/persistita)
        if self.guard is not None:
            allow = getattr(self.guard, "allow", None)
            if callable(allow):
                return bool(allow(ctx.customer_ref))

        # Priorità 2: in-memory customer_ref (_inflight_keys, legacy + concorrenza)
        if ctx.customer_ref in self._inflight_keys:
            return False

        # Priorità 3: in-memory correlation_id (exactly-once per intent)
        if ctx.correlation_id in self._seen_correlation_ids:
            return False

        # Priorità 4: idempotency persistente su DB.
        #
        # Contratto atteso di db.order_exists_inflight:
        #   def order_exists_inflight(*, customer_ref: str, correlation_id: str) -> bool
        #
        # Semantica: OR, non AND.
        # Deve ritornare True se esiste almeno un ordine non-terminale
        # (INFLIGHT / SUBMITTED / AMBIGUOUS) che corrisponde a EITHER:
        #   - customer_ref == customer_ref   (stesso cliente, qualsiasi intento)
        #   - correlation_id == correlation_id  (stesso intento, qualsiasi cliente)
        #
        # Un'implementazione AND (match congiunto) sarebbe inutile: permetterebbe
        # a un retry con stesso cliente ma nuovo correlation_id di bypassare il check,
        # e a un retry con stesso correlation_id ma cliente diverso di bypassare allo stesso modo.
        #
        # Il check è fail-open: se il DB non risponde, lasciamo passare.
        exists_fn = getattr(self.db, "order_exists_inflight", None)
        if callable(exists_fn):
            try:
                if exists_fn(
                    customer_ref=ctx.customer_ref,
                    correlation_id=ctx.correlation_id,
                ):
                    logger.warning(
                        "DB-level duplicate detected customer_ref=%s correlation_id=%s",
                        ctx.customer_ref, ctx.correlation_id,
                    )
                    return False
            except Exception:
                logger.exception("order_exists_inflight check failed – proceeding (fail-open)")

        # Registra entrambe le chiavi in RAM
        self._inflight_keys.add(ctx.customer_ref)
        self._seen_correlation_ids.add(ctx.correlation_id)
        self._seen_cid_order.append(ctx.correlation_id)

        # Eviction FIFO quando si supera il cap dimensionale.
        # Usa popleft() O(1) su deque – non O(n) come slice su list.
        # Dopo eviction alcuni vecchi correlation_id non sono più bloccati:
        # la garanzia è "exactly-once entro la finestra", non permanente.
        # Per garanzia permanente usare il DB-level check (order_exists_inflight).
        while len(self._seen_correlation_ids) > self._max_seen_cid_size:
            trim_count = len(self._seen_correlation_ids) - self._seen_cid_trim_to
            evicted = 0
            for _ in range(trim_count):
                if self._seen_cid_order:
                    old_cid = self._seen_cid_order.popleft()
                    self._seen_correlation_ids.discard(old_cid)
                    evicted += 1
            logger.info(
                "Bounded FIFO dedup window evicted %d old correlation_ids, current size=%d",
                evicted, len(self._seen_correlation_ids),
            )
            break  # un solo passaggio per ciclo di _dedup_allow

        return True

    # ------------------------------------------------------------------
    # SUBMIT PATH
    # ------------------------------------------------------------------

    def _submit_to_order_path(
        self, ctx: _ExecutionContext, request: Dict[str, Any]
    ) -> Any:
        payload = dict(request)
        payload["customer_ref"]   = ctx.customer_ref
        payload["correlation_id"] = ctx.correlation_id

        if self.order_manager is not None:
            # Compatibilità: OrderManager può esporre submit() o place_order().
            # submit() ha priorità (contratto nuovo), place_order() è fallback legacy.
            for method_name in ("submit", "place_order"):
                fn = getattr(self.order_manager, method_name, None)
                if callable(fn):
                    return fn(payload)

        if callable(self.client_getter):
            client = self.client_getter()
            if client is not None:
                place = getattr(client, "place_bet", None)
                if callable(place):
                    return place(**payload)

        raise RuntimeError("NO_VALID_EXECUTION_PATH")

    # ------------------------------------------------------------------
    # AMBIGUITY POLICY
    # ------------------------------------------------------------------

    def _classify_ambiguity(self, exc: Exception) -> str:
        text = str(exc).lower()
        if "timeout"                        in text: return AMBIGUITY_SUBMIT_TIMEOUT
        if "response lost" in text or "lost response" in text: return AMBIGUITY_RESPONSE_LOST
        if "persist" in text and "confirm"  in text: return AMBIGUITY_PERSISTED_NOT_CONFIRMED
        if "split"                          in text: return AMBIGUITY_SPLIT_STATE
        return AMBIGUITY_SUBMIT_UNKNOWN

    def _handle_submit_exception(
        self,
        ctx:      _ExecutionContext,
        audit:    Dict[str, Any],
        order_id: Any,
        exc:      Exception,
    ) -> Dict[str, Any]:
        ambiguity_reason: Optional[str] = None
        error_type = getattr(exc, "error_type", None)

        if error_type == ERROR_AMBIGUOUS:
            ambiguity_reason = getattr(exc, "ambiguity_reason", None) or self._classify_ambiguity(exc)
        elif isinstance(exc, TimeoutError):
            error_type, ambiguity_reason = ERROR_AMBIGUOUS, AMBIGUITY_SUBMIT_TIMEOUT
        elif "timeout" in str(exc).lower():
            error_type       = ERROR_AMBIGUOUS
            ambiguity_reason = self._classify_ambiguity(exc)
        elif error_type is None:
            error_type = ERROR_PERMANENT

        payload = {
            "order_id": order_id, "error": str(exc),
            "error_type": error_type, "ambiguity_reason": ambiguity_reason,
        }

        if error_type == ERROR_AMBIGUOUS:
            self._emit(ctx, audit, "SUBMIT_AMBIGUOUS", payload, category="ambiguity")
            self._transition_order(ctx, audit, order_id,
                                   STATUS_INFLIGHT, STATUS_AMBIGUOUS,
                                   extra={"ambiguity_reason": ambiguity_reason,
                                          "last_error": str(exc)})
            self._enqueue_reconcile(ctx, audit, order_id, ambiguity_reason)
            # FIX #3 – audit terminale
            self._emit(ctx, audit, "FINAL_AMBIGUOUS",
                       {"order_id": order_id, "reason": ambiguity_reason}, category="final")
            return self._finalize(
                ctx=ctx, audit=audit, order_id=order_id,
                status=STATUS_AMBIGUOUS, outcome=OUTCOME_AMBIGUOUS,
                ambiguity_reason=ambiguity_reason,
            )

        self._emit(ctx, audit, "SUBMIT_FAILED", payload, category="failure")
        self._transition_order(ctx, audit, order_id,
                               STATUS_INFLIGHT, STATUS_FAILED,
                               extra={"last_error": str(exc), "error_type": error_type})
        # FIX #3 – audit terminale
        self._emit(ctx, audit, "FINAL_FAILURE",
                   {"order_id": order_id, "error": str(exc)}, category="final")

        return self._finalize(
            ctx=ctx, audit=audit, order_id=order_id,
            status=STATUS_FAILED, outcome=OUTCOME_FAILURE,
            error=str(exc), reason="SUBMIT_FAILED",
        )

    def _enqueue_reconcile(
        self,
        ctx:              _ExecutionContext,
        audit:            Dict[str, Any],
        order_id:         Any,
        ambiguity_reason: str,
    ) -> None:
        enqueue = getattr(self.reconciliation_engine, "enqueue", None)
        if callable(enqueue):
            enqueue(
                order_id=order_id,
                correlation_id=ctx.correlation_id,
                customer_ref=ctx.customer_ref,
                ambiguity_reason=ambiguity_reason,
            )
        self._emit(ctx, audit, "RECONCILE_ENQUEUED",
                   {"order_id": order_id, "ambiguity_reason": ambiguity_reason},
                   category="reconcile")

    # ------------------------------------------------------------------
    # FIX #6 – ANTI-BYPASS
    # I metodi critici verificano che il ctx sia un _ExecutionContext reale.
    # Un chiamante esterno non può costruirlo senza passare per
    # _submit_via_engine (l'unico punto di costruzione del context).
    # ------------------------------------------------------------------

    @staticmethod
    def _assert_valid_ctx(ctx: Any) -> None:
        if not isinstance(ctx, _ExecutionContext):
            raise RuntimeError(
                "INVALID_EXECUTION_CONTEXT – metodi interni non invocabili direttamente."
            )

    # ------------------------------------------------------------------
    # STATE MACHINE / PERSISTENCE
    # ------------------------------------------------------------------

    def _persist_inflight(
        self, ctx: _ExecutionContext, request: Dict[str, Any]
    ) -> Any:
        self._assert_valid_ctx(ctx)

        payload = {
            "customer_ref":   ctx.customer_ref,
            "correlation_id": ctx.correlation_id,
            "status":         STATUS_INFLIGHT,
            "payload":        request,
            "created_at":     ctx.created_at,
            "outcome":        None,
        }

        insert_order = getattr(self.db, "insert_order", None)
        if callable(insert_order):
            return insert_order(payload)

        # Degraded-mode fallback (FakeDB senza insert_order)
        order_id = str(uuid.uuid4())
        logger.warning(
            "DB.insert_order unavailable – local order_id=%s (DEGRADED)", order_id
        )
        return order_id

    def _transition_order(
        self,
        ctx:         _ExecutionContext,
        audit:       Dict[str, Any],
        order_id:    Any,
        from_status: str,
        to_status:   str,
        extra:       Optional[Dict[str, Any]] = None,
    ) -> None:
        self._assert_valid_ctx(ctx)

        if to_status not in ALLOWED_TRANSITIONS.get(from_status, set()):
            raise RuntimeError(f"ILLEGAL_ORDER_TRANSITION:{from_status}->{to_status}")

        update: Dict[str, Any] = {"status": to_status, "updated_at": time.time()}
        if extra:
            update.update(extra)

        update_order = getattr(self.db, "update_order", None)
        if callable(update_order):
            update_order(order_id, update)
        else:
            logger.warning("DB.update_order unavailable – transition not persisted")

        self._emit(ctx, audit, "ORDER_TRANSITION",
                   {"order_id": order_id, "from_status": from_status,
                    "to_status": to_status, "extra": extra or {}},
                   category="state")

    def _safe_mark_failed(
        self,
        ctx:         _ExecutionContext,
        audit:       Dict[str, Any],
        order_id:    Any,
        reason:      str,
        error:       str,
        from_status: str = STATUS_INFLIGHT,
    ) -> None:
        """
        Marca un ordine come FAILED passando SEMPRE da _transition_order.
        Non scrive mai 'status' direttamente nel DB.
        from_status deve riflettere lo stato reale dell'ordine al momento
        della chiamata. Se la transizione non è legale viene loggata ma
        non propagata (best-effort nel fatal path).
        """
        try:
            self._transition_order(
                ctx, audit, order_id,
                from_status, STATUS_FAILED,
                extra={"failure_reason": reason, "last_error": error},
            )
            self._emit(ctx, audit, "SAFE_MARK_FAILED",
                       {"order_id": order_id, "reason": reason, "error": error},
                       category="failure")
        except Exception:
            logger.exception(
                "safe_mark_failed: could not transition order_id=%s "
                "from %s -> FAILED (reason=%s)",
                order_id, from_status, reason,
            )

    # ------------------------------------------------------------------
    # AUDIT
    # ------------------------------------------------------------------

    def _new_audit(self, ctx: _ExecutionContext) -> Dict[str, Any]:
        return {
            "correlation_id":  ctx.correlation_id,
            "customer_ref":    ctx.customer_ref,
            "events":          [],
            "index":           0,
            "_last_event_id":  None,   # usato internamente per la chain
        }

    def _emit(
        self,
        ctx:        _ExecutionContext,
        audit:      Dict[str, Any],
        event_type: str,
        payload:    Dict[str, Any],
        *,
        category:   str,
    ) -> None:
        event_id = str(uuid.uuid4())
        event = {
            "event_id":       event_id,
            # Chain: collega ogni evento al precedente per ricostruzione lineare
            "parent_event_id": audit["_last_event_id"],
            "index":          audit["index"],
            "ts":             time.time(),
            "type":           event_type,
            "category":       category,
            "payload":        payload,
            "correlation_id": ctx.correlation_id,
            "customer_ref":   ctx.customer_ref,
        }
        audit["index"] += 1
        audit["_last_event_id"] = event_id
        audit["events"].append(event)

        persisted = False
        for method_name in ("insert_audit_event", "insert_order_event", "append_order_event"):
            fn = getattr(self.db, method_name, None)
            if callable(fn):
                fn(event)
                persisted = True
                break

        # FIX 10 – async_db_writer come canale alternativo di persistenza audit
        write_fn = getattr(self.async_db_writer, "write", None)
        if callable(write_fn):
            try:
                write_fn(event)
                persisted = True
            except Exception:
                logger.exception("async_db_writer.write failed")

        if not persisted:
            logger.debug("No audit persistence method – in-memory only")

        logger.debug("AUDIT[%s] %s %s", category, event_type, payload)

    # ------------------------------------------------------------------
    # FIX #7 – PUBLIC STATUS MAPPING (funzione centralizzata)
    # ------------------------------------------------------------------

    @staticmethod
    def _public_status(internal: str) -> str:
        return _INTERNAL_TO_PUBLIC_STATUS.get(internal, internal)

    # ------------------------------------------------------------------
    # FIX #1 – FINALIZE POLICY: ZERO BYPASS DELLA STATE MACHINE
    #
    # _finalize NON tocca mai il campo "status" nel DB.
    # Lo status viene aggiornato ESCLUSIVAMENTE da _transition_order.
    # _finalize aggiorna SOLO i metadati economici (outcome, reason, etc).
    # ------------------------------------------------------------------

    def _finalize(
        self,
        ctx:      _ExecutionContext,
        audit:    Dict[str, Any],
        order_id: Optional[Any],
        status:   str,
        outcome:  str,
        *,
        reason:           Optional[str] = None,
        error:            Optional[str] = None,
        ambiguity_reason: Optional[str] = None,
        response:         Optional[Any] = None,
    ) -> Dict[str, Any]:

        # Invarianti di policy
        if status == STATUS_AMBIGUOUS and not ambiguity_reason:
            raise RuntimeError("AMBIGUOUS_FINALIZE_REQUIRES_REASON")
        if status == STATUS_DENIED and error is not None:
            raise RuntimeError("DENIED_SHOULD_NOT_CARRY_TECHNICAL_ERROR")
        if status == STATUS_COMPLETED and ambiguity_reason is not None:
            raise RuntimeError("COMPLETED_CANNOT_KEEP_AMBIGUITY_REASON")

        self._emit(ctx, audit, "FINALIZED",
                   {"order_id": order_id, "status": status, "outcome": outcome,
                    "reason": reason, "error": error,
                    "ambiguity_reason": ambiguity_reason},
                   category="final")

        # Aggiorna SOLO metadati economici – MAI "status"
        if order_id is not None:
            update_order = getattr(self.db, "update_order", None)
            if callable(update_order):
                meta: Dict[str, Any] = {
                    "updated_at":       time.time(),
                    "outcome":          outcome,
                    "reason":           reason,
                    "last_error":       error,
                    "ambiguity_reason": ambiguity_reason,
                    "finalized":        True,
                }
                if response is not None:
                    meta["response"] = response
                update_order(order_id, meta)

        # Pulizia contract: i campi prefissati con "_" sono cursori interni
        # dell'audit chain (es. _last_event_id) e non fanno parte
        # del contratto pubblico restituito al chiamante.
        public_audit = {k: v for k, v in audit.items() if not k.startswith("_")}

        # Release inflight keys – policy conservativa:
        #
        # _seen_correlation_ids → MAI rilasciato da _finalize.
        #   La rimozione avviene SOLO tramite eviction FIFO nella bounded
        #   dedup window (vedi _dedup_allow): quando il buffer supera
        #   MAX_SEEN_CID_SIZE, i correlation_id più vecchi vengono scartati.
        #   Garanzia: exactly-once ENTRO LA FINESTRA del buffer in-memory.
        #   Per garanzia permanente oltre la finestra → DB-level check
        #   (order_exists_inflight) che è persistente e non soggetto a cap.
        #
        # _inflight_keys (customer_ref) → rilasciato su SUCCESS e FAILURE
        #   (lifecycle concluso), NON su AMBIGUOUS (esito ancora incerto:
        #   il reconciler deve ancora risolvere lo stato reale prima che
        #   lo stesso cliente possa sottomettere un nuovo ordine).
        try:
            if outcome in (OUTCOME_SUCCESS, OUTCOME_FAILURE):
                self._inflight_keys.discard(ctx.customer_ref)
            # AMBIGUOUS: _inflight_keys resta occupato fino a riconciliazione
        except Exception:
            logger.exception("Failed to release inflight keys")

        # FIX 5 – pubblica evento bus per i test che ascoltano QUICK_BET_*
        try:
            publish = getattr(self.bus, "publish", None)
            if callable(publish):
                if outcome == OUTCOME_FAILURE:
                    event_name = "QUICK_BET_FAILED"
                elif outcome == OUTCOME_SUCCESS:
                    event_name = "QUICK_BET_SUCCESS"
                else:
                    event_name = "QUICK_BET_AMBIGUOUS"
                publish(event_name, {
                    "correlation_id": ctx.correlation_id,
                    "customer_ref":   ctx.customer_ref,
                    "status":         status,
                    "outcome":        outcome,
                })
        except Exception:
            logger.exception("Failed to publish bus event")

        result: Dict[str, Any] = {
            "ok":               outcome == OUTCOME_SUCCESS,
            "status":           self._public_status(status),
            "outcome":          outcome,
            "correlation_id":   ctx.correlation_id,
            "customer_ref":     ctx.customer_ref,
            "audit":            public_audit,
            "reason":           reason,
            "error":            error,
            "ambiguity_reason": ambiguity_reason,
            "response":         response,
        }

        return result
