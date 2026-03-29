from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from typing import Any, Dict, Optional, Set

from order_manager import OrderManager

logger = logging.getLogger(__name__)


class TradingEngine:
    """
    Trading engine headless (ASYNC ACCEPTANCE + SAGA/OUTBOX READY).

    Responsabilità:
    - entry point orchestration
    - dedup avanzato / idempotenza
    - integrazione saga/outbox
    - gestione inflight + recovery hook
    - hook opzionali: safe mode / risk middleware / reconciliation / recovery
    - fail-safe totale
    - non blocca il caller/EventBus sul placement reale

    Semantica:
    - _handle_quick_bet() conferma la presa in carico della richiesta
    - l'esecuzione reale avviene async
    - gli errori operativi diventano eventi async + stato persistente
    """

    MIN_EXCHANGE_STAKE = 2.0
    MICRO_MIN_STAKE = 0.10

    SAGA_PENDING = "PENDING"
    SAGA_ACCEPTED = "ACCEPTED"
    SAGA_EXECUTING = "EXECUTING"
    SAGA_PLACED = "PLACED"
    SAGA_FAILED = "FAILED"
    SAGA_ROLLBACK_REQUIRED = "ROLLBACK_REQUIRED"

    OUTBOX_PENDING = "PENDING"
    OUTBOX_DONE = "DONE"
    OUTBOX_FAILED = "FAILED"

    def __init__(
        self,
        bus,
        db,
        client_getter,
        executor=None,
        safe_mode=None,
        risk_middleware=None,
        reconciliation_engine=None,
        state_recovery=None,
        async_db_writer=None,
    ):
        self.bus = bus
        self.db = db
        self.client_getter = client_getter
        self.executor = executor

        self.safe_mode = safe_mode
        self.risk_middleware = risk_middleware
        self.reconciliation_engine = reconciliation_engine
        self.state_recovery = state_recovery
        self.async_db_writer = async_db_writer

        self.order_manager = OrderManager(
            bus=bus,
            db=db,
            client_getter=client_getter,
        )

        self._lock = threading.RLock()
        self._inflight_keys: Set[str] = set()
        self._started = False

        self._subscribe_bus()

    # =========================================================
    # BUS SUBSCRIPTION
    # =========================================================
    def _subscribe_bus(self) -> None:
        if self._started:
            return

        self.bus.subscribe("CMD_QUICK_BET", self._handle_quick_bet)
        self.bus.subscribe("REQ_QUICK_BET", self._handle_quick_bet)

        try:
            self.bus.subscribe("RECONCILE_NOW", self._handle_reconcile_now)
        except Exception:
            logger.exception("Errore subscribe RECONCILE_NOW")

        try:
            self.bus.subscribe("RECOVER_PENDING", self._handle_recover_pending)
        except Exception:
            logger.exception("Errore subscribe RECOVER_PENDING")

        self._started = True

    # =========================================================
    # INTERNAL HELPERS
    # =========================================================
    def _now(self) -> str:
        return datetime.utcnow().isoformat()

    def _publish(self, event_name: str, payload: Dict[str, Any]) -> None:
        try:
            self.bus.publish(event_name, payload)
        except Exception:
            logger.exception("Errore publish %s", event_name)

    def _run_in_background(self, fn, *args, **kwargs):
        """
        Esegue in background in modo fail-safe.
        Non deve mai propagare eccezioni al caller di _handle_quick_bet().
        """
        def _wrapped():
            try:
                return fn(*args, **kwargs)
            except Exception:
                logger.exception("Errore worker TradingEngine")
                return None

        if self.executor and hasattr(self.executor, "submit"):
            try:
                return self.executor.submit("trading_engine", _wrapped)
            except TypeError:
                try:
                    return self.executor.submit(_wrapped)
                except Exception:
                    logger.exception("Executor submit fallita, fallback thread")
            except Exception:
                logger.exception("Executor submit fallita, fallback thread")

        t = threading.Thread(
            target=_wrapped,
            name="TradingEngineAsync",
            daemon=True,
        )
        t.start()
        return t

    def _safe_side(self, value: Any) -> str:
        side = str(value or "BACK").upper().strip()
        return side if side in {"BACK", "LAY"} else "BACK"

    def _safe_bool(self, value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value in (None, ""):
                return float(default)
            return float(value)
        except Exception:
            return float(default)

    def _safe_int(self, value: Any, default: int = 0) -> int:
        try:
            if value in (None, ""):
                return int(default)
            return int(value)
        except Exception:
            return int(default)

    def _safe_json_dumps(self, value: Any) -> str:
        try:
            return json.dumps(value if value is not None else {}, ensure_ascii=False)
        except Exception:
            return "{}"

    def _build_dedup_key(self, payload: Dict[str, Any]) -> str:
        customer_ref = str(payload.get("customer_ref") or "").strip()
        if customer_ref:
            return customer_ref

        event_key = str(payload.get("event_key") or "").strip()
        if event_key:
            return event_key

        return (
            f"{payload.get('market_id', '')}:"
            f"{payload.get('selection_id', '')}:"
            f"{payload.get('bet_type', 'BACK')}:"
            f"{payload.get('price', 0)}:"
            f"{payload.get('stake', 0)}"
        )

    def _is_microstake(self, stake: float) -> bool:
        stake = float(stake or 0.0)
        return self.MICRO_MIN_STAKE <= stake < self.MIN_EXCHANGE_STAKE

    def _release_inflight(self, dedup_key: str) -> None:
        with self._lock:
            self._inflight_keys.discard(dedup_key)

    def _mark_inflight(self, dedup_key: str) -> bool:
        with self._lock:
            if dedup_key in self._inflight_keys:
                return False
            self._inflight_keys.add(dedup_key)
            return True

    # =========================================================
    # NORMALIZATION
    # =========================================================
    def _normalize_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(payload or {})

        market_id = normalized.get("market_id", normalized.get("marketId"))
        selection_id = normalized.get("selection_id", normalized.get("selectionId"))
        price_raw = normalized.get("price", normalized.get("odds"))
        stake_raw = normalized.get("stake", normalized.get("size"))

        if market_id in (None, ""):
            raise ValueError("Payload mancante di market_id")
        if selection_id in (None, ""):
            raise ValueError("Payload mancante di selection_id")
        if price_raw in (None, ""):
            raise ValueError("Payload mancante di price")
        if stake_raw in (None, ""):
            raise ValueError("Payload mancante di stake")

        normalized["market_id"] = str(market_id).strip()
        normalized["selection_id"] = self._safe_int(selection_id)
        normalized["bet_type"] = self._safe_side(
            normalized.get("bet_type")
            or normalized.get("side")
            or normalized.get("action")
            or "BACK"
        )
        normalized["price"] = self._safe_float(price_raw)
        normalized["stake"] = self._safe_float(stake_raw)

        normalized["event_name"] = str(
            normalized.get("event_name")
            or normalized.get("event")
            or normalized.get("match")
            or ""
        )
        normalized["market_name"] = str(
            normalized.get("market_name")
            or normalized.get("market")
            or normalized.get("market_type")
            or ""
        )
        normalized["runner_name"] = str(
            normalized.get("runner_name")
            or normalized.get("runnerName")
            or normalized.get("selection")
            or ""
        )

        normalized["simulation_mode"] = self._safe_bool(
            normalized.get("simulation_mode", False),
            default=False,
        )
        normalized["event_key"] = str(normalized.get("event_key") or "")
        normalized["batch_id"] = str(normalized.get("batch_id") or "")
        normalized["customer_ref"] = str(normalized.get("customer_ref") or "")
        normalized["roserpina_reason"] = str(normalized.get("roserpina_reason") or "")
        normalized["roserpina_mode"] = str(normalized.get("roserpina_mode") or "")
        normalized["source"] = str(normalized.get("source") or "")
        normalized["table_id"] = (
            None
            if normalized.get("table_id") in (None, "")
            else self._safe_int(normalized.get("table_id"))
        )

        return normalized

    def _validate_payload(self, payload: Dict[str, Any]) -> None:
        if not payload["market_id"]:
            raise ValueError("market_id non valido")

        if int(payload["selection_id"]) <= 0:
            raise ValueError("selection_id non valido")

        if float(payload["price"]) <= 1.0:
            raise ValueError("Quota non valida")

        if float(payload["stake"]) < self.MICRO_MIN_STAKE:
            raise ValueError("Stake sotto MICRO_MIN_STAKE")

        if payload["bet_type"] not in {"BACK", "LAY"}:
            raise ValueError("bet_type non valido")

    # =========================================================
    # OPTIONAL HOOKS
    # =========================================================
    def _safe_mode_allows(self, payload: Dict[str, Any]) -> None:
        if self.safe_mode is None:
            return

        try:
            if hasattr(self.safe_mode, "is_enabled") and self.safe_mode.is_enabled():
                raise RuntimeError("Safe mode attivo")

            if hasattr(self.safe_mode, "enabled") and bool(getattr(self.safe_mode, "enabled")):
                raise RuntimeError("Safe mode attivo")

            if hasattr(self.safe_mode, "assert_can_trade"):
                self.safe_mode.assert_can_trade(payload)
        except Exception as exc:
            raise RuntimeError(str(exc)) from exc

    def _risk_allows(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if self.risk_middleware is None:
            return payload

        try:
            if hasattr(self.risk_middleware, "process_request"):
                result = self.risk_middleware.process_request(payload)
                if result is None:
                    raise RuntimeError("Risk middleware ha rifiutato la richiesta")
                return result

            if hasattr(self.risk_middleware, "allow"):
                allowed = self.risk_middleware.allow(payload)
                if not allowed:
                    raise RuntimeError("Risk middleware ha bloccato la richiesta")
                return payload

            return payload
        except Exception as exc:
            raise RuntimeError(str(exc)) from exc

    def _call_reconciliation_hook(self, payload: Dict[str, Any]) -> None:
        if self.reconciliation_engine is None:
            return

        try:
            if hasattr(self.reconciliation_engine, "on_order_submitted"):
                self.reconciliation_engine.on_order_submitted(payload)
            elif hasattr(self.reconciliation_engine, "enqueue"):
                self.reconciliation_engine.enqueue(payload)
        except Exception:
            logger.exception("Errore hook reconciliation")

    # =========================================================
    # SAGA / OUTBOX
    # =========================================================
    def _create_saga_record(self, payload: Dict[str, Any], dedup_key: str) -> Dict[str, Any]:
        return {
            "customer_ref": payload.get("customer_ref") or dedup_key,
            "batch_id": payload.get("batch_id", ""),
            "event_key": payload.get("event_key", ""),
            "table_id": payload.get("table_id"),
            "market_id": payload.get("market_id", ""),
            "selection_id": payload.get("selection_id", 0),
            "bet_type": payload.get("bet_type", "BACK"),
            "price": payload.get("price", 0.0),
            "stake": payload.get("stake", 0.0),
            "status": self.SAGA_ACCEPTED,
            "payload": dict(payload),
        }

    def _persist_saga_create(self, payload: Dict[str, Any], dedup_key: str) -> None:
        saga = self._create_saga_record(payload, dedup_key)

        try:
            if hasattr(self.db, "create_order_saga"):
                self.db.create_order_saga(
                    customer_ref=saga["customer_ref"],
                    batch_id=saga["batch_id"],
                    event_key=saga["event_key"],
                    table_id=saga["table_id"],
                    market_id=saga["market_id"],
                    selection_id=saga["selection_id"],
                    bet_type=saga["bet_type"],
                    price=saga["price"],
                    stake=saga["stake"],
                    payload=saga["payload"],
                    status=saga["status"],
                )
        except Exception:
            logger.exception("Errore create_order_saga")

    def _persist_saga_update(
        self,
        *,
        dedup_key: str,
        status: str,
        bet_id: str = "",
        error_text: str = "",
    ) -> None:
        customer_ref = str(dedup_key or "")
        if not customer_ref:
            return

        try:
            if hasattr(self.db, "update_order_saga"):
                self.db.update_order_saga(
                    customer_ref=customer_ref,
                    status=status,
                    bet_id=bet_id,
                    error_text=error_text,
                )
        except Exception:
            logger.exception("Errore update_order_saga")

    def _persist_outbox_event(
        self,
        *,
        event_type: str,
        payload: Dict[str, Any],
        dedup_key: str,
        status: str = OUTBOX_PENDING,
    ) -> None:
        record = {
            "event_type": str(event_type),
            "dedup_key": str(dedup_key or ""),
            "payload_json": self._safe_json_dumps(payload),
            "status": str(status),
            "created_at": self._now(),
        }

        try:
            if self.async_db_writer and hasattr(self.async_db_writer, "submit"):
                self.async_db_writer.submit("outbox", record)
                return

            if hasattr(self.db, "save_outbox_event"):
                self.db.save_outbox_event(**record)
        except Exception:
            logger.exception("Errore persist outbox")

    def _db_log_submission(self, payload: Dict[str, Any], dedup_key: str) -> None:
        record = {
            "customer_ref": payload.get("customer_ref") or dedup_key,
            "market_id": payload.get("market_id", ""),
            "selection_id": payload.get("selection_id", 0),
            "bet_type": payload.get("bet_type", "BACK"),
            "price": payload.get("price", 0.0),
            "stake": payload.get("stake", 0.0),
            "event_key": payload.get("event_key", ""),
            "batch_id": payload.get("batch_id", ""),
            "event_name": payload.get("event_name", ""),
            "market_name": payload.get("market_name", ""),
            "runner_name": payload.get("runner_name", ""),
            "simulation_mode": payload.get("simulation_mode", False),
            "status": "SUBMITTED",
        }

        try:
            if self.async_db_writer and hasattr(self.async_db_writer, "submit"):
                self.async_db_writer.submit("bet", record)
                return

            if hasattr(self.db, "save_bet"):
                self.db.save_bet(**record)
        except Exception:
            logger.exception("Errore DB writer integration")

    # =========================================================
    # EXECUTION PATH
    # =========================================================
    def _extract_bet_id(self, result: Any) -> str:
        try:
            if isinstance(result, dict):
                reports = result.get("instructionReports") or []
                if reports and isinstance(reports[0], dict):
                    return str(reports[0].get("betId") or "")
                return str(result.get("bet_id") or result.get("betId") or "")
        except Exception:
            return ""
        return ""

    def _execute_quick_bet(self, normalized: Dict[str, Any], dedup_key: str) -> None:
        try:
            self._persist_saga_update(dedup_key=dedup_key, status=self.SAGA_EXECUTING)

            self._publish(
                "QUICK_BET_EXECUTION_STARTED",
                {
                    **normalized,
                    "status": "EXECUTION_STARTED",
                    "dedup_key": dedup_key,
                },
            )

            result = self.order_manager.place_order(normalized)
            bet_id = self._extract_bet_id(result)

            self._persist_saga_update(
                dedup_key=dedup_key,
                status=self.SAGA_PLACED,
                bet_id=bet_id,
            )

            self._persist_outbox_event(
                event_type="QUICK_BET_EXECUTION_FINISHED",
                payload={
                    **normalized,
                    "status": "EXECUTION_FINISHED",
                    "dedup_key": dedup_key,
                    "result": result,
                },
                dedup_key=dedup_key,
                status=self.OUTBOX_DONE,
            )

            self._publish(
                "QUICK_BET_EXECUTION_FINISHED",
                {
                    **normalized,
                    "status": "EXECUTION_FINISHED",
                    "dedup_key": dedup_key,
                    "result": result,
                },
            )

        except Exception as exc:
            error_text = str(exc)

            self._persist_saga_update(
                dedup_key=dedup_key,
                status=self.SAGA_ROLLBACK_REQUIRED,
                error_text=error_text,
            )

            rollback_payload = {
                **normalized,
                "status": "ROLLBACK_REQUIRED",
                "dedup_key": dedup_key,
                "error": error_text,
            }
            failed_payload = {
                **normalized,
                "status": "FAILED",
                "dedup_key": dedup_key,
                "error": error_text,
            }

            self._persist_outbox_event(
                event_type="QUICK_BET_ROLLBACK_REQUIRED",
                payload=rollback_payload,
                dedup_key=dedup_key,
                status=self.OUTBOX_DONE,
            )
            self._persist_outbox_event(
                event_type="QUICK_BET_FAILED",
                payload=failed_payload,
                dedup_key=dedup_key,
                status=self.OUTBOX_DONE,
            )

            self._publish("QUICK_BET_ROLLBACK_REQUIRED", rollback_payload)
            self._publish("QUICK_BET_FAILED", failed_payload)

            self._persist_saga_update(
                dedup_key=dedup_key,
                status=self.SAGA_FAILED,
                error_text=error_text,
            )

            logger.exception("Errore async quick bet execution: %s", exc)

        finally:
            self._release_inflight(dedup_key)

    # =========================================================
    # MAIN HANDLER
    # =========================================================
    def _handle_quick_bet(self, payload):
        fail_payload: Dict[str, Any] = dict(payload or {})
        dedup_key = ""

        try:
            normalized = self._normalize_payload(payload)
            self._validate_payload(normalized)

            self._safe_mode_allows(normalized)
            normalized = self._risk_allows(normalized)

            dedup_key = self._build_dedup_key(normalized)

            if not self._mark_inflight(dedup_key):
                duplicate_payload = {
                    **normalized,
                    "status": "DUPLICATE_BLOCKED",
                    "dedup_key": dedup_key,
                }
                self._persist_outbox_event(
                    event_type="QUICK_BET_DUPLICATE_BLOCKED",
                    payload=duplicate_payload,
                    dedup_key=dedup_key,
                    status=self.OUTBOX_DONE,
                )
                self._publish("QUICK_BET_DUPLICATE_BLOCKED", duplicate_payload)
                return {
                    "ok": True,
                    "status": "DUPLICATE_BLOCKED",
                    "dedup_key": dedup_key,
                }

            normalized["microstake_mode"] = self._is_microstake(normalized["stake"])
            normalized["dedup_key"] = dedup_key

            self._persist_saga_create(normalized, dedup_key)
            self._db_log_submission(normalized, dedup_key)
            self._call_reconciliation_hook(normalized)

            routed_payload = {
                **normalized,
                "status": "ACCEPTED_FOR_PROCESSING",
            }

            self._persist_outbox_event(
                event_type="QUICK_BET_ROUTED",
                payload=routed_payload,
                dedup_key=dedup_key,
                status=self.OUTBOX_DONE,
            )
            self._publish("QUICK_BET_ROUTED", routed_payload)

            self._run_in_background(
                self._execute_quick_bet,
                normalized,
                dedup_key,
            )

            return {
                "ok": True,
                "status": "ACCEPTED_FOR_PROCESSING",
                "simulation_mode": bool(normalized.get("simulation_mode", False)),
                "dedup_key": dedup_key,
            }

        except Exception as exc:
            if dedup_key:
                self._release_inflight(dedup_key)

            fail_payload["error"] = str(exc)
            if dedup_key:
                fail_payload["dedup_key"] = dedup_key

            self._persist_outbox_event(
                event_type="QUICK_BET_FAILED",
                payload=fail_payload,
                dedup_key=dedup_key,
                status=self.OUTBOX_FAILED,
            )
            self._publish("QUICK_BET_FAILED", fail_payload)
            logger.exception("Errore _handle_quick_bet: %s", exc)

            return {
                "ok": False,
                "status": "FAILED",
                "error": str(exc),
            }

    # =========================================================
    # RECOVERY / RECONCILIATION
    # =========================================================
    def _recover_inflight_from_db(self) -> int:
        """
        Ripristina le chiavi inflight da saghe pendenti al restart.
        """
        count = 0
        try:
            if not hasattr(self.db, "get_pending_sagas"):
                return 0

            pending = self.db.get_pending_sagas() or []
            with self._lock:
                for item in pending:
                    customer_ref = str(item.get("customer_ref") or "").strip()
                    if not customer_ref:
                        continue
                    self._inflight_keys.add(customer_ref)
                    count += 1
        except Exception:
            logger.exception("Errore recovery inflight da DB")
        return count

    def _handle_reconcile_now(self, payload=None):
        try:
            if self.reconciliation_engine and hasattr(self.reconciliation_engine, "run_once"):
                return self._run_in_background(self.reconciliation_engine.run_once, payload)

            if self.reconciliation_engine and hasattr(self.reconciliation_engine, "reconcile"):
                return self._run_in_background(self.reconciliation_engine.reconcile, payload)

            return None
        except Exception:
            logger.exception("Errore reconcile hook")
            return None

    def _handle_recover_pending(self, payload=None):
        try:
            self._recover_inflight_from_db()

            if self.state_recovery and hasattr(self.state_recovery, "recover_pending"):
                return self._run_in_background(self.state_recovery.recover_pending, payload)

            if self.state_recovery and hasattr(self.state_recovery, "run"):
                return self._run_in_background(self.state_recovery.run, payload)

            return None
        except Exception:
            logger.exception("Errore recovery hook")
            return None

    def recover_after_restart(self) -> Dict[str, Any]:
        """
        API diretta per recovery esplicita al riavvio.
        """
        try:
            restored = self._recover_inflight_from_db()
            self._handle_recover_pending({"source": "recover_after_restart"})
            self._handle_reconcile_now({"source": "recover_after_restart"})
            return {
                "ok": True,
                "status": "RECOVERY_TRIGGERED",
                "restored_inflight": restored,
            }
        except Exception as exc:
            logger.exception("Errore recover_after_restart: %s", exc)
            return {"ok": False, "status": "RECOVERY_FAILED", "error": str(exc)}

    # =========================================================
    # OPTIONAL EXTENSIONS
    # =========================================================
    def submit_quick_bet(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._handle_quick_bet(payload)