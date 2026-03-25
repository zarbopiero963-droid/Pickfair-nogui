from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from core.dutching_batch_manager import DutchingBatchManager


logger = logging.getLogger(__name__)


class ReconciliationEngine:
    """
    Riconcilia i batch dutching al riavvio o su richiesta.

    Fonti usate:
    - dutching_batches / dutching_batch_legs
    - order_saga (se presente nel DB)
    - BetfairClient.get_current_orders(...) se il client è disponibile

    Obiettivo:
    - capire se un batch è ancora vivo, parziale, fallito o terminale
    - aggiornare le legs
    - chiudere/rilasciare batch quando possibile
    """

    def __init__(
        self,
        *,
        db,
        bus=None,
        batch_manager: Optional[DutchingBatchManager] = None,
        betfair_service=None,
        client_getter=None,
        table_manager=None,
        duplication_guard=None,
    ):
        self.db = db
        self.bus = bus
        self.batch_manager = batch_manager or DutchingBatchManager(db, bus=bus)
        self.betfair_service = betfair_service
        self.client_getter = client_getter
        self.table_manager = table_manager
        self.duplication_guard = duplication_guard

    # =========================================================
    # HELPERS
    # =========================================================
    def _publish(self, event_name: str, payload: Dict[str, Any]) -> None:
        if not self.bus:
            return
        try:
            self.bus.publish(event_name, payload)
        except Exception:
            logger.exception("Errore publish %s", event_name)

    def _get_client(self):
        if self.client_getter:
            try:
                client = self.client_getter()
                if client:
                    return client
            except Exception:
                logger.exception("Errore client_getter()")

        if self.betfair_service and hasattr(self.betfair_service, "get_client"):
            try:
                client = self.betfair_service.get_client()
                if client:
                    return client
            except Exception:
                logger.exception("Errore betfair_service.get_client()")

        return None

    def _get_pending_saga_refs(self) -> set[str]:
        result = set()
        getter = getattr(self.db, "get_pending_sagas", None)
        if not callable(getter):
            return result

        try:
            for row in getter() or []:
                ref = str(row.get("customer_ref") or "").strip()
                if ref:
                    result.add(ref)
        except Exception:
            logger.exception("Errore get_pending_sagas")
        return result

    def _fetch_current_orders_by_market(self, market_id: str) -> List[Dict[str, Any]]:
        client = self._get_client()
        if not client:
            return []

        try:
            orders = client.get_current_orders(market_ids=[market_id])
            if isinstance(orders, list):
                return orders
            return list(orders or [])
        except Exception:
            logger.exception("Errore get_current_orders market_id=%s", market_id)
            return []

    def _extract_customer_ref(self, order: Dict[str, Any]) -> str:
        candidates = [
            order.get("customerOrderRef"),
            order.get("customer_ref"),
            order.get("customerRef"),
            order.get("customerOrderReference"),
        ]
        for value in candidates:
            if value:
                return str(value).strip()
        return ""

    def _extract_bet_id(self, order: Dict[str, Any]) -> str:
        candidates = [
            order.get("betId"),
            order.get("bet_id"),
            order.get("betID"),
        ]
        for value in candidates:
            if value:
                return str(value).strip()
        return ""

    def _extract_order_status(self, order: Dict[str, Any]) -> str:
        candidates = [
            order.get("status"),
            order.get("orderStatus"),
            order.get("currentOrderStatus"),
        ]
        for value in candidates:
            if value:
                return str(value).strip().upper()
        return ""

    def _map_remote_status_to_leg_status(self, order: Dict[str, Any]) -> str:
        status = self._extract_order_status(order)
        size_matched = float(order.get("sizeMatched", 0.0) or 0.0)
        size_remaining = float(order.get("sizeRemaining", 0.0) or 0.0)

        if status in {"EXECUTION_COMPLETE", "EXECUTABLE"}:
            if size_matched > 0 and size_remaining > 0:
                return "PARTIAL"
            if size_matched > 0 and size_remaining <= 0:
                return "MATCHED"
            return "PLACED"

        if status in {"CANCELLED", "LAPSED", "VOIDED"}:
            return "CANCELLED"

        if status in {"FAILED", "REJECTED"}:
            return "FAILED"

        return "SUBMITTED"

    def _load_batch_order_map(self, batch: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        market_id = str(batch.get("market_id") or "")
        orders = self._fetch_current_orders_by_market(market_id)
        by_ref: Dict[str, Dict[str, Any]] = {}

        for order in orders:
            ref = self._extract_customer_ref(order)
            if ref:
                by_ref[ref] = order

        return by_ref

    # =========================================================
    # RECONCILIATION
    # =========================================================
    def reconcile_all_open_batches(self) -> Dict[str, Any]:
        batches = self.batch_manager.get_open_batches()
        reconciled = []
        failed = []

        for batch in batches:
            batch_id = str(batch.get("batch_id") or "")
            try:
                result = self.reconcile_batch(batch_id)
                reconciled.append(result)
            except Exception as exc:
                logger.exception("Errore reconcile batch_id=%s", batch_id)
                failed.append(
                    {
                        "batch_id": batch_id,
                        "error": str(exc),
                    }
                )

        summary = {
            "ok": len(failed) == 0,
            "reconciled_count": len(reconciled),
            "failed_count": len(failed),
            "reconciled": reconciled,
            "failed": failed,
        }
        self._publish("RECONCILIATION_ALL_DONE", summary)
        return summary

    def reconcile_batch(self, batch_id: str) -> Dict[str, Any]:
        batch = self.batch_manager.get_batch(batch_id)
        if not batch:
            return {"ok": False, "error": "Batch non trovato", "batch_id": batch_id}

        legs = self.batch_manager.get_batch_legs(batch_id)
        if not legs:
            self.batch_manager.mark_batch_failed(batch_id, reason="Batch senza legs")
            self.batch_manager.release_runtime_artifacts(
                batch_id=batch_id,
                duplication_guard=self.duplication_guard,
                table_manager=self.table_manager,
            )
            result = {
                "ok": True,
                "batch_id": batch_id,
                "status": "FAILED",
                "reason": "Batch senza legs",
            }
            self._publish("RECONCILIATION_BATCH_DONE", result)
            return result

        pending_saga_refs = self._get_pending_saga_refs()
        remote_by_ref = self._load_batch_order_map(batch)

        found_remote = 0
        still_pending = 0
        resolved_failures = 0

        for leg in legs:
            leg_index = int(leg["leg_index"])
            current_status = str(leg.get("status") or "").upper()
            customer_ref = str(leg.get("customer_ref") or "").strip()

            # Se la gamba è già terminale, non la tocchiamo.
            if current_status in {"MATCHED", "FAILED", "CANCELLED", "ROLLED_BACK"}:
                continue

            remote_order = remote_by_ref.get(customer_ref) if customer_ref else None

            if remote_order:
                found_remote += 1
                leg_status = self._map_remote_status_to_leg_status(remote_order)
                bet_id = self._extract_bet_id(remote_order)
                self.batch_manager.update_leg_status(
                    batch_id=batch_id,
                    leg_index=leg_index,
                    status=leg_status,
                    bet_id=bet_id,
                    raw_response=remote_order,
                )
                continue

            if customer_ref and customer_ref in pending_saga_refs:
                still_pending += 1
                # resta aperto: non forziamo failure
                continue

            # Se era SUBMITTED/CREATED/PLACED/PARTIAL ma non esiste né remoto né saga pending,
            # lo segniamo come FAILED conservativamente.
            if current_status in {"CREATED", "SUBMITTED", "PLACED", "PARTIAL"}:
                resolved_failures += 1
                self.batch_manager.update_leg_status(
                    batch_id=batch_id,
                    leg_index=leg_index,
                    status="FAILED",
                    error_text="Non trovato né su exchange né in saga pending",
                )

        new_batch = self.batch_manager.recompute_batch_status(batch_id)
        status = str((new_batch or {}).get("status") or "")

        # Se batch terminale, rilascia lock/tavolo
        if status in {"EXECUTED", "ROLLED_BACK", "FAILED", "CANCELLED"}:
            self.batch_manager.release_runtime_artifacts(
                batch_id=batch_id,
                duplication_guard=self.duplication_guard,
                table_manager=self.table_manager,
            )

        result = {
            "ok": True,
            "batch_id": batch_id,
            "status": status,
            "found_remote_orders": found_remote,
            "still_pending_sagas": still_pending,
            "resolved_failures": resolved_failures,
        }

        self._publish("RECONCILIATION_BATCH_DONE", result)
        return result

    # =========================================================
    # POLICY HELPERS
    # =========================================================
    def mark_partial_as_rollback_pending(self, batch_id: str, reason: str = "") -> Dict[str, Any]:
        batch = self.batch_manager.get_batch(batch_id)
        if not batch:
            return {"ok": False, "error": "Batch non trovato", "batch_id": batch_id}

        if str(batch.get("status") or "") not in {"PARTIAL", "LIVE"}:
            return {
                "ok": False,
                "error": f"Batch non rollbackabile nello stato {batch.get('status')}",
                "batch_id": batch_id,
            }

        self.batch_manager.mark_batch_rollback_pending(batch_id, reason=reason or "Rollback richiesto")
        result = {
            "ok": True,
            "batch_id": batch_id,
            "status": "ROLLBACK_PENDING",
        }
        self._publish("RECONCILIATION_ROLLBACK_PENDING", result)
        return result

    def finalize_terminal_batch(
        self,
        batch_id: str,
        *,
        status: str,
        reason: str = "",
        pnl: float = 0.0,
    ) -> Dict[str, Any]:
        batch = self.batch_manager.get_batch(batch_id)
        if not batch:
            return {"ok": False, "error": "Batch non trovato", "batch_id": batch_id}

        status = str(status or "").upper()
        if status not in {"EXECUTED", "ROLLED_BACK", "FAILED", "CANCELLED"}:
            return {"ok": False, "error": f"Stato terminale non valido: {status}", "batch_id": batch_id}

        self.batch_manager.update_batch_status(batch_id, status, notes=reason)
        self.batch_manager.release_runtime_artifacts(
            batch_id=batch_id,
            duplication_guard=self.duplication_guard,
            table_manager=self.table_manager,
            pnl=float(pnl or 0.0),
        )

        result = {
            "ok": True,
            "batch_id": batch_id,
            "status": status,
            "reason": reason,
            "pnl": float(pnl or 0.0),
        }
        self._publish("RECONCILIATION_TERMINALIZED", result)
        return result
