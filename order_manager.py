from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


class OrderManager:
    """
    Manager ordini con:
    - customer_ref end-to-end
    - saga persistente
    - parsing robusto della risposta BetfairClient
    - eventi downstream verso EventBus
    """

    TERMINAL_OK = {"SUCCESS", "PROCESSED_WITH_ERRORS", "PROCESSED"}
    LEG_OK = {"SUCCESS"}
    LEG_FAIL = {"FAILURE", "FAILED", "ERROR"}

    def __init__(self, db, bus=None, client_getter=None):
        self.db = db
        self.bus = bus
        self.client_getter = client_getter

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

    def _client(self):
        if not self.client_getter:
            return None
        return self.client_getter()

    def _extract_instruction_report(self, response: Dict[str, Any]) -> Dict[str, Any]:
        reports = response.get("instructionReports") or response.get("instruction_reports") or []
        if not reports:
            return {}
        return reports[0] or {}

    def _extract_customer_ref(self, payload: Dict[str, Any]) -> str:
        return str(payload.get("customer_ref") or uuid.uuid4().hex)

    def _extract_batch_id(self, payload: Dict[str, Any]) -> str:
        return str(payload.get("batch_id") or "")

    def _extract_event_key(self, payload: Dict[str, Any]) -> str:
        return str(payload.get("event_key") or "")

    def _extract_table_id(self, payload: Dict[str, Any]) -> Optional[int]:
        value = payload.get("table_id")
        return int(value) if value is not None else None

    # =========================================================
    # PLACE ORDER
    # =========================================================
    def place_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        client = self._client()
        if client is None:
            raise RuntimeError("Betfair client non disponibile")

        market_id = str(payload["market_id"])
        selection_id = int(payload["selection_id"])
        bet_type = str(payload.get("bet_type", "BACK")).upper()
        price = float(payload["price"])
        stake = float(payload["stake"])

        customer_ref = self._extract_customer_ref(payload)
        batch_id = self._extract_batch_id(payload)
        event_key = self._extract_event_key(payload)
        table_id = self._extract_table_id(payload)

        saga_payload = dict(payload)
        saga_payload["customer_ref"] = customer_ref

        self.db.create_order_saga(
            customer_ref=customer_ref,
            batch_id=batch_id,
            event_key=event_key,
            table_id=table_id,
            market_id=market_id,
            selection_id=selection_id,
            bet_type=bet_type,
            price=price,
            stake=stake,
            payload=saga_payload,
            status="PENDING",
        )

        self._publish(
            "QUICK_BET_SUBMITTED",
            {
                **saga_payload,
                "customer_ref": customer_ref,
            },
        )

        try:
            response = client.place_bet(
                market_id=market_id,
                selection_id=selection_id,
                side=bet_type,
                price=price,
                size=stake,
            )
        except Exception as exc:
            self.db.update_order_saga(
                customer_ref=customer_ref,
                status="FAILED",
                error_text=str(exc),
            )
            failure_payload = {
                **saga_payload,
                "customer_ref": customer_ref,
                "error": str(exc),
            }
            self._publish("QUICK_BET_FAILED", failure_payload)
            return {
                "ok": False,
                "status": "FAILED",
                "customer_ref": customer_ref,
                "error": str(exc),
            }

        instruction_report = self._extract_instruction_report(response)
        leg_status = str(instruction_report.get("status") or "").upper()
        bet_id = str(instruction_report.get("betId") or "")
        size_matched = float(instruction_report.get("sizeMatched", 0.0) or 0.0)

        if leg_status in self.LEG_OK:
            if size_matched > 0 and size_matched < stake:
                saga_status = "PARTIAL"
                event_name = "QUICK_BET_PARTIAL"
            elif size_matched >= stake and stake > 0:
                saga_status = "MATCHED"
                event_name = "QUICK_BET_FILLED"
            else:
                saga_status = "PLACED"
                event_name = "QUICK_BET_ACCEPTED"
        elif leg_status in self.LEG_FAIL:
            saga_status = "FAILED"
            event_name = "QUICK_BET_FAILED"
        else:
            # fallback robusto
            overall_status = str(response.get("status") or "").upper()
            if overall_status in self.TERMINAL_OK:
                saga_status = "PLACED"
                event_name = "QUICK_BET_ACCEPTED"
            else:
                saga_status = "FAILED"
                event_name = "QUICK_BET_FAILED"

        self.db.update_order_saga(
            customer_ref=customer_ref,
            status=saga_status,
            bet_id=bet_id,
            error_text="" if saga_status != "FAILED" else str(response),
        )

        out_payload = {
            **saga_payload,
            "customer_ref": customer_ref,
            "bet_id": bet_id,
            "response": response,
            "order_status": saga_status,
        }
        self._publish(event_name, out_payload)

        return {
            "ok": saga_status != "FAILED",
            "status": saga_status,
            "customer_ref": customer_ref,
            "bet_id": bet_id,
            "response": response,
        }

    # =========================================================
    # ROLLBACK / CANCEL PLACEHOLDER
    # =========================================================
    def mark_rollback_pending(self, customer_ref: str, reason: str = "") -> None:
        self.db.update_order_saga(
            customer_ref=customer_ref,
            status="ROLLBACK_PENDING",
            error_text=reason,
        )
        saga = self.db.get_order_saga(customer_ref)
        self._publish(
            "QUICK_BET_ROLLBACK_PENDING",
            {
                "customer_ref": customer_ref,
                "reason": reason,
                "saga": saga,
            },
        )

    def mark_rolled_back(self, customer_ref: str, reason: str = "") -> None:
        self.db.update_order_saga(
            customer_ref=customer_ref,
            status="ROLLED_BACK",
            error_text=reason,
        )
        saga = self.db.get_order_saga(customer_ref)
        self._publish(
            "QUICK_BET_ROLLBACK_DONE",
            {
                "customer_ref": customer_ref,
                "reason": reason,
                "saga": saga,
            },
        )
