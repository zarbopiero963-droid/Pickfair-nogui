from __future__ import annotations

import logging
import uuid
from typing import Any, Dict

logger = logging.getLogger("OrderManager")


class OrderManager:
    """
    Order Manager unificato:
    - LIVE: usa BetfairClient
    - SIM : usa SimulationBroker

    Il routing dipende dal broker restituito da client_getter().
    """

    TERMINAL_OK = {"SUCCESS", "PROCESSED_WITH_ERRORS", "PROCESSED"}
    LEG_OK = {"SUCCESS"}
    LEG_FAIL = {"FAILURE", "FAILED", "ERROR"}

    def __init__(self, app: Any = None, bus: Any = None, db: Any = None, client_getter=None):
        self.app = app
        self.bus = bus if bus is not None else getattr(app, "bus", None)
        self.db = db if db is not None else getattr(app, "db", None)
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
        if callable(self.client_getter):
            return self.client_getter()
        if self.app is not None:
            return getattr(self.app, "betfair_client", None)
        return None

    def _extract_customer_ref(self, payload: Dict[str, Any]) -> str:
        ref = payload.get("customer_ref")
        return str(ref) if ref else uuid.uuid4().hex

    def _extract_instruction_report(self, response: Dict[str, Any]) -> Dict[str, Any]:
        reports = response.get("instructionReports") or response.get("instruction_reports") or []
        if not reports:
            return {}
        return reports[0] or {}

    def _is_simulated_response(self, response: Dict[str, Any]) -> bool:
        if bool(response.get("simulated", False)):
            return True
        report = self._extract_instruction_report(response)
        return bool(report.get("simulated", False))

    # =========================================================
    # MAIN API
    # =========================================================
    def place_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        client = self._client()
        if client is None:
            raise RuntimeError("Broker client non disponibile")

        market_id = str(payload["market_id"])
        selection_id = int(payload["selection_id"])
        bet_type = str(payload.get("bet_type", "BACK")).upper()
        price = float(payload["price"])
        stake = float(payload["stake"])
        simulation_mode = bool(payload.get("simulation_mode", False))

        customer_ref = self._extract_customer_ref(payload)
        batch_id = str(payload.get("batch_id") or "")
        event_key = str(payload.get("event_key") or "")
        table_id = payload.get("table_id")

        saga_payload = dict(payload)
        saga_payload["customer_ref"] = customer_ref

        if self.db and hasattr(self.db, "create_order_saga"):
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
                customer_ref=customer_ref,
                event_key=event_key,
                table_id=table_id,
                batch_id=batch_id,
                event_name=str(payload.get("event_name") or ""),
                market_name=str(payload.get("market_name") or ""),
                runner_name=str(payload.get("runner_name") or ""),
            )
        except TypeError:
            # fallback compatibilità BetfairClient reale
            try:
                response = client.place_bet(
                    market_id=market_id,
                    selection_id=selection_id,
                    side=bet_type,
                    price=price,
                    size=stake,
                )
            except Exception as exc:
                if self.db and hasattr(self.db, "update_order_saga"):
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
        except Exception as exc:
            if self.db and hasattr(self.db, "update_order_saga"):
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
        avg_matched = float(instruction_report.get("averagePriceMatched", 0.0) or 0.0)
        simulated_response = self._is_simulated_response(response) or simulation_mode

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
            overall_status = str(response.get("status") or "").upper()
            if overall_status in self.TERMINAL_OK:
                if size_matched > 0 and size_matched < stake:
                    saga_status = "PARTIAL"
                    event_name = "QUICK_BET_PARTIAL"
                elif size_matched >= stake and stake > 0:
                    saga_status = "MATCHED"
                    event_name = "QUICK_BET_FILLED"
                else:
                    saga_status = "PLACED"
                    event_name = "QUICK_BET_ACCEPTED"
            else:
                saga_status = "FAILED"
                event_name = "QUICK_BET_FAILED"

        if self.db and hasattr(self.db, "update_order_saga"):
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
            "matched": float(size_matched),
            "average_price_matched": float(avg_matched),
            "sim": bool(simulated_response),
        }
        self._publish(event_name, out_payload)

        return {
            "ok": saga_status != "FAILED",
            "status": saga_status,
            "customer_ref": customer_ref,
            "bet_id": bet_id,
            "response": response,
            "simulated": bool(simulated_response),
        }

    # =========================================================
    # ROLLBACK STATE HELPERS
    # =========================================================
    def mark_rollback_pending(self, customer_ref: str, reason: str = "") -> None:
        if self.db and hasattr(self.db, "update_order_saga"):
            self.db.update_order_saga(
                customer_ref=customer_ref,
                status="ROLLBACK_PENDING",
                error_text=reason,
            )

        saga = (
            self.db.get_order_saga(customer_ref)
            if self.db and hasattr(self.db, "get_order_saga")
            else None
        )

        self._publish(
            "QUICK_BET_ROLLBACK_PENDING",
            {
                "customer_ref": customer_ref,
                "reason": reason,
                "saga": saga,
            },
        )

    def mark_rolled_back(self, customer_ref: str, reason: str = "") -> None:
        if self.db and hasattr(self.db, "update_order_saga"):
            self.db.update_order_saga(
                customer_ref=customer_ref,
                status="ROLLED_BACK",
                error_text=reason,
            )

        saga = (
            self.db.get_order_saga(customer_ref)
            if self.db and hasattr(self.db, "get_order_saga")
            else None
        )

        self._publish(
            "QUICK_BET_ROLLBACK_DONE",
            {
                "customer_ref": customer_ref,
                "reason": reason,
                "saga": saga,
            },
        )