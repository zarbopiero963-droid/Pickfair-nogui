from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, Optional

logger = logging.getLogger("OrderManager")


class OrderManager:
    """
    Order Manager headless reale/simulato.
    - usa il broker tramite client_getter
    - salva saga ordini nel DB
    - pubblica eventi downstream sul bus
    - gestisce customer_ref / batch_id / event_key / table_id
    - compatibile LIVE + SIMULATION
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

    def _safe_side(self, value: Any) -> str:
        side = str(value or "BACK").upper().strip()
        return side if side in {"BACK", "LAY"} else "BACK"

    def _extract_customer_ref(self, payload: Dict[str, Any]) -> str:
        ref = payload.get("customer_ref")
        return str(ref) if ref else uuid.uuid4().hex

    def _extract_instruction_report(self, response: Dict[str, Any]) -> Dict[str, Any]:
        reports = (
            response.get("instructionReports")
            or response.get("instruction_reports")
            or []
        )
        if not reports:
            return {}
        return reports[0] or {}

    def _normalize_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(payload or {})

        normalized["market_id"] = str(
            normalized.get("market_id")
            or normalized.get("marketId")
            or ""
        ).strip()
        normalized["selection_id"] = self._safe_int(
            normalized.get("selection_id", normalized.get("selectionId"))
        )
        normalized["bet_type"] = self._safe_side(
            normalized.get("bet_type")
            or normalized.get("side")
            or normalized.get("action")
            or "BACK"
        )
        normalized["price"] = self._safe_float(
            normalized.get("price", normalized.get("odds"))
        )
        normalized["stake"] = self._safe_float(
            normalized.get("stake", normalized.get("size"))
        )
        normalized["simulation_mode"] = bool(normalized.get("simulation_mode", False))
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
        normalized["event_key"] = str(normalized.get("event_key") or "")
        normalized["batch_id"] = str(normalized.get("batch_id") or "")
        normalized["customer_ref"] = self._extract_customer_ref(normalized)
        normalized["table_id"] = (
            None if normalized.get("table_id") in (None, "") else self._safe_int(normalized.get("table_id"))
        )

        return normalized

    def _validate_payload(self, payload: Dict[str, Any]) -> None:
        if not payload["market_id"]:
            raise RuntimeError("market_id mancante")
        if payload["selection_id"] <= 0:
            raise RuntimeError("selection_id non valido")
        if payload["price"] <= 1.0:
            raise RuntimeError("price non valido")
        if payload["stake"] <= 0.0:
            raise RuntimeError("stake non valido")

    def _save_saga_pending(self, payload: Dict[str, Any]) -> None:
        if self.db and hasattr(self.db, "create_order_saga"):
            self.db.create_order_saga(
                customer_ref=payload["customer_ref"],
                batch_id=payload["batch_id"],
                event_key=payload["event_key"],
                table_id=payload["table_id"],
                market_id=payload["market_id"],
                selection_id=payload["selection_id"],
                bet_type=payload["bet_type"],
                price=payload["price"],
                stake=payload["stake"],
                payload=payload,
                status="PENDING",
            )

    def _update_saga(
        self,
        *,
        customer_ref: str,
        status: str,
        bet_id: str = "",
        error_text: str = "",
    ) -> None:
        if self.db and hasattr(self.db, "update_order_saga"):
            self.db.update_order_saga(
                customer_ref=customer_ref,
                status=status,
                bet_id=bet_id,
                error_text=error_text,
            )

    # =========================================================
    # MAIN API
    # =========================================================
    def place_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        payload = self._normalize_payload(payload)
        self._validate_payload(payload)

        client = self._client()
        if client is None:
            raise RuntimeError("Broker client non disponibile")

        market_id = payload["market_id"]
        selection_id = int(payload["selection_id"])
        bet_type = payload["bet_type"]
        price = float(payload["price"])
        stake = float(payload["stake"])

        customer_ref = payload["customer_ref"]
        batch_id = payload["batch_id"]
        event_key = payload["event_key"]
        table_id = payload["table_id"]

        self._save_saga_pending(payload)

        self._publish(
            "QUICK_BET_SUBMITTED",
            {
                **payload,
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
                event_name=payload.get("event_name", ""),
                market_name=payload.get("market_name", ""),
                runner_name=payload.get("runner_name", ""),
            )
        except TypeError:
            # fallback per client live legacy che non accetta kwargs extra
            try:
                response = client.place_bet(
                    market_id=market_id,
                    selection_id=selection_id,
                    side=bet_type,
                    price=price,
                    size=stake,
                )
            except Exception as exc:
                self._update_saga(
                    customer_ref=customer_ref,
                    status="FAILED",
                    error_text=str(exc),
                )
                failure_payload = {
                    **payload,
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
            self._update_saga(
                customer_ref=customer_ref,
                status="FAILED",
                error_text=str(exc),
            )
            failure_payload = {
                **payload,
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
        size_matched = self._safe_float(instruction_report.get("sizeMatched"), 0.0)

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

        self._update_saga(
            customer_ref=customer_ref,
            status=saga_status,
            bet_id=bet_id,
            error_text="" if saga_status != "FAILED" else str(response),
        )

        out_payload = {
            **payload,
            "customer_ref": customer_ref,
            "bet_id": bet_id,
            "response": response,
            "order_status": saga_status,
            "matched_size": size_matched,
            "simulation_mode": bool(payload.get("simulation_mode", False)),
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
    # ROLLBACK HELPERS
    # =========================================================
    def mark_rollback_pending(self, customer_ref: str, reason: str = "") -> None:
        self._update_saga(
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
        self._update_saga(
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