from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from order_manager import OrderManager

logger = logging.getLogger(__name__)


class TradingEngine:
    """
    Trading engine headless.

    Responsabilità:
    - ascolta CMD_QUICK_BET
    - normalizza payload
    - decide il routing verso OrderManager
    - supporta LIVE e SIMULATION
    - non blocca l'EventBus con attese inutili

    Non contiene:
    - logica UI
    - money management
    - parsing Telegram
    """

    MIN_EXCHANGE_STAKE = 2.0
    MICRO_MIN_STAKE = 0.10

    def __init__(self, bus, db, client_getter, executor=None):
        self.bus = bus
        self.db = db
        self.client_getter = client_getter
        self.executor = executor

        self.order_manager = OrderManager(
            bus=bus,
            db=db,
            client_getter=client_getter,
        )

        self.bus.subscribe("CMD_QUICK_BET", self._handle_quick_bet)

    # =========================================================
    # INTERNAL HELPERS
    # =========================================================
    def _publish(self, event_name: str, payload: Dict[str, Any]) -> None:
        try:
            self.bus.publish(event_name, payload)
        except Exception:
            logger.exception("Errore publish %s", event_name)

    def _submit(self, fn, *args, **kwargs):
        """
        Esegue via executor senza bloccare il consumer dell'EventBus.
        """
        if self.executor and hasattr(self.executor, "submit"):
            try:
                return self.executor.submit("trading_engine", fn, *args, **kwargs)
            except TypeError:
                try:
                    return self.executor.submit(fn, *args, **kwargs)
                except Exception:
                    logger.exception("Executor submit fallita, fallback sync")
        return fn(*args, **kwargs)

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
            None if normalized.get("table_id") in (None, "") else self._safe_int(normalized.get("table_id"))
        )

        return normalized

    def _is_microstake(self, stake: float) -> bool:
        stake = float(stake or 0.0)
        return self.MICRO_MIN_STAKE <= stake < self.MIN_EXCHANGE_STAKE

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
    # MAIN HANDLER
    # =========================================================
    def _handle_quick_bet(self, payload):
        fail_payload: Dict[str, Any] = dict(payload or {})

        try:
            normalized = self._normalize_payload(payload)
            self._validate_payload(normalized)

            normalized["microstake_mode"] = self._is_microstake(normalized["stake"])

            # Ack presa in carico
            self._publish(
                "QUICK_BET_ROUTED",
                {
                    **normalized,
                    "status": "ACCEPTED_FOR_PROCESSING",
                },
            )

            # Dispatch non bloccante
            self._submit(self.order_manager.place_order, normalized)

            return {
                "ok": True,
                "status": "ACCEPTED_FOR_PROCESSING",
                "simulation_mode": bool(normalized.get("simulation_mode", False)),
            }

        except Exception as exc:
            fail_payload["error"] = str(exc)
            self._publish("QUICK_BET_FAILED", fail_payload)
            logger.exception("Errore _handle_quick_bet: %s", exc)
            return {
                "ok": False,
                "status": "FAILED",
                "error": str(exc),
            }

    # =========================================================
    # OPTIONAL EXTENSIONS
    # =========================================================
    def submit_quick_bet(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        API diretta opzionale oltre al bus.
        """
        return self._handle_quick_bet(payload)