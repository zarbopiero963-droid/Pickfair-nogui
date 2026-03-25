from __future__ import annotations

import logging
from typing import Any, Dict

from order_manager import OrderManager

logger = logging.getLogger(__name__)


class TradingEngine:
    """
    Trading engine headless.
    Mantiene il wiring EventBus + OrderManager.
    """

    MIN_EXCHANGE_STAKE = 2.0
    MICRO_MIN_STAKE = 0.10

    def __init__(self, bus, db, client_getter, executor):
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

    def _submit(self, fn, *args, **kwargs):
        """
        Esegue via executor senza bloccare il consumer EventBus.
        """
        if self.executor and hasattr(self.executor, "submit"):
            return self.executor.submit("trading_engine", fn, *args, **kwargs)
        return fn(*args, **kwargs)

    def _normalize_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(payload or {})

        required = ("market_id", "selection_id", "price", "stake")
        missing = [k for k in required if k not in normalized or normalized.get(k) in (None, "")]
        if missing:
            raise ValueError(f"Payload mancante di: {', '.join(missing)}")

        normalized["market_id"] = str(normalized["market_id"])
        normalized["selection_id"] = int(normalized["selection_id"])
        normalized["bet_type"] = str(normalized.get("bet_type", "BACK")).upper()
        normalized["price"] = float(normalized["price"])
        normalized["stake"] = float(normalized["stake"])
        normalized["simulation_mode"] = bool(normalized.get("simulation_mode", False))

        return normalized

    def _is_microstake(self, stake: float) -> bool:
        return self.MICRO_MIN_STAKE <= float(stake or 0.0) < self.MIN_EXCHANGE_STAKE

    def _handle_quick_bet(self, payload):
        try:
            payload = self._normalize_payload(payload)

            if payload["price"] <= 1.01:
                raise ValueError("Quota non valida")

            if payload["stake"] < self.MICRO_MIN_STAKE:
                raise ValueError("Stake sotto MICRO_MIN_STAKE")

            payload["microstake_mode"] = self._is_microstake(payload["stake"])

            self._submit(self.order_manager.place_order, payload)

            return {
                "ok": True,
                "status": "ACCEPTED_FOR_PROCESSING",
            }

        except Exception as exc:
            fail_payload = dict(payload or {})
            fail_payload["error"] = str(exc)
            self.bus.publish("QUICK_BET_FAILED", fail_payload)
            logger.error("Errore _handle_quick_bet: %s", exc)
            return {
                "ok": False,
                "status": "FAILED",
                "error": str(exc),
            }