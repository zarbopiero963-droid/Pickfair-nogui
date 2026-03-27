from __future__ import annotations

import logging
from typing import Any, Dict

from order_manager import OrderManager

logger = logging.getLogger(__name__)


class TradingEngine:
    """
    Trading engine headless.

    Ruolo:
    - ascolta CMD_QUICK_BET
    - normalizza payload
    - applica validazioni minime
    - instrada verso OrderManager
    - supporta sia LIVE che SIMULATION tramite il broker attivo
      restituito da client_getter()
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

    # =========================================================
    # HELPERS
    # =========================================================
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
        missing = [
            key
            for key in required
            if key not in normalized or normalized.get(key) in (None, "")
        ]
        if missing:
            raise ValueError(f"Payload mancante di: {', '.join(missing)}")

        normalized["market_id"] = str(normalized["market_id"])
        normalized["selection_id"] = int(normalized["selection_id"])
        normalized["bet_type"] = str(
            normalized.get("bet_type")
            or normalized.get("side")
            or normalized.get("action")
            or "BACK"
        ).upper()
        normalized["price"] = float(normalized["price"])
        normalized["stake"] = float(normalized["stake"])
        normalized["simulation_mode"] = bool(normalized.get("simulation_mode", False))
        normalized["batch_id"] = str(normalized.get("batch_id") or "")
        normalized["event_key"] = str(normalized.get("event_key") or "")
        normalized["event_name"] = str(normalized.get("event_name") or normalized.get("event") or "")
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

        return normalized

    def _is_microstake(self, stake: float) -> bool:
        return self.MICRO_MIN_STAKE <= float(stake or 0.0) < self.MIN_EXCHANGE_STAKE

    def _validate_payload(self, payload: Dict[str, Any]) -> None:
        if payload["bet_type"] not in {"BACK", "LAY"}:
            raise ValueError("bet_type non valido")

        if payload["price"] <= 1.0:
            raise ValueError("Quota non valida")

        if payload["stake"] < self.MICRO_MIN_STAKE:
            raise ValueError("Stake sotto MICRO_MIN_STAKE")

        selection_id = payload["selection_id"]
        if not isinstance(selection_id, int):
            raise ValueError("selection_id non valido")

        market_id = str(payload["market_id"]).strip()
        if not market_id:
            raise ValueError("market_id vuoto")

    def _publish_failure(self, payload: Dict[str, Any], error: str) -> Dict[str, Any]:
        fail_payload = dict(payload or {})
        fail_payload["error"] = str(error)

        self.bus.publish("QUICK_BET_FAILED", fail_payload)
        logger.error("Errore _handle_quick_bet: %s", error)

        return {
            "ok": False,
            "status": "FAILED",
            "error": str(error),
        }

    # =========================================================
    # MAIN EVENT HANDLER
    # =========================================================
    def _handle_quick_bet(self, payload):
        try:
            payload = self._normalize_payload(payload)
            self._validate_payload(payload)

            payload["microstake_mode"] = self._is_microstake(payload["stake"])

            # NB:
            # non blocchiamo l'EventBus. LIVE e SIM vengono gestiti
            # internamente da OrderManager tramite il broker attivo.
            self._submit(self.order_manager.place_order, payload)

            return {
                "ok": True,
                "status": "ACCEPTED_FOR_PROCESSING",
                "simulated": bool(payload.get("simulation_mode", False)),
                "microstake_mode": bool(payload.get("microstake_mode", False)),
            }

        except Exception as exc:
            return self._publish_failure(dict(payload or {}), str(exc))