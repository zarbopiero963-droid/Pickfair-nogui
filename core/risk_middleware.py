"""
Risk Middleware - OMS Puro

Responsabilità:
1. Anti-duplicate / anti double-click
2. Normalizzazione payload
3. Forwarding REQ_* -> CMD_*

NON fa:
- decisioni strategiche
- filtri WoM
- filtri volatilità
- validazioni qualitative del trade
"""

import hashlib
import json
import logging
import threading
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)


class RiskMiddleware:
    def __init__(self, bus, guardrail=None, wom_engine=None):
        self.bus = bus

        # Tenuti solo per compatibilità con il wiring esistente del main.py
        self.guardrail = guardrail
        self.wom_engine = wom_engine

        self._recent_requests: Dict[str, float] = {}
        self._duplicate_window_sec = 2.0
        self._gc_window_sec = 15.0
        self._lock = threading.Lock()

        self.bus.subscribe("REQ_QUICK_BET", self._handle_quick_bet)
        self.bus.subscribe("REQ_PLACE_DUTCHING", self._handle_dutching)
        self.bus.subscribe("REQ_EXECUTE_CASHOUT", self._handle_cashout)

        # Compatibilità con eventuale lifecycle ordini legacy/UI
        self.bus.subscribe("REQ_CANCEL_ORDER", self._handle_cancel_order)
        self.bus.subscribe("REQ_REPLACE_ORDER", self._handle_replace_order)

    # =========================================================
    # INTERNALS
    # =========================================================

    def _make_hashable_payload(self, payload: Any) -> Any:
        if isinstance(payload, dict):
            return {
                str(k): self._make_hashable_payload(v)
                for k, v in sorted(payload.items())
            }
        if isinstance(payload, list):
            return [self._make_hashable_payload(v) for v in payload]
        if isinstance(payload, tuple):
            return [self._make_hashable_payload(v) for v in payload]
        if isinstance(payload, (str, int, float, bool)) or payload is None:
            return payload
        return str(payload)

    def _request_hash(self, payload: Dict[str, Any]) -> str:
        safe_payload = self._make_hashable_payload(payload)
        encoded = json.dumps(safe_payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _cleanup_old_requests(self) -> None:
        now = time.time()
        self._recent_requests = {
            req_hash: ts
            for req_hash, ts in self._recent_requests.items()
            if now - ts <= self._gc_window_sec
        }

    def _is_duplicate(self, payload: Dict[str, Any]) -> bool:
        try:
            req_hash = self._request_hash(payload)
            now = time.time()

            with self._lock:
                self._cleanup_old_requests()

                previous_ts = self._recent_requests.get(req_hash)
                if previous_ts is not None and (
                    now - previous_ts
                ) < self._duplicate_window_sec:
                    return True

                self._recent_requests[req_hash] = now
                return False
        except Exception as e:
            logger.error(f"[RiskMiddleware] Errore calcolo duplicate hash: {e}")
            return False

    # =========================================================
    # REQ -> CMD
    # =========================================================

    def _handle_quick_bet(self, payload: Dict[str, Any]) -> None:
        if self._is_duplicate(payload):
            logger.warning("[RiskMiddleware] REQ_QUICK_BET duplicata ignorata.")
            return

        try:
            copy_meta = payload.get("copy_meta")
            pattern_meta = payload.get("pattern_meta")
            source = str(payload.get("source", "UI"))

            order_origin = str(
                payload.get("order_origin")
                or ("COPY" if isinstance(copy_meta, dict) else "PATTERN" if isinstance(pattern_meta, dict) else source)
            ).upper()

            normalized = {
                "market_id": str(payload.get("market_id", "")),
                "market_type": str(payload.get("market_type", "MATCH_ODDS")),
                "event_name": str(payload.get("event_name", "")),
                "market_name": str(payload.get("market_name", "")),
                "selection_id": int(payload.get("selection_id")),
                "runner_name": str(
                    payload.get("runner_name", payload.get("selection_id", ""))
                ),
                "bet_type": str(payload.get("bet_type", "BACK")).upper(),
                "price": float(payload.get("price", 0.0)),
                "stake": float(payload.get("stake", 0.0)),
                "simulation_mode": bool(payload.get("simulation_mode", False)),
                "source": source,
                "order_origin": order_origin,
            }
            if isinstance(copy_meta, dict):
                normalized["copy_meta"] = dict(copy_meta)
            if isinstance(pattern_meta, dict):
                normalized["pattern_meta"] = dict(pattern_meta)
        except Exception as e:
            logger.error(
                f"[RiskMiddleware] Payload QUICK_BET invalido: {e} | payload={payload}"
            )
            self.bus.publish("QUICK_BET_FAILED", f"Payload QUICK_BET invalido: {e}")
            return

        logger.info("[RiskMiddleware] Forward REQ_QUICK_BET -> CMD_QUICK_BET")
        self.bus.publish("CMD_QUICK_BET", normalized)

    def _handle_dutching(self, payload: Dict[str, Any]) -> None:
        if self._is_duplicate(payload):
            logger.warning("[RiskMiddleware] REQ_PLACE_DUTCHING duplicata ignorata.")
            return

        try:
            normalized_results = []
            for r in payload.get("results", []) or []:
                normalized_results.append(
                    {
                        "selectionId": int(r.get("selectionId")),
                        "runnerName": str(r.get("runnerName", "")),
                        "price": float(r.get("price", 0.0)),
                        "stake": float(r.get("stake", 0.0)),
                        "side": (
                            str(
                                r.get(
                                    "side",
                                    payload.get("bet_type", "BACK"),
                                )
                            ).upper()
                            if r.get("side") is not None
                            else str(payload.get("bet_type", "BACK")).upper()
                        ),
                        "effectiveType": (
                            str(
                                r.get(
                                    "effectiveType",
                                    r.get("side", payload.get("bet_type", "BACK")),
                                )
                            ).upper()
                            if (
                                r.get("effectiveType") is not None
                                or r.get("side") is not None
                            )
                            else str(payload.get("bet_type", "BACK")).upper()
                        ),
                    }
                )

            normalized = {
                "market_id": str(payload.get("market_id", "")),
                "market_type": str(payload.get("market_type", "MATCH_ODDS")),
                "event_name": str(payload.get("event_name", "")),
                "market_name": str(payload.get("market_name", "")),
                "results": normalized_results,
                "bet_type": str(payload.get("bet_type", "BACK")).upper(),
                "total_stake": float(payload.get("total_stake", 0.0)),
                "use_best_price": bool(payload.get("use_best_price", False)),
                "simulation_mode": bool(payload.get("simulation_mode", False)),
                "auto_green": bool(payload.get("auto_green", False)),
                "stop_loss": payload.get("stop_loss"),
                "take_profit": payload.get("take_profit"),
                "trailing": payload.get("trailing"),
                "source": str(payload.get("source", "UI")),
            }
        except Exception as e:
            logger.error(
                f"[RiskMiddleware] Payload DUTCHING invalido: {e} | payload={payload}"
            )
            self.bus.publish("DUTCHING_FAILED", f"Payload DUTCHING invalido: {e}")
            return

        logger.info("[RiskMiddleware] Forward REQ_PLACE_DUTCHING -> CMD_PLACE_DUTCHING")
        self.bus.publish("CMD_PLACE_DUTCHING", normalized)

    def _handle_cashout(self, payload: Dict[str, Any]) -> None:
        if self._is_duplicate(payload):
            logger.warning("[RiskMiddleware] REQ_EXECUTE_CASHOUT duplicata ignorata.")
            return

        try:
            normalized = {
                "market_id": str(payload.get("market_id", "")),
                "selection_id": int(payload.get("selection_id")),
                "side": str(payload.get("side", "LAY")).upper(),
                "stake": float(payload.get("stake", 0.0)),
                "price": float(payload.get("price", 0.0)),
                "green_up": float(payload.get("green_up", 0.0)),
                "original_pos": payload.get("original_pos"),
                "source": str(payload.get("source", "UI")),
            }
        except Exception as e:
            logger.error(
                f"[RiskMiddleware] Payload CASHOUT invalido: {e} | payload={payload}"
            )
            self.bus.publish("CASHOUT_FAILED", f"Payload CASHOUT invalido: {e}")
            return

        logger.info("[RiskMiddleware] Forward REQ_EXECUTE_CASHOUT -> CMD_EXECUTE_CASHOUT")
        self.bus.publish("CMD_EXECUTE_CASHOUT", normalized)

    def _handle_cancel_order(self, payload: Dict[str, Any]) -> None:
        if self._is_duplicate(payload):
            logger.warning("[RiskMiddleware] REQ_CANCEL_ORDER duplicata ignorata.")
            return

        try:
            normalized = {
                "market_id": str(payload.get("market_id", "")),
                "bet_id": str(payload.get("bet_id", "")),
                "source": str(payload.get("source", "UI")),
            }
        except Exception as e:
            logger.error(
                f"[RiskMiddleware] Payload CANCEL invalido: {e} | payload={payload}"
            )
            self.bus.publish("ORDER_CANCEL_FAILED", f"Payload CANCEL invalido: {e}")
            return

        logger.info("[RiskMiddleware] Forward REQ_CANCEL_ORDER -> CMD_CANCEL_ORDER")
        self.bus.publish("CMD_CANCEL_ORDER", normalized)

    def _handle_replace_order(self, payload: Dict[str, Any]) -> None:
        if self._is_duplicate(payload):
            logger.warning("[RiskMiddleware] REQ_REPLACE_ORDER duplicata ignorata.")
            return

        try:
            normalized = {
                "market_id": str(payload.get("market_id", "")),
                "bet_id": str(payload.get("bet_id", "")),
                "new_price": float(payload.get("new_price", 0.0)),
                "source": str(payload.get("source", "UI")),
            }
        except Exception as e:
            logger.error(
                f"[RiskMiddleware] Payload REPLACE invalido: {e} | payload={payload}"
            )
            self.bus.publish("ORDER_REPLACE_FAILED", f"Payload REPLACE invalido: {e}")
            return

        logger.info("[RiskMiddleware] Forward REQ_REPLACE_ORDER -> CMD_REPLACE_ORDER")
        self.bus.publish("CMD_REPLACE_ORDER", normalized)
