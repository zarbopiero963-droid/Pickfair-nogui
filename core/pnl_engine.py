from __future__ import annotations

import logging
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


class PnLEngine:
    """
    PnL Engine unificato (live + simulation).

    Responsabilità:
    - tracciare posizioni aperte
    - aggiornare mark-to-market su tick
    - decidere chiusura automatica
    - pubblicare evento RUNTIME_CLOSE_POSITION
    """

    def __init__(self, bus=None):
        self.bus = bus

        # key: event_key
        self._positions: Dict[str, Dict[str, Any]] = {}

        if self.bus:
            self.bus.subscribe("QUICK_BET_FILLED", self._on_order_filled)
            self.bus.subscribe("QUICK_BET_PARTIAL", self._on_order_partial)
            self.bus.subscribe("MARKET_BOOK_UPDATE", self._on_market_update)

    # =========================================================
    # POSITION TRACKING
    # =========================================================
    def _on_order_filled(self, payload: Dict[str, Any]) -> None:
        self._register_position(payload)

    def _on_order_partial(self, payload: Dict[str, Any]) -> None:
        self._register_position(payload)

    def _register_position(self, payload: Dict[str, Any]) -> None:
        try:
            event_key = str(payload.get("event_key") or "")
            if not event_key:
                return

            position = {
                "event_key": event_key,
                "market_id": payload.get("market_id"),
                "selection_id": payload.get("selection_id"),
                "side": str(payload.get("bet_type") or "BACK"),
                "price": float(payload.get("price") or 0.0),
                "stake": float(payload.get("stake") or 0.0),
                "table_id": payload.get("table_id"),
                "batch_id": payload.get("batch_id"),
                "event_name": payload.get("event_name"),
                "closed": False,
            }

            self._positions[event_key] = position

            logger.info(f"[PnL] Posizione registrata: {event_key}")

        except Exception:
            logger.exception("Errore registrazione posizione")

    # =========================================================
    # MARKET UPDATE → PNL CALC
    # =========================================================
    def _on_market_update(self, market_book: Dict[str, Any]) -> None:
        try:
            market_id = str(market_book.get("marketId") or "")
            if not market_id:
                return

            for event_key, pos in list(self._positions.items()):
                if pos["closed"]:
                    continue

                if str(pos.get("market_id")) != market_id:
                    continue

                pnl = self._calculate_pnl(pos, market_book)

                # 🔥 LOGICA USCITA
                if self._should_close(pnl, pos):
                    self._close_position(pos, pnl)

        except Exception:
            logger.exception("Errore update pnl")

    # =========================================================
    # PNL CALCULATION
    # =========================================================
    def _calculate_pnl(self, position: Dict[str, Any], market_book: Dict[str, Any]) -> float:
        try:
            selection_id = int(position["selection_id"])
            side = position["side"]
            entry_price = float(position["price"])
            stake = float(position["stake"])

            runners = market_book.get("runners") or []

            for r in runners:
                if int(r.get("selectionId")) != selection_id:
                    continue

                ex = r.get("ex", {})
                back = (ex.get("availableToBack") or [{}])[0].get("price")
                lay = (ex.get("availableToLay") or [{}])[0].get("price")

                if not back or not lay:
                    return 0.0

                if side == "BACK":
                    # cashout via lay
                    pnl = (lay - entry_price) * stake * -1
                else:
                    pnl = (entry_price - back) * stake

                return float(pnl)

        except Exception:
            return 0.0

        return 0.0

    # =========================================================
    # EXIT LOGIC
    # =========================================================
    def _should_close(self, pnl: float, position: Dict[str, Any]) -> bool:
        """
        Regole base:
        - take profit
        - stop loss
        """

        stake = float(position.get("stake") or 0.0)

        if stake <= 0:
            return False

        # 🔥 puoi regolare queste soglie
        take_profit = stake * 0.03   # +3%
        stop_loss = -stake * 0.05    # -5%

        if pnl >= take_profit:
            return True

        if pnl <= stop_loss:
            return True

        return False

    # =========================================================
    # CLOSE POSITION
    # =========================================================
    def _close_position(self, position: Dict[str, Any], pnl: float) -> None:
        try:
            event_key = position["event_key"]

            position["closed"] = True

            payload = {
                "event_key": event_key,
                "table_id": position.get("table_id"),
                "batch_id": position.get("batch_id"),
                "pnl": float(pnl),
            }

            logger.info(f"[PnL] Chiusura posizione {event_key} pnl={pnl:.2f}")

            if self.bus:
                self.bus.publish("RUNTIME_CLOSE_POSITION", payload)

            # cleanup
            self._positions.pop(event_key, None)

        except Exception:
            logger.exception("Errore chiusura posizione")

    # =========================================================
    # STATUS
    # =========================================================
    def snapshot(self) -> Dict[str, Any]:
        return {
            "open_positions": len(self._positions),
            "positions": list(self._positions.values()),
        } 