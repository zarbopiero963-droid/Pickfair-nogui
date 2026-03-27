from __future__ import annotations

import logging
from typing import Any, Dict


logger = logging.getLogger(__name__)


class PnLEngine:
    """
    PnL Engine completo.

    - tracking posizioni
    - mark-to-market
    - chiusura automatica
    - publish RUNTIME_CLOSE_POSITION
    """

    def __init__(self, bus=None, commission_pct: float = 4.5):
        self.bus = bus
        self._positions: Dict[str, Dict[str, Any]] = {}
        self.commission = float(commission_pct) / 100.0

        if self.bus:
            self.bus.subscribe("QUICK_BET_FILLED", self._on_filled)
            self.bus.subscribe("QUICK_BET_PARTIAL", self._on_filled)
            self.bus.subscribe("MARKET_BOOK_UPDATE", self._on_market)

    # =========================================================
    # POSITION TRACKING
    # =========================================================
    def _on_filled(self, payload):
        event_key = str(payload.get("event_key") or "")
        if not event_key:
            return

        self._positions[event_key] = {
            "event_key": event_key,
            "market_id": payload.get("market_id"),
            "selection_id": payload.get("selection_id"),
            "side": str(payload.get("bet_type", "BACK")),
            "price": float(payload.get("price") or 0.0),
            "stake": float(payload.get("stake") or 0.0),
            "table_id": payload.get("table_id"),
            "batch_id": payload.get("batch_id"),
        }

    # =========================================================
    # MARKET UPDATE
    # =========================================================
    def _on_market(self, market_book):
        market_id = str(market_book.get("marketId") or "")

        for pos in list(self._positions.values()):
            if pos["market_id"] != market_id:
                continue

            pnl = self._calc(pos, market_book)

            # 🎯 LOGICA USCITA
            if pnl >= pos["stake"] * 0.03 or pnl <= -pos["stake"] * 0.05:
                self._close(pos, pnl)

    # =========================================================
    # PNL CALC
    # =========================================================
    def _calc(self, pos, market_book):
        sel = int(pos["selection_id"])
        side = pos["side"]
        entry = pos["price"]
        stake = pos["stake"]

        for r in market_book.get("runners", []):
            if int(r.get("selectionId")) != sel:
                continue

            ex = r.get("ex", {})
            back = (ex.get("availableToBack") or [{}])[0].get("price")
            lay = (ex.get("availableToLay") or [{}])[0].get("price")

            if not back or not lay:
                return 0.0

            if side == "BACK":
                pnl = (entry - lay) * stake
            else:
                pnl = (back - entry) * stake

            # 💰 commissione
            pnl_net = pnl * (1 - self.commission)

            return float(pnl_net)

        return 0.0

    # =========================================================
    # CLOSE
    # =========================================================
    def _close(self, pos, pnl):
        payload = {
            "event_key": pos["event_key"],
            "table_id": pos["table_id"],
            "batch_id": pos["batch_id"],
            "pnl": pnl,
        }

        logger.info(f"[PnL] Close {pos['event_key']} pnl={pnl:.2f}")

        if self.bus:
            self.bus.publish("RUNTIME_CLOSE_POSITION", payload)

        self._positions.pop(pos["event_key"], None)

    # =========================================================
    # STATUS
    # =========================================================
    def snapshot(self):
        return {
            "open_positions": len(self._positions),
            "positions": list(self._positions.values()),
        }