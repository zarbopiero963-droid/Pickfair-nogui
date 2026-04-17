from __future__ import annotations

import logging
from typing import Any, Dict

from trading_config import enforce_betfair_italy_commission_pct

logger = logging.getLogger(__name__)


class MarketNetRealizedSettlementAggregator:
    def __init__(self, *, commission_pct: float, context: str):
        self._commission_pct = float(commission_pct or 0.0)
        self._context = str(context or "market_net_realized")
        self._ledger: Dict[str, Dict[str, float]] = {}

    @property
    def ledger(self) -> Dict[str, Dict[str, float]]:
        return self._ledger

    @ledger.setter
    def ledger(self, value: Dict[str, Dict[str, float]]) -> None:
        self._ledger = value if isinstance(value, dict) else {}

    def apply(self, *, market_id: str, gross_pnl: float) -> dict[str, float | str]:
        market_key = str(market_id or "").strip()
        if not market_key:
            raise ValueError("market_id is required for realized settlement")

        commission_pct = enforce_betfair_italy_commission_pct(
            self._commission_pct,
            context=self._context,
        )
        ledger_row = self._ledger.setdefault(
            market_key,
            {"gross": 0.0, "commission": 0.0},
        )
        previous_market_gross = float(ledger_row.get("gross", 0.0))
        previous_market_commission = float(ledger_row.get("commission", 0.0))
        market_gross_after = previous_market_gross + float(gross_pnl or 0.0)
        desired_market_commission = 0.0
        if market_gross_after > 0.0 and commission_pct > 0.0:
            desired_market_commission = market_gross_after * (commission_pct / 100.0)

        commission_delta = desired_market_commission - previous_market_commission
        net_pnl = float(gross_pnl or 0.0) - commission_delta
        ledger_row["gross"] = market_gross_after
        ledger_row["commission"] = desired_market_commission
        return {
            "gross_pnl": float(gross_pnl or 0.0),
            "commission_amount": float(commission_delta),
            "net_pnl": float(net_pnl),
            "commission_pct": float(commission_pct),
            "market_net_gross": float(market_gross_after),
            "market_commission_amount_total": float(desired_market_commission),
            "settlement_basis": "market_net_realized",
        }


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
        self._market_net_realized_aggregator = MarketNetRealizedSettlementAggregator(
            commission_pct=(self.commission * 100.0),
            context="core_pnl_engine_realized_settlement",
        )

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
        matched_price = payload.get("avg_price_matched")
        if matched_price is None:
            matched_price = payload.get("matched_price")
        if matched_price is None:
            matched_price = payload.get("price")

        matched_size = payload.get("matched_size")
        if matched_size is None:
            matched_size = payload.get("stake")

        self._positions[event_key] = {
            "event_key": event_key,
            "market_id": payload.get("market_id"),
            "selection_id": payload.get("selection_id"),
            "side": str(payload.get("bet_type", "BACK")),
            "price": float(matched_price or 0.0),
            "stake": float(matched_size or 0.0),
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

            settlement = self._calc_settlement(pos, market_book)
            pnl = float(settlement["net_pnl"])

            # 🎯 LOGICA USCITA
            if pnl >= pos["stake"] * 0.03 or pnl <= -pos["stake"] * 0.05:
                self._close(pos, settlement)

    # =========================================================
    # PNL CALC
    # =========================================================
    def _calc(self, pos, market_book):
        return float(self._calc_settlement(pos, market_book)["net_pnl"])

    def _calc_settlement(self, pos, market_book):
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
                return {
                    "gross_pnl": 0.0,
                    "commission_amount": 0.0,
                    "net_pnl": 0.0,
                    "commission_pct": float(self.commission * 100.0),
                    "settlement_source": "core_pnl_engine",
                    "settlement_kind": "mark_to_market_estimate",
                }

            if side == "BACK":
                gross_pnl = (entry - lay) * stake
            else:
                gross_pnl = (back - entry) * stake

            # 💰 commissione applicata solo su profitto positivo
            commission_amount = self._commission_amount(gross_pnl)
            pnl_net = gross_pnl - commission_amount

            return {
                "gross_pnl": float(gross_pnl),
                "commission_amount": float(commission_amount),
                "net_pnl": float(pnl_net),
                "commission_pct": float(self.commission * 100.0),
                "settlement_source": "core_pnl_engine",
                "settlement_kind": "mark_to_market_estimate",
            }

        return {
            "gross_pnl": 0.0,
            "commission_amount": 0.0,
            "net_pnl": 0.0,
            "commission_pct": float(self.commission * 100.0),
            "settlement_source": "core_pnl_engine",
            "settlement_kind": "mark_to_market_estimate",
        }

    def _commission_amount(self, gross_pnl: float) -> float:
        gross_pnl = float(gross_pnl or 0.0)
        if gross_pnl <= 0.0:
            return 0.0
        return gross_pnl * float(self.commission)

    def _apply_realized_market_net_commission(self, *, market_id: str, gross_pnl: float) -> dict[str, float | str]:
        return self._market_net_realized_aggregator.apply(market_id=market_id, gross_pnl=gross_pnl)

    # =========================================================
    # CLOSE
    # =========================================================
    def _close(self, pos, settlement):
        settlement = dict(settlement or {})
        market_id = str(pos.get("market_id") or "").strip()
        gross_pnl = float(settlement.get("gross_pnl", settlement.get("net_pnl", 0.0)) or 0.0)
        realized = self._apply_realized_market_net_commission(market_id=market_id, gross_pnl=gross_pnl)
        net_pnl = float(realized["net_pnl"])
        commission_amount = float(realized["commission_amount"])
        commission_pct = float(realized["commission_pct"])
        settlement_source = str(
            settlement.get("settlement_source")
            or settlement.get("source")
            or "core_pnl_engine"
        )
        settlement_kind = "realized_settlement"
        payload = {
            "event_key": pos["event_key"],
            "market_id": market_id,
            "table_id": pos["table_id"],
            "batch_id": pos["batch_id"],
            # legacy alias (net pnl) kept for compatibility
            "pnl": net_pnl,
            "gross_pnl": gross_pnl,
            "commission_amount": commission_amount,
            "net_pnl": net_pnl,
            "commission_pct": commission_pct,
            "market_net_gross": float(realized["market_net_gross"]),
            "market_commission_amount_total": float(realized["market_commission_amount_total"]),
            "settlement_basis": str(realized["settlement_basis"]),
            "settlement_source": settlement_source,
            "settlement_kind": settlement_kind,
        }

        logger.info(f"[PnL] Close {pos['event_key']} pnl={net_pnl:.2f}")

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
