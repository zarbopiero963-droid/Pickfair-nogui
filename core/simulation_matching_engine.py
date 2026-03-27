from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.simulation_order_book import SimulationOrderBook
from core.simulation_state import SimulationPosition, SimulationState


@dataclass
class SimulationMatchResult:
    bet_id: str
    status: str
    matched_size: float
    average_matched_price: float
    requested_size: float
    remaining_size: float
    message: str = ""


class SimulationMatchingEngine:
    """
    Matching engine simulato.

    Regole:
    - BACK matcha contro availableToLay
    - LAY  matcha contro availableToBack
    - supporta partial fill opzionale
    - può consumare liquidità dal ladder
    """

    def __init__(
        self,
        *,
        order_book: SimulationOrderBook,
        state: SimulationState,
        partial_fill_enabled: bool = True,
        consume_liquidity: bool = True,
    ):
        self.order_book = order_book
        self.state = state
        self.partial_fill_enabled = bool(partial_fill_enabled)
        self.consume_liquidity = bool(consume_liquidity)

    # =========================================================
    # HELPERS
    # =========================================================
    def _side(self, value: Any) -> str:
        side = str(value or "BACK").upper().strip()
        return side if side in {"BACK", "LAY"} else "BACK"

    def _crosses(
        self,
        *,
        side: str,
        order_price: float,
        book_price: float,
    ) -> bool:
        if side == "BACK":
            return order_price >= book_price
        return order_price <= book_price

    def _status_from_match(self, requested: float, matched: float) -> str:
        if matched <= 0.0:
            return "EXECUTABLE"
        if matched < requested:
            return "PARTIAL"
        return "EXECUTION_COMPLETE"

    def _weighted_avg(self, fills: List[Dict[str, float]]) -> float:
        total_size = sum(float(x["size"]) for x in fills)
        if total_size <= 0.0:
            return 0.0
        weighted = sum(float(x["price"]) * float(x["size"]) for x in fills)
        return weighted / total_size

    # =========================================================
    # CORE MATCH
    # =========================================================
    def _simulate_match(
        self,
        *,
        market_id: str,
        selection_id: int,
        side: str,
        price: float,
        size: float,
    ) -> Dict[str, Any]:
        ladder = self.order_book.get_opposite_ladder(market_id, selection_id, side)
        if not ladder:
            return {
                "matched_size": 0.0,
                "average_matched_price": 0.0,
                "remaining_size": float(size),
                "fills": [],
                "status": "EXECUTABLE",
                "message": "no_liquidity",
            }

        remaining = float(size)
        fills: List[Dict[str, float]] = []

        for level in ladder:
            level_price = float(level.get("price", 0.0) or 0.0)
            level_size = float(level.get("size", 0.0) or 0.0)

            if level_price <= 0.0 or level_size <= 0.0:
                continue
            if not self._crosses(side=side, order_price=float(price), book_price=level_price):
                continue

            take = min(remaining, level_size)
            if take <= 0.0:
                continue

            fills.append({"price": level_price, "size": take})
            remaining -= take

            if remaining <= 0.0:
                break

            if not self.partial_fill_enabled:
                break

        matched = sum(x["size"] for x in fills)
        avg_price = self._weighted_avg(fills)
        status = self._status_from_match(float(size), matched)

        return {
            "matched_size": float(matched),
            "average_matched_price": float(avg_price),
            "remaining_size": float(max(0.0, remaining)),
            "fills": fills,
            "status": status,
            "message": "ok" if matched > 0.0 else "not_crossed_or_empty",
        }

    # =========================================================
    # SUBMIT ORDER
    # =========================================================
    def submit_order(
        self,
        *,
        bet_id: str,
        market_id: str,
        selection_id: int,
        side: str,
        price: float,
        size: float,
        customer_ref: str = "",
        event_key: str = "",
        table_id: Optional[int] = None,
        batch_id: str = "",
        event_name: str = "",
        market_name: str = "",
        runner_name: str = "",
    ) -> SimulationMatchResult:
        side = self._side(side)
        market_id = str(market_id or "")
        selection_id = int(selection_id)
        price = float(price or 0.0)
        size = float(size or 0.0)

        if not market_id:
            return SimulationMatchResult(
                bet_id=str(bet_id),
                status="FAILURE",
                matched_size=0.0,
                average_matched_price=0.0,
                requested_size=size,
                remaining_size=size,
                message="market_id_missing",
            )

        if price <= 1.0:
            return SimulationMatchResult(
                bet_id=str(bet_id),
                status="FAILURE",
                matched_size=0.0,
                average_matched_price=0.0,
                requested_size=size,
                remaining_size=size,
                message="invalid_price",
            )

        if size <= 0.0:
            return SimulationMatchResult(
                bet_id=str(bet_id),
                status="FAILURE",
                matched_size=0.0,
                average_matched_price=0.0,
                requested_size=size,
                remaining_size=size,
                message="invalid_size",
            )

        self.state.add_position(
            bet_id=str(bet_id),
            market_id=market_id,
            selection_id=selection_id,
            side=side,
            price=price,
            size=size,
            customer_ref=customer_ref,
            event_key=event_key,
            table_id=table_id,
            batch_id=batch_id,
            event_name=event_name,
            market_name=market_name,
            runner_name=runner_name,
        )

        match = self._simulate_match(
            market_id=market_id,
            selection_id=selection_id,
            side=side,
            price=price,
            size=size,
        )

        self.state.update_match(
            bet_id=str(bet_id),
            matched_size=float(match["matched_size"]),
            avg_price_matched=float(match["average_matched_price"]),
            status=str(match["status"]),
        )

        if self.consume_liquidity:
            for fill in match["fills"]:
                self.order_book.consume_liquidity(
                    market_id=market_id,
                    selection_id=selection_id,
                    order_side=side,
                    matched_price=float(fill["price"]),
                    matched_size=float(fill["size"]),
                )

        return SimulationMatchResult(
            bet_id=str(bet_id),
            status=str(match["status"]),
            matched_size=float(match["matched_size"]),
            average_matched_price=float(match["average_matched_price"]),
            requested_size=float(size),
            remaining_size=float(match["remaining_size"]),
            message=str(match["message"]),
        )

    # =========================================================
    # REPROCESS OPEN ORDERS
    # =========================================================
    def reprocess_open_orders(self, market_id: str) -> Dict[str, Any]:
        market_id = str(market_id or "")
        results: List[Dict[str, Any]] = []

        for pos in self.state.list_open_positions():
            if str(pos.market_id) != market_id:
                continue

            already_matched = float(pos.matched_size or 0.0)
            requested = float(pos.size or 0.0)
            remaining = max(0.0, requested - already_matched)

            if remaining <= 0.0:
                continue

            match = self._simulate_match(
                market_id=market_id,
                selection_id=int(pos.selection_id),
                side=str(pos.side),
                price=float(pos.price),
                size=float(remaining),
            )

            newly_matched = float(match["matched_size"])
            total_matched = already_matched + newly_matched

            if newly_matched > 0.0:
                if already_matched > 0.0 and float(pos.avg_price_matched or 0.0) > 0.0:
                    weighted_value = (
                        float(pos.avg_price_matched) * already_matched
                        + float(match["average_matched_price"]) * newly_matched
                    )
                    avg_price = weighted_value / total_matched
                else:
                    avg_price = float(match["average_matched_price"])
            else:
                avg_price = float(pos.avg_price_matched or 0.0)

            final_status = self._status_from_match(requested, total_matched)

            self.state.update_match(
                bet_id=str(pos.bet_id),
                matched_size=total_matched,
                avg_price_matched=avg_price,
                status=final_status,
            )

            if self.consume_liquidity:
                for fill in match["fills"]:
                    self.order_book.consume_liquidity(
                        market_id=market_id,
                        selection_id=int(pos.selection_id),
                        order_side=str(pos.side),
                        matched_price=float(fill["price"]),
                        matched_size=float(fill["size"]),
                    )

            results.append(
                {
                    "bet_id": str(pos.bet_id),
                    "newly_matched": newly_matched,
                    "total_matched": total_matched,
                    "avg_price_matched": avg_price,
                    "status": final_status,
                }
            )

        return {
            "ok": True,
            "market_id": market_id,
            "results": results,
            "processed_count": len(results),
        }

    # =========================================================
    # CANCEL
    # =========================================================
    def cancel_order(self, bet_id: str) -> Dict[str, Any]:
        pos = self.state.get_position(str(bet_id))
        if not pos:
            return {
                "ok": False,
                "reason": "bet_not_found",
                "size_cancelled": 0.0,
            }

        requested = float(pos.size or 0.0)
        matched = float(pos.matched_size or 0.0)
        size_cancelled = max(0.0, requested - matched)

        updated = self.state.cancel_position(str(bet_id))
        if not updated:
            return {
                "ok": False,
                "reason": "cancel_failed",
                "size_cancelled": 0.0,
            }

        return {
            "ok": True,
            "bet_id": str(bet_id),
            "size_cancelled": float(size_cancelled),
            "status": updated.status,
        }

    # =========================================================
    # SETTLE
    # =========================================================
    def settle_position(self, bet_id: str, pnl: float) -> Dict[str, Any]:
        pos = self.state.settle_position(str(bet_id), float(pnl or 0.0))
        if not pos:
            return {
                "ok": False,
                "reason": "bet_not_found",
            }

        return {
            "ok": True,
            "bet_id": str(bet_id),
            "pnl": float(pos.realized_pnl),
            "status": str(pos.status),
            "settled_at": datetime.utcnow().isoformat(),
        }