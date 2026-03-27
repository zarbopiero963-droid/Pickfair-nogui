from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from threading import RLock
from typing import Any, Dict, Optional

from core.simulation_order_book import SimulationOrderBook
from core.simulation_state import SimulationPosition, SimulationState


@dataclass
class SimulationMatchResult:
    bet_id: str
    market_id: str
    selection_id: int
    side: str
    requested_price: float
    requested_size: float
    matched_size: float
    average_matched_price: float
    remaining_size: float
    status: str
    fully_matched: bool
    simulated: bool = True
    matched_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SimulationMatchingEngine:
    """
    Matching engine del paper trading.

    Responsabilità:
    - ricevere ordini simulati
    - provarne il matching contro il SimulationOrderBook
    - aggiornare SimulationState
    - restituire un risultato coerente con il flusso OMS

    NON gestisce:
    - EventBus
    - DB
    - GUI
    - streaming esterno
    """

    def __init__(
        self,
        order_book: SimulationOrderBook,
        state: SimulationState,
        *,
        partial_fill_enabled: bool = True,
        consume_liquidity: bool = True,
    ):
        self.order_book = order_book
        self.state = state
        self.partial_fill_enabled = bool(partial_fill_enabled)
        self.consume_liquidity = bool(consume_liquidity)
        self._lock = RLock()

    # =========================================================
    # PUBLIC API
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
        with self._lock:
            market_id = str(market_id)
            selection_id = int(selection_id)
            side = str(side or "BACK").upper()
            price = float(price or 0.0)
            size = float(size or 0.0)

            if size <= 0:
                return self._build_result(
                    bet_id=bet_id,
                    market_id=market_id,
                    selection_id=selection_id,
                    side=side,
                    requested_price=price,
                    requested_size=size,
                    matched_size=0.0,
                    average_matched_price=0.0,
                    status="FAILURE",
                )

            if price <= 1.0:
                return self._build_result(
                    bet_id=bet_id,
                    market_id=market_id,
                    selection_id=selection_id,
                    side=side,
                    requested_price=price,
                    requested_size=size,
                    matched_size=0.0,
                    average_matched_price=0.0,
                    status="FAILURE",
                )

            reserve_amount = self.state.reserve_for_order(
                side=side,
                price=price,
                size=size,
            )

            position = SimulationPosition(
                bet_id=str(bet_id),
                market_id=market_id,
                selection_id=selection_id,
                side=side,
                price=price,
                size=size,
                matched_size=0.0,
                avg_price_matched=0.0,
                status="EXECUTABLE",
                event_key=str(event_key or ""),
                table_id=table_id,
                batch_id=str(batch_id or ""),
                event_name=str(event_name or ""),
                market_name=str(market_name or ""),
                runner_name=str(runner_name or ""),
            )
            position.notes["reserved_amount"] = float(reserve_amount)
            position.notes["customer_ref"] = str(customer_ref or "")
            self.state.add_position(position)

            matched_size, avg_price = self._match_position(position)

            if matched_size <= 0:
                self.state.update_position(
                    bet_id=position.bet_id,
                    matched_size=0.0,
                    avg_price_matched=0.0,
                    status="EXECUTABLE",
                )
                return self._build_result(
                    bet_id=position.bet_id,
                    market_id=market_id,
                    selection_id=selection_id,
                    side=side,
                    requested_price=price,
                    requested_size=size,
                    matched_size=0.0,
                    average_matched_price=0.0,
                    status="EXECUTABLE",
                )

            new_status = "EXECUTION_COMPLETE" if matched_size >= size else "EXECUTABLE"
            self.state.update_position(
                bet_id=position.bet_id,
                matched_size=matched_size,
                avg_price_matched=avg_price,
                status=new_status,
            )

            if matched_size < size:
                unmatched_size = size - matched_size
                self._release_unmatched_reserve(position, unmatched_size)

            return self._build_result(
                bet_id=position.bet_id,
                market_id=market_id,
                selection_id=selection_id,
                side=side,
                requested_price=price,
                requested_size=size,
                matched_size=matched_size,
                average_matched_price=avg_price,
                status=new_status,
            )

    def reprocess_open_orders(self, market_id: str) -> Dict[str, Any]:
        """
        Riprova il matching di tutte le posizioni ancora aperte su un mercato.
        Utile dopo update_market_book().
        """
        results = []

        for position in self.state.list_open_positions():
            if str(position.market_id) != str(market_id):
                continue

            if position.remaining_size() <= 0:
                continue

            matched_size, avg_price = self._match_position(position)
            if matched_size <= position.matched_size:
                continue

            old_matched = float(position.matched_size or 0.0)
            delta = matched_size - old_matched
            if delta <= 0:
                continue

            new_status = "EXECUTION_COMPLETE" if matched_size >= position.size else "EXECUTABLE"

            self.state.update_position(
                bet_id=position.bet_id,
                matched_size=matched_size,
                avg_price_matched=avg_price,
                status=new_status,
            )

            if new_status == "EXECUTION_COMPLETE":
                unmatched_size = max(0.0, position.size - matched_size)
                if unmatched_size > 0:
                    self._release_unmatched_reserve(position, unmatched_size)

            results.append(
                self._build_result(
                    bet_id=position.bet_id,
                    market_id=position.market_id,
                    selection_id=position.selection_id,
                    side=position.side,
                    requested_price=position.price,
                    requested_size=position.size,
                    matched_size=matched_size,
                    average_matched_price=avg_price,
                    status=new_status,
                ).to_dict()
            )

        return {
            "market_id": str(market_id),
            "updated": len(results),
            "results": results,
            "simulated": True,
        }

    def cancel_order(self, bet_id: str) -> Dict[str, Any]:
        with self._lock:
            position = self.state.get_position(bet_id)
            if not position:
                return {
                    "ok": False,
                    "status": "NOT_FOUND",
                    "bet_id": str(bet_id),
                    "simulated": True,
                }

            if position.status in {"CANCELLED", "SETTLED", "CLOSED"}:
                return {
                    "ok": False,
                    "status": position.status,
                    "bet_id": position.bet_id,
                    "simulated": True,
                }

            remaining = position.remaining_size()
            if remaining > 0:
                self._release_unmatched_reserve(position, remaining)

            self.state.update_position(
                bet_id=position.bet_id,
                status="CANCELLED",
            )

            return {
                "ok": True,
                "status": "CANCELLED",
                "bet_id": position.bet_id,
                "size_cancelled": float(remaining),
                "simulated": True,
            }

    def settle_position(self, bet_id: str, pnl: float) -> Dict[str, Any]:
        """
        Chiude una posizione simulata applicando il pnl realizzato.
        """
        with self._lock:
            position = self.state.get_position(bet_id)
            if not position:
                return {
                    "ok": False,
                    "status": "NOT_FOUND",
                    "bet_id": str(bet_id),
                    "simulated": True,
                }

            if position.status in {"SETTLED", "CLOSED"}:
                return {
                    "ok": False,
                    "status": position.status,
                    "bet_id": position.bet_id,
                    "simulated": True,
                }

            reserved_amount = float(position.notes.get("reserved_amount", 0.0) or 0.0)

            self.state.bankroll_available += reserved_amount
            self.state.exposure_open = max(0.0, self.state.exposure_open - reserved_amount)
            self.state.apply_realized_pnl(float(pnl or 0.0))
            self.state.update_position(
                bet_id=position.bet_id,
                status="SETTLED",
            )

            return {
                "ok": True,
                "status": "SETTLED",
                "bet_id": position.bet_id,
                "pnl": float(pnl or 0.0),
                "simulated": True,
            }

    # =========================================================
    # INTERNAL
    # =========================================================
    def _match_position(self, position: SimulationPosition) -> tuple[float, float]:
        remaining = position.remaining_size()
        if remaining <= 0:
            return float(position.matched_size or 0.0), float(position.avg_price_matched or 0.0)

        if self.consume_liquidity:
            delta_matched, delta_avg = self.order_book.apply_virtual_fill(
                market_id=position.market_id,
                selection_id=position.selection_id,
                side=position.side,
                price=position.price,
                size=remaining,
                partial_fill_enabled=self.partial_fill_enabled,
            )
        else:
            delta_matched, delta_avg = self.order_book.preview_match(
                market_id=position.market_id,
                selection_id=position.selection_id,
                side=position.side,
                price=position.price,
                size=remaining,
                partial_fill_enabled=self.partial_fill_enabled,
            )

        if delta_matched <= 0:
            return float(position.matched_size or 0.0), float(position.avg_price_matched or 0.0)

        old_matched = float(position.matched_size or 0.0)
        old_avg = float(position.avg_price_matched or 0.0)
        new_total = old_matched + delta_matched

        if new_total <= 0:
            return 0.0, 0.0

        new_avg = ((old_avg * old_matched) + (delta_avg * delta_matched)) / new_total
        return new_total, new_avg

    def _release_unmatched_reserve(self, position: SimulationPosition, unmatched_size: float) -> None:
        if unmatched_size <= 0:
            return

        refund = self.state.release_reserved(
            side=position.side,
            price=position.price,
            size=unmatched_size,
        )

        reserved_before = float(position.notes.get("reserved_amount", 0.0) or 0.0)
        reserved_after = max(0.0, reserved_before - refund)
        position.notes["reserved_amount"] = reserved_after

    def _build_result(
        self,
        *,
        bet_id: str,
        market_id: str,
        selection_id: int,
        side: str,
        requested_price: float,
        requested_size: float,
        matched_size: float,
        average_matched_price: float,
        status: str,
    ) -> SimulationMatchResult:
        matched_size = float(matched_size or 0.0)
        requested_size = float(requested_size or 0.0)

        return SimulationMatchResult(
            bet_id=str(bet_id),
            market_id=str(market_id),
            selection_id=int(selection_id),
            side=str(side),
            requested_price=float(requested_price or 0.0),
            requested_size=requested_size,
            matched_size=matched_size,
            average_matched_price=float(average_matched_price or 0.0),
            remaining_size=max(0.0, requested_size - matched_size),
            status=str(status),
            fully_matched=matched_size >= requested_size and requested_size > 0.0,
            simulated=True,
            matched_at=datetime.utcnow().isoformat(),
        )