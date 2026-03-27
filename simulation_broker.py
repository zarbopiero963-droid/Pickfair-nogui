from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class SimOrder:
    bet_id: str
    market_id: str
    selection_id: int
    side: str
    price: float
    size: float
    size_matched: float = 0.0
    avg_price_matched: float = 0.0
    status: str = "EXECUTABLE"
    placed_at: float = field(default_factory=time.time)
    matched_at: Optional[float] = None
    customer_ref: str = ""
    notes: Dict[str, Any] = field(default_factory=dict)

    @property
    def remaining_size(self) -> float:
        return max(0.0, self.size - self.size_matched)

    @property
    def is_complete(self) -> bool:
        return self.size_matched >= self.size and self.size > 0


class SimulationBroker:
    """
    Paper trading broker quasi-live.

    Non manda ordini reali a Betfair.
    Simula:
    - place_bet / place_orders
    - account funds
    - fill completo o parziale
    - ordine pendente se il prezzo non è matchabile

    Per usare bene questo broker, devi alimentarlo con snapshot ladder live tramite:
        update_market_book(market_id, market_book)

    market_book atteso:
    {
        "marketId": "1.234",
        "runners": [
            {
                "selectionId": 123,
                "ex": {
                    "availableToBack": [{"price": 2.0, "size": 120.0}, ...],
                    "availableToLay":  [{"price": 2.02, "size": 80.0}, ...],
                }
            }
        ]
    }
    """

    def __init__(
        self,
        commission_pct: float = 4.5,
        starting_balance: float = 1000.0,
        simulated_latency_ms: int = 120,
        partial_fill_enabled: bool = True,
    ):
        self.commission_pct = float(commission_pct or 0.0)
        self.starting_balance = float(starting_balance or 0.0)
        self.simulated_latency_ms = int(simulated_latency_ms or 0)
        self.partial_fill_enabled = bool(partial_fill_enabled)

        self._lock = threading.RLock()
        self._connected = True
        self._session_token = f"SIM-{uuid.uuid4().hex}"

        self._available_balance = float(starting_balance or 0.0)
        self._exposure = 0.0

        self._market_books: Dict[str, Dict[str, Any]] = {}
        self._orders: Dict[str, SimOrder] = {}
        self._orders_by_market: Dict[str, List[str]] = {}

    # =========================================================
    # SESSION / ACCOUNT
    # =========================================================
    def login(self, password: str = "") -> Dict[str, Any]:
        with self._lock:
            self._connected = True
            return {
                "session_token": self._session_token,
                "expiry": "",
                "simulated": True,
            }

    def logout(self) -> None:
        with self._lock:
            self._connected = False

    def get_account_funds(self) -> Dict[str, float]:
        with self._lock:
            total = self._available_balance + self._exposure
            return {
                "available": round(self._available_balance, 2),
                "exposure": round(self._exposure, 2),
                "total": round(total, 2),
            }

    # =========================================================
    # MARKET DATA INPUT
    # =========================================================
    def update_market_book(self, market_id: str, market_book: Dict[str, Any]) -> None:
        with self._lock:
            self._market_books[str(market_id)] = dict(market_book or {})
            self._reprocess_market_orders(str(market_id))

    def get_market_book(self, market_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            book = self._market_books.get(str(market_id))
            return dict(book) if book else None

    # =========================================================
    # PUBLIC ORDER API
    # =========================================================
    def place_bet(
        self,
        *,
        market_id: str,
        selection_id: int,
        side: str,
        price: float,
        size: float,
        customer_ref: str = "",
    ) -> Dict[str, Any]:
        response = self.place_orders(
            market_id=market_id,
            instructions=[
                {
                    "selection_id": int(selection_id),
                    "side": str(side).upper(),
                    "price": float(price),
                    "size": float(size),
                    "customer_ref": customer_ref,
                }
            ],
            customer_ref=customer_ref,
        )
        return response

    def place_orders(
        self,
        *,
        market_id: str,
        instructions: List[Dict[str, Any]],
        customer_ref: str = "",
    ) -> Dict[str, Any]:
        if self.simulated_latency_ms > 0:
            time.sleep(self.simulated_latency_ms / 1000.0)

        with self._lock:
            market_id = str(market_id)
            reports = []

            for instr in instructions or []:
                selection_id = int(instr["selection_id"])
                side = str(instr["side"]).upper()
                price = float(instr["price"])
                size = float(instr["size"])
                ref = str(instr.get("customer_ref") or customer_ref or uuid.uuid4().hex)

                if size <= 0:
                    reports.append(
                        {
                            "status": "FAILURE",
                            "errorCode": "INVALID_SIZE",
                            "betId": "",
                            "sizeMatched": 0.0,
                            "customerRef": ref,
                        }
                    )
                    continue

                if price <= 1.0:
                    reports.append(
                        {
                            "status": "FAILURE",
                            "errorCode": "INVALID_PRICE",
                            "betId": "",
                            "sizeMatched": 0.0,
                            "customerRef": ref,
                        }
                    )
                    continue

                reserve_needed = self._reserve_needed(side=side, price=price, size=size)
                if reserve_needed > self._available_balance:
                    reports.append(
                        {
                            "status": "FAILURE",
                            "errorCode": "INSUFFICIENT_FUNDS",
                            "betId": "",
                            "sizeMatched": 0.0,
                            "customerRef": ref,
                        }
                    )
                    continue

                bet_id = f"SIMBET-{uuid.uuid4().hex[:16]}"
                order = SimOrder(
                    bet_id=bet_id,
                    market_id=market_id,
                    selection_id=selection_id,
                    side=side,
                    price=price,
                    size=size,
                    customer_ref=ref,
                )

                self._available_balance -= reserve_needed
                self._exposure += reserve_needed

                self._orders[bet_id] = order
                self._orders_by_market.setdefault(market_id, []).append(bet_id)

                self._attempt_match(order)

                reports.append(
                    {
                        "status": "SUCCESS",
                        "betId": bet_id,
                        "sizeMatched": round(order.size_matched, 2),
                        "averagePriceMatched": round(order.avg_price_matched, 2),
                        "customerRef": ref,
                    }
                )

            return {
                "status": "SUCCESS",
                "marketId": market_id,
                "instructionReports": reports,
                "simulated": True,
            }

    def list_current_orders(self, market_id: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            if market_id:
                ids = self._orders_by_market.get(str(market_id), [])
                orders = [self._serialize_order(self._orders[oid]) for oid in ids if oid in self._orders]
            else:
                orders = [self._serialize_order(o) for o in self._orders.values()]

            return {
                "currentOrders": orders,
                "simulated": True,
            }

    def cancel_orders(
        self,
        *,
        market_id: Optional[str] = None,
        bet_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        with self._lock:
            target_ids: List[str] = []

            if bet_ids:
                target_ids = [str(x) for x in bet_ids if str(x) in self._orders]
            elif market_id:
                target_ids = list(self._orders_by_market.get(str(market_id), []))
            else:
                target_ids = list(self._orders.keys())

            reports = []
            for bet_id in target_ids:
                order = self._orders.get(bet_id)
                if not order:
                    continue

                if order.status in {"EXECUTION_COMPLETE", "CANCELLED"}:
                    reports.append(
                        {
                            "status": "FAILURE",
                            "betId": bet_id,
                            "errorCode": "BET_TAKEN_OR_LAPSED",
                        }
                    )
                    continue

                remaining = order.remaining_size
                refund = self._reserve_needed(
                    side=order.side,
                    price=order.price,
                    size=remaining,
                )
                self._available_balance += refund
                self._exposure = max(0.0, self._exposure - refund)
                order.status = "CANCELLED"

                reports.append(
                    {
                        "status": "SUCCESS",
                        "betId": bet_id,
                        "sizeCancelled": round(remaining, 2),
                    }
                )

            return {
                "status": "SUCCESS",
                "instructionReports": reports,
                "simulated": True,
            }

    # =========================================================
    # INTERNAL MATCHING
    # =========================================================
    def _reprocess_market_orders(self, market_id: str) -> None:
        ids = list(self._orders_by_market.get(market_id, []))
        for bet_id in ids:
            order = self._orders.get(bet_id)
            if not order:
                continue
            if order.status in {"EXECUTION_COMPLETE", "CANCELLED"}:
                continue
            self._attempt_match(order)

    def _attempt_match(self, order: SimOrder) -> None:
        book = self._market_books.get(order.market_id)
        if not book:
            order.status = "EXECUTABLE"
            return

        runner = self._find_runner(book, order.selection_id)
        if not runner:
            order.status = "EXECUTABLE"
            return

        ex = runner.get("ex", {}) or {}
        back_ladder = ex.get("availableToBack", []) or []
        lay_ladder = ex.get("availableToLay", []) or []

        if order.side == "BACK":
            # Per un BACK, il match avviene contro il lato lay disponibile
            matched_size, matched_price = self._consume_cross_ladder(
                wanted_price=order.price,
                wanted_size=order.remaining_size,
                cross_ladder=lay_ladder,
                is_back_order=True,
            )
        else:
            # Per un LAY, il match avviene contro il lato back disponibile
            matched_size, matched_price = self._consume_cross_ladder(
                wanted_price=order.price,
                wanted_size=order.remaining_size,
                cross_ladder=back_ladder,
                is_back_order=False,
            )

        if matched_size <= 0:
            order.status = "EXECUTABLE"
            return

        prev_matched = order.size_matched
        new_total_matched = prev_matched + matched_size

        if new_total_matched > 0:
            order.avg_price_matched = (
                ((order.avg_price_matched * prev_matched) + (matched_price * matched_size))
                / new_total_matched
            )

        order.size_matched = new_total_matched

        if order.is_complete:
            order.status = "EXECUTION_COMPLETE"
            order.matched_at = time.time()
            self._finalize_completed_order(order)
        else:
            order.status = "EXECUTABLE"

    def _consume_cross_ladder(
        self,
        *,
        wanted_price: float,
        wanted_size: float,
        cross_ladder: List[Dict[str, Any]],
        is_back_order: bool,
    ) -> tuple[float, float]:
        """
        BACK order:
            matcha se lay_price <= wanted_price
        LAY order:
            matcha se back_price >= wanted_price
        """
        if wanted_size <= 0:
            return 0.0, 0.0

        for level in cross_ladder:
            try:
                level_price = float(level.get("price", 0.0) or 0.0)
                level_size = float(level.get("size", 0.0) or 0.0)
            except Exception:
                continue

            if level_price <= 0 or level_size <= 0:
                continue

            if is_back_order:
                if level_price > wanted_price:
                    continue
            else:
                if level_price < wanted_price:
                    continue

            matched = min(wanted_size, level_size)

            if not self.partial_fill_enabled and matched < wanted_size:
                return 0.0, 0.0

            return matched, level_price

        return 0.0, 0.0

    def _finalize_completed_order(self, order: SimOrder) -> None:
        remaining_reserved = self._reserve_needed(
            side=order.side,
            price=order.price,
            size=order.remaining_size,
        )
        if remaining_reserved > 0:
            self._available_balance += remaining_reserved
            self._exposure = max(0.0, self._exposure - remaining_reserved)

    # =========================================================
    # HELPERS
    # =========================================================
    def _reserve_needed(self, *, side: str, price: float, size: float) -> float:
        if str(side).upper() == "LAY":
            return max(0.0, size * max(0.0, price - 1.0))
        return max(0.0, size)

    def _find_runner(self, market_book: Dict[str, Any], selection_id: int) -> Optional[Dict[str, Any]]:
        for runner in market_book.get("runners", []) or []:
            try:
                if int(runner.get("selectionId")) == int(selection_id):
                    return runner
            except Exception:
                continue
        return None

    def _serialize_order(self, order: SimOrder) -> Dict[str, Any]:
        return {
            "betId": order.bet_id,
            "marketId": order.market_id,
            "selectionId": order.selection_id,
            "side": order.side,
            "priceSize": {
                "price": round(order.price, 2),
                "size": round(order.size, 2),
            },
            "sizeMatched": round(order.size_matched, 2),
            "averagePriceMatched": round(order.avg_price_matched, 2),
            "status": order.status,
            "placedDate": datetime.fromtimestamp(order.placed_at).isoformat(),
            "matchedDate": (
                datetime.fromtimestamp(order.matched_at).isoformat()
                if order.matched_at is not None
                else None
            ),
            "customerRef": order.customer_ref,
            "simulated": True,
        }