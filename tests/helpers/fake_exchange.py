from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional


OrderState = Literal["EXECUTABLE", "PARTIALLY_MATCHED", "MATCHED", "CANCELLED"]
DuplicateMode = Literal["reject", "return_existing", "single_exposure"]


@dataclass
class FakeOrder:
    order_id: str
    customer_ref: str
    market_id: str
    selection_id: int
    side: str
    price: float
    size: float
    status: OrderState = "EXECUTABLE"
    matched_size: float = 0.0
    remaining_size: float = 0.0

    def __post_init__(self) -> None:
        if self.remaining_size == 0.0:
            self.remaining_size = float(self.size)

    def to_exchange_row(self) -> Dict[str, Any]:
        return {
            "bet_id": self.order_id,
            "order_id": self.order_id,
            "customer_ref": self.customer_ref,
            "market_id": self.market_id,
            "selection_id": self.selection_id,
            "side": self.side,
            "price": self.price,
            "size": self.size,
            "matched_size": self.matched_size,
            "remaining_size": self.remaining_size,
            "status": self.status,
        }


class FakeExchange:
    """Deterministic in-memory exchange model for trading and reconcile tests."""

    def __init__(self, *, duplicate_mode: DuplicateMode = "reject") -> None:
        self.duplicate_mode = duplicate_mode
        self._next_id = 1
        self._orders: Dict[str, FakeOrder] = {}
        self._customer_ref_to_order: Dict[str, str] = {}
        self._liquidity: Dict[tuple[str, int, str], float] = {}
        self._forced_next_submit: Optional[str] = None
        self._forced_partial_fraction: Optional[float] = None

    def force_timeout_on_next_submit(self) -> None:
        self._forced_next_submit = "timeout"

    def force_reject_on_next_submit(self) -> None:
        self._forced_next_submit = "reject"

    def force_partial_on_next_submit(self, *, matched_fraction: float = 0.5) -> None:
        if not 0.0 < matched_fraction < 1.0:
            raise ValueError("matched_fraction must be between 0 and 1")
        self._forced_partial_fraction = matched_fraction

    def seed_liquidity(self, *, market_id: str, selection_id: int, side: str, size: float) -> None:
        key = (market_id, selection_id, side)
        self._liquidity[key] = float(size)

    def place_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        customer_ref = str(payload.get("customer_ref") or "")
        if not customer_ref:
            raise ValueError("payload.customer_ref is required")

        existing_id = self._customer_ref_to_order.get(customer_ref)
        if existing_id:
            if self.duplicate_mode == "reject":
                raise RuntimeError(f"duplicate customer_ref rejected: {customer_ref}")
            if self.duplicate_mode in {"return_existing", "single_exposure"}:
                return self._orders[existing_id].to_exchange_row()
            raise RuntimeError(f"unsupported duplicate mode: {self.duplicate_mode}")

        order = self._create_order(payload)
        self._orders[order.order_id] = order
        self._customer_ref_to_order[customer_ref] = order.order_id

        forced = self._consume_forced_submit()
        if forced == "reject":
            del self._orders[order.order_id]
            del self._customer_ref_to_order[customer_ref]
            raise RuntimeError("forced reject on submit")

        self._apply_liquidity(order)
        self._apply_forced_partial(order)

        if forced == "timeout":
            raise TimeoutError("forced timeout after remote accept")

        return order.to_exchange_row()

    def get_current_orders(
        self,
        *,
        customer_ref: Optional[str] = None,
        market_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        rows = []
        for order in self._orders.values():
            if customer_ref and order.customer_ref != customer_ref:
                continue
            if market_id and order.market_id != market_id:
                continue
            rows.append(order.to_exchange_row())
        return sorted(rows, key=lambda x: str(x["order_id"]))

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        order = self._must_get_order(order_id)
        if order.status in {"MATCHED", "CANCELLED"}:
            return order.to_exchange_row()
        order.status = "CANCELLED"
        order.remaining_size = 0.0
        return order.to_exchange_row()

    def replace_order(self, order_id: str, *, new_price: float) -> Dict[str, Any]:
        order = self._must_get_order(order_id)
        if order.status in {"MATCHED", "CANCELLED"}:
            raise RuntimeError(f"cannot replace terminal order: {order_id}")
        order.price = float(new_price)
        return order.to_exchange_row()

    def advance_fill(self, order_id: str, *, new_status: OrderState, matched_size: Optional[float] = None) -> Dict[str, Any]:
        order = self._must_get_order(order_id)
        if matched_size is None:
            matched_size = order.matched_size
        matched = max(0.0, min(float(matched_size), float(order.size)))
        remaining = max(0.0, float(order.size) - matched)

        if new_status == "MATCHED":
            matched = float(order.size)
            remaining = 0.0
        elif new_status == "CANCELLED":
            remaining = 0.0
        elif new_status == "EXECUTABLE":
            matched = 0.0
            remaining = float(order.size)
        elif new_status == "PARTIALLY_MATCHED" and matched in {0.0, float(order.size)}:
            raise RuntimeError("PARTIALLY_MATCHED requires matched_size strictly between 0 and size")

        order.status = new_status
        order.matched_size = matched
        order.remaining_size = remaining
        return order.to_exchange_row()

    def snapshot_orders(self) -> Dict[str, Dict[str, Any]]:
        return {oid: order.to_exchange_row() for oid, order in sorted(self._orders.items())}

    def _create_order(self, payload: Dict[str, Any]) -> FakeOrder:
        oid = f"BET-{self._next_id}"
        self._next_id += 1
        return FakeOrder(
            order_id=oid,
            customer_ref=str(payload["customer_ref"]),
            market_id=str(payload["market_id"]),
            selection_id=int(payload["selection_id"]),
            side=str(payload["side"]),
            price=float(payload["price"]),
            size=float(payload["size"]),
        )

    def _apply_liquidity(self, order: FakeOrder) -> None:
        key = (order.market_id, order.selection_id, self._opposite_side(order.side))
        available = float(self._liquidity.get(key, 0.0))
        if available <= 0.0:
            return

        fill = min(available, order.remaining_size)
        self._liquidity[key] = max(0.0, available - fill)
        order.matched_size += fill
        order.remaining_size = max(0.0, order.size - order.matched_size)
        if order.remaining_size == 0.0:
            order.status = "MATCHED"
        elif order.matched_size > 0.0:
            order.status = "PARTIALLY_MATCHED"

    def _apply_forced_partial(self, order: FakeOrder) -> None:
        if self._forced_partial_fraction is None:
            return
        matched = round(order.size * self._forced_partial_fraction, 6)
        self._forced_partial_fraction = None
        if matched <= 0.0 or matched >= order.size:
            raise RuntimeError("forced partial created invalid matched size")
        order.status = "PARTIALLY_MATCHED"
        order.matched_size = matched
        order.remaining_size = max(0.0, order.size - matched)

    def _must_get_order(self, order_id: str) -> FakeOrder:
        if order_id not in self._orders:
            raise RuntimeError(f"unknown order_id: {order_id}")
        return self._orders[order_id]

    def _consume_forced_submit(self) -> Optional[str]:
        forced = self._forced_next_submit
        self._forced_next_submit = None
        return forced

    @staticmethod
    def _opposite_side(side: str) -> str:
        side_upper = side.upper()
        if side_upper == "BACK":
            return "LAY"
        if side_upper == "LAY":
            return "BACK"
        raise RuntimeError(f"unsupported side: {side}")
