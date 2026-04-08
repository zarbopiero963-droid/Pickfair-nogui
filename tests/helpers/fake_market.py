from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Literal, Tuple


Side = Literal["BACK", "LAY"]
OrderStatus = Literal["RESTING", "PARTIALLY_MATCHED", "MATCHED"]


@dataclass
class LadderLevel:
    price: float
    available_size: float


@dataclass
class RestingOrder:
    order_id: str
    selection_id: int
    side: Side
    requested_price: float
    requested_size: float
    queue_ahead_remaining: float
    allow_worse_fill: bool = False
    matched_size: float = 0.0
    status: OrderStatus = "RESTING"
    fills: List[Tuple[float, float]] = field(default_factory=list)

    @property
    def remaining_size(self) -> float:
        return max(0.0, self.requested_size - self.matched_size)

    @property
    def average_fill_price(self) -> float:
        if self.matched_size <= 0:
            return 0.0
        gross = sum(price * size for price, size in self.fills)
        return round(gross / self.matched_size, 6)


class FakeMarket:
    """Deterministic fake market with explicit, manual execution progression."""

    def __init__(self) -> None:
        self._ladders: Dict[int, Dict[Side, List[LadderLevel]]] = {}
        self._orders: Dict[str, RestingOrder] = {}
        self._selection_orders: Dict[int, List[str]] = {}
        self._next_order_id = 1

    def seed_back_ladder(self, selection_id: int, levels: Iterable[Tuple[float, float]]) -> None:
        self._set_ladder(selection_id, "BACK", levels)

    def seed_lay_ladder(self, selection_id: int, levels: Iterable[Tuple[float, float]]) -> None:
        self._set_ladder(selection_id, "LAY", levels)

    def seed_selection(
        self,
        selection_id: int,
        back_levels: Iterable[Tuple[float, float]],
        lay_levels: Iterable[Tuple[float, float]],
    ) -> None:
        self.seed_back_ladder(selection_id, back_levels)
        self.seed_lay_ladder(selection_id, lay_levels)

    def add_liquidity(self, selection_id: int, side: Side, price: float, size: float) -> None:
        if size <= 0:
            raise ValueError("size must be positive")
        ladder = self._get_ladder(selection_id, side)
        for level in ladder:
            if level.price == float(price):
                level.available_size += float(size)
                return
        ladder.append(LadderLevel(price=float(price), available_size=float(size)))
        self._sort_ladder(ladder, side)

    def remove_liquidity(self, selection_id: int, side: Side, price: float, size: float) -> None:
        if size <= 0:
            raise ValueError("size must be positive")
        ladder = self._get_ladder(selection_id, side)
        for level in ladder:
            if level.price == float(price):
                level.available_size = max(0.0, level.available_size - float(size))
                return
        raise RuntimeError(f"price level not found: {selection_id} {side} {price}")

    def move_market(
        self,
        selection_id: int,
        *,
        back_levels: Iterable[Tuple[float, float]],
        lay_levels: Iterable[Tuple[float, float]],
    ) -> None:
        self.seed_selection(selection_id, back_levels, lay_levels)

    def place_resting_order(
        self,
        *,
        selection_id: int,
        side: Side,
        price: float,
        size: float,
        allow_worse_fill: bool = False,
    ) -> str:
        if size <= 0:
            raise ValueError("size must be positive")
        if price <= 0:
            raise ValueError("price must be positive")

        oid = f"FM-{self._next_order_id}"
        self._next_order_id += 1

        queue_ahead = self._visible_matchable_liquidity(selection_id, side, float(price), allow_worse_fill)
        order = RestingOrder(
            order_id=oid,
            selection_id=selection_id,
            side=side,
            requested_price=float(price),
            requested_size=float(size),
            queue_ahead_remaining=queue_ahead,
            allow_worse_fill=allow_worse_fill,
        )
        self._orders[oid] = order
        self._selection_orders.setdefault(selection_id, []).append(oid)
        return oid

    def advance_tick(self, selection_id: int, *, opposing_traded_size: float) -> None:
        if opposing_traded_size < 0:
            raise ValueError("opposing_traded_size cannot be negative")

        remaining_volume = float(opposing_traded_size)
        for oid in self._selection_orders.get(selection_id, []):
            order = self._orders[oid]
            if order.status == "MATCHED" or remaining_volume <= 0:
                continue

            drained = min(order.queue_ahead_remaining, remaining_volume)
            order.queue_ahead_remaining -= drained
            remaining_volume -= drained
            if remaining_volume <= 0:
                continue

            matched_now = self._consume_ladder(order, remaining_volume)
            remaining_volume -= matched_now

    def snapshot_order(self, order_id: str) -> Dict[str, float | str | List[Tuple[float, float]]]:
        order = self._must_get_order(order_id)
        return {
            "order_id": order.order_id,
            "selection_id": order.selection_id,
            "side": order.side,
            "requested_price": order.requested_price,
            "requested_size": order.requested_size,
            "matched_size": round(order.matched_size, 6),
            "remaining_size": round(order.remaining_size, 6),
            "average_fill_price": order.average_fill_price,
            "queue_ahead_remaining": round(order.queue_ahead_remaining, 6),
            "status": order.status,
            "fills": list(order.fills),
        }

    def _consume_ladder(self, order: RestingOrder, max_match_size: float) -> float:
        opposing_side: Side = "LAY" if order.side == "BACK" else "BACK"
        ladder = self._get_ladder(order.selection_id, opposing_side)

        matched_total = 0.0
        for level in ladder:
            if order.remaining_size <= 0 or max_match_size <= 0:
                break
            if level.available_size <= 0:
                continue
            if not self._is_price_matchable(order, level.price):
                continue

            take = min(level.available_size, order.remaining_size, max_match_size)
            if take <= 0:
                continue

            level.available_size -= take
            order.matched_size += take
            order.fills.append((level.price, take))
            matched_total += take
            max_match_size -= take

        if order.matched_size <= 0:
            order.status = "RESTING"
        elif order.remaining_size <= 0:
            order.status = "MATCHED"
        else:
            order.status = "PARTIALLY_MATCHED"
        return matched_total

    def _visible_matchable_liquidity(
        self,
        selection_id: int,
        side: Side,
        requested_price: float,
        allow_worse_fill: bool,
    ) -> float:
        opposing_side: Side = "LAY" if side == "BACK" else "BACK"
        ladder = self._get_ladder(selection_id, opposing_side)
        total = 0.0
        for level in ladder:
            if level.available_size <= 0:
                continue
            if self._is_price_matchable_by_request(side, requested_price, level.price, allow_worse_fill):
                total += level.available_size
        return total

    def _is_price_matchable(self, order: RestingOrder, opposing_price: float) -> bool:
        return self._is_price_matchable_by_request(
            order.side,
            order.requested_price,
            opposing_price,
            order.allow_worse_fill,
        )

    @staticmethod
    def _is_price_matchable_by_request(
        side: Side,
        requested_price: float,
        opposing_price: float,
        allow_worse_fill: bool,
    ) -> bool:
        if side == "BACK":
            return opposing_price <= requested_price if not allow_worse_fill else True
        if side == "LAY":
            return opposing_price >= requested_price if not allow_worse_fill else True
        raise RuntimeError(f"unsupported side: {side}")

    def _get_ladder(self, selection_id: int, side: Side) -> List[LadderLevel]:
        if selection_id not in self._ladders:
            self._ladders[selection_id] = {"BACK": [], "LAY": []}
        return self._ladders[selection_id][side]

    def _set_ladder(self, selection_id: int, side: Side, levels: Iterable[Tuple[float, float]]) -> None:
        clean_levels = []
        for price, size in levels:
            if price <= 0:
                raise ValueError("price must be positive")
            if size < 0:
                raise ValueError("size cannot be negative")
            clean_levels.append(LadderLevel(price=float(price), available_size=float(size)))
        self._sort_ladder(clean_levels, side)
        self._get_ladder(selection_id, side).clear()
        self._get_ladder(selection_id, side).extend(clean_levels)

    @staticmethod
    def _sort_ladder(ladder: List[LadderLevel], side: Side) -> None:
        ladder.sort(key=lambda lvl: lvl.price, reverse=(side == "BACK"))

    def _must_get_order(self, order_id: str) -> RestingOrder:
        if order_id not in self._orders:
            raise RuntimeError(f"unknown order_id: {order_id}")
        return self._orders[order_id]
