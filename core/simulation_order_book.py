from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class LadderLevel:
    price: float
    size: float

    def to_dict(self) -> Dict[str, float]:
        return {
            "price": float(self.price),
            "size": float(self.size),
        }


@dataclass
class RunnerBook:
    selection_id: int
    available_to_back: List[LadderLevel] = field(default_factory=list)
    available_to_lay: List[LadderLevel] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "selectionId": int(self.selection_id),
            "ex": {
                "availableToBack": [lvl.to_dict() for lvl in self.available_to_back],
                "availableToLay": [lvl.to_dict() for lvl in self.available_to_lay],
            },
        }


class SimulationOrderBook:
    """
    Order book simulato alimentato da snapshot di mercato live.

    Responsabilità:
    - memorizzare il ladder per market_id / selection_id
    - offrire accesso ai livelli back/lay
    - determinare se un ordine è matchabile
    - calcolare fill completo o parziale contro il lato opposto del ladder

    NON gestisce:
    - bankroll
    - pnl
    - stato posizioni
    - persistenza
    """

    def __init__(self):
        self._lock = RLock()
        self._books: Dict[str, Dict[int, RunnerBook]] = {}

    # =========================================================
    # UPDATE / SNAPSHOT
    # =========================================================
    def update_market_book(self, market_id: str, market_book: Dict[str, Any]) -> None:
        market_id = str(market_id)
        parsed = self._parse_market_book(market_book)

        with self._lock:
            self._books[market_id] = parsed

    def get_market_book(self, market_id: str) -> Dict[str, Any]:
        market_id = str(market_id)

        with self._lock:
            runners = self._books.get(market_id, {})
            return {
                "marketId": market_id,
                "runners": [runner.to_dict() for runner in runners.values()],
            }

    def has_market(self, market_id: str) -> bool:
        with self._lock:
            return str(market_id) in self._books

    # =========================================================
    # ACCESS HELPERS
    # =========================================================
    def get_runner(self, market_id: str, selection_id: int) -> Optional[RunnerBook]:
        market_id = str(market_id)
        selection_id = int(selection_id)

        with self._lock:
            return self._books.get(market_id, {}).get(selection_id)

    def get_best_back(self, market_id: str, selection_id: int) -> Optional[Tuple[float, float]]:
        runner = self.get_runner(market_id, selection_id)
        if not runner or not runner.available_to_back:
            return None
        lvl = runner.available_to_back[0]
        return float(lvl.price), float(lvl.size)

    def get_best_lay(self, market_id: str, selection_id: int) -> Optional[Tuple[float, float]]:
        runner = self.get_runner(market_id, selection_id)
        if not runner or not runner.available_to_lay:
            return None
        lvl = runner.available_to_lay[0]
        return float(lvl.price), float(lvl.size)

    # =========================================================
    # MATCH EVALUATION
    # =========================================================
    def is_matchable(
        self,
        *,
        market_id: str,
        selection_id: int,
        side: str,
        price: float,
    ) -> bool:
        matched_size, _ = self.preview_match(
            market_id=market_id,
            selection_id=selection_id,
            side=side,
            price=price,
            size=0.01,
            partial_fill_enabled=True,
        )
        return matched_size > 0

    def preview_match(
        self,
        *,
        market_id: str,
        selection_id: int,
        side: str,
        price: float,
        size: float,
        partial_fill_enabled: bool = True,
    ) -> Tuple[float, float]:
        """
        Ritorna:
        - matched_size
        - average_matched_price

        BACK order:
            matcha contro available_to_lay se lay_price <= wanted_price

        LAY order:
            matcha contro available_to_back se back_price >= wanted_price
        """
        side = str(side or "BACK").upper()
        market_id = str(market_id)
        selection_id = int(selection_id)
        wanted_price = float(price or 0.0)
        wanted_size = float(size or 0.0)

        if wanted_price <= 0 or wanted_size <= 0:
            return 0.0, 0.0

        runner = self.get_runner(market_id, selection_id)
        if not runner:
            return 0.0, 0.0

        if side == "BACK":
            ladder = runner.available_to_lay
            return self._consume_cross_ladder(
                wanted_price=wanted_price,
                wanted_size=wanted_size,
                cross_ladder=ladder,
                is_back_order=True,
                partial_fill_enabled=partial_fill_enabled,
            )

        ladder = runner.available_to_back
        return self._consume_cross_ladder(
            wanted_price=wanted_price,
            wanted_size=wanted_size,
            cross_ladder=ladder,
            is_back_order=False,
            partial_fill_enabled=partial_fill_enabled,
        )

    def apply_virtual_fill(
        self,
        *,
        market_id: str,
        selection_id: int,
        side: str,
        price: float,
        size: float,
        partial_fill_enabled: bool = True,
    ) -> Tuple[float, float]:
        """
        Consuma virtualmente liquidità dal ladder simulato.
        Utile se vuoi che gli ordini simulati impattino l'order book locale.

        Ritorna:
        - matched_size
        - average_matched_price
        """
        side = str(side or "BACK").upper()
        market_id = str(market_id)
        selection_id = int(selection_id)
        wanted_price = float(price or 0.0)
        wanted_size = float(size or 0.0)

        if wanted_price <= 0 or wanted_size <= 0:
            return 0.0, 0.0

        with self._lock:
            runner = self._books.get(market_id, {}).get(selection_id)
            if not runner:
                return 0.0, 0.0

            if side == "BACK":
                ladder = runner.available_to_lay
                return self._consume_and_mutate_ladder(
                    wanted_price=wanted_price,
                    wanted_size=wanted_size,
                    cross_ladder=ladder,
                    is_back_order=True,
                    partial_fill_enabled=partial_fill_enabled,
                )

            ladder = runner.available_to_back
            return self._consume_and_mutate_ladder(
                wanted_price=wanted_price,
                wanted_size=wanted_size,
                cross_ladder=ladder,
                is_back_order=False,
                partial_fill_enabled=partial_fill_enabled,
            )

    # =========================================================
    # INTERNAL PARSING
    # =========================================================
    def _parse_market_book(self, market_book: Dict[str, Any]) -> Dict[int, RunnerBook]:
        result: Dict[int, RunnerBook] = {}

        for runner in (market_book or {}).get("runners", []) or []:
            try:
                selection_id = int(runner.get("selectionId"))
            except Exception:
                continue

            ex = runner.get("ex", {}) or {}
            back_ladder = self._parse_ladder(ex.get("availableToBack", []) or [])
            lay_ladder = self._parse_ladder(ex.get("availableToLay", []) or [])

            result[selection_id] = RunnerBook(
                selection_id=selection_id,
                available_to_back=back_ladder,
                available_to_lay=lay_ladder,
            )

        return result

    def _parse_ladder(self, ladder: List[Dict[str, Any]]) -> List[LadderLevel]:
        parsed: List[LadderLevel] = []
        for level in ladder or []:
            try:
                price = float(level.get("price", 0.0) or 0.0)
                size = float(level.get("size", 0.0) or 0.0)
            except Exception:
                continue

            if price <= 0 or size <= 0:
                continue

            parsed.append(LadderLevel(price=price, size=size))

        return parsed

    # =========================================================
    # INTERNAL MATCH LOGIC
    # =========================================================
    def _consume_cross_ladder(
        self,
        *,
        wanted_price: float,
        wanted_size: float,
        cross_ladder: List[LadderLevel],
        is_back_order: bool,
        partial_fill_enabled: bool,
    ) -> Tuple[float, float]:
        remaining = float(wanted_size)
        matched_total = 0.0
        weighted_sum = 0.0

        for level in cross_ladder:
            level_price = float(level.price)
            level_size = float(level.size)

            if level_price <= 0 or level_size <= 0:
                continue

            if is_back_order:
                if level_price > wanted_price:
                    continue
            else:
                if level_price < wanted_price:
                    continue

            take = min(remaining, level_size)

            if take < remaining and not partial_fill_enabled and matched_total == 0.0:
                return 0.0, 0.0

            matched_total += take
            weighted_sum += take * level_price
            remaining -= take

            if remaining <= 0:
                break

        if matched_total <= 0:
            return 0.0, 0.0

        avg_price = weighted_sum / matched_total
        return matched_total, avg_price

    def _consume_and_mutate_ladder(
        self,
        *,
        wanted_price: float,
        wanted_size: float,
        cross_ladder: List[LadderLevel],
        is_back_order: bool,
        partial_fill_enabled: bool,
    ) -> Tuple[float, float]:
        preview_matched, preview_avg = self._consume_cross_ladder(
            wanted_price=wanted_price,
            wanted_size=wanted_size,
            cross_ladder=cross_ladder,
            is_back_order=is_back_order,
            partial_fill_enabled=partial_fill_enabled,
        )

        if preview_matched <= 0:
            return 0.0, 0.0

        remaining = float(preview_matched)

        for level in cross_ladder:
            if remaining <= 0:
                break

            level_price = float(level.price)
            level_size = float(level.size)

            if is_back_order:
                if level_price > wanted_price:
                    continue
            else:
                if level_price < wanted_price:
                    continue

            if level_size <= 0:
                continue

            take = min(remaining, level_size)
            level.size = max(0.0, level.size - take)
            remaining -= take

        cross_ladder[:] = [lvl for lvl in cross_ladder if lvl.size > 0]
        return preview_matched, preview_avg