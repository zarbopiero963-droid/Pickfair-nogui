from __future__ import annotations

from copy import deepcopy
from threading import RLock
from typing import Any, Dict, List, Optional


class SimulationOrderBook:
    """
    Order book simulato per market books live/sim.

    Responsabilità:
    - memorizzare market book per market_id
    - leggere ladder BACK / LAY di un runner
    - opzionalmente consumare liquidità quando un ordine viene matchato
    - restituire snapshot sicuri copiati

    Struttura attesa market_book:
    {
        "marketId" / "market_id": "...",
        "runners": [
            {
                "selectionId": 123,
                "ex": {
                    "availableToBack": [{"price": 2.0, "size": 100.0}, ...],
                    "availableToLay":  [{"price": 2.02, "size": 80.0}, ...],
                }
            }
        ]
    }
    """

    def __init__(self):
        self._lock = RLock()
        self._books: Dict[str, Dict[str, Any]] = {}

    # =========================================================
    # INTERNAL HELPERS
    # =========================================================
    def _normalize_market_id(self, market_id: Any) -> str:
        return str(market_id or "").strip()

    def _extract_market_id(self, market_book: Dict[str, Any]) -> str:
        return self._normalize_market_id(
            market_book.get("market_id")
            or market_book.get("marketId")
            or market_book.get("id")
            or ""
        )

    def _safe_runner_selection_id(self, runner: Dict[str, Any]) -> Optional[int]:
        try:
            return int(runner.get("selectionId"))
        except Exception:
            try:
                return int(runner.get("selection_id"))
            except Exception:
                return None

    def _safe_ladder(self, runner: Dict[str, Any], key: str) -> List[Dict[str, float]]:
        ex = runner.get("ex") or {}
        ladder = ex.get(key) or []
        out: List[Dict[str, float]] = []

        for level in ladder:
            try:
                price = float(level.get("price", 0.0) or 0.0)
                size = float(level.get("size", 0.0) or 0.0)
                if price > 0.0 and size >= 0.0:
                    out.append({"price": price, "size": size})
            except Exception:
                continue
        return out

    def _get_runner_ref(self, market_id: str, selection_id: int) -> Optional[Dict[str, Any]]:
        book = self._books.get(market_id)
        if not book:
            return None

        runners = book.get("runners") or []
        for runner in runners:
            try:
                if self._safe_runner_selection_id(runner) == int(selection_id):
                    return runner
            except Exception:
                continue
        return None

    # =========================================================
    # PUBLIC API
    # =========================================================
    def update_market_book(self, market_id: str, market_book: Dict[str, Any]) -> None:
        with self._lock:
            incoming = deepcopy(market_book or {})
            resolved_market_id = self._normalize_market_id(market_id) or self._extract_market_id(incoming)
            if not resolved_market_id:
                raise ValueError("market_id mancante")

            incoming["market_id"] = resolved_market_id
            if "marketId" not in incoming:
                incoming["marketId"] = resolved_market_id

            self._books[resolved_market_id] = incoming

    def get_market_book(self, market_id: str) -> Dict[str, Any]:
        with self._lock:
            market_id = self._normalize_market_id(market_id)
            book = self._books.get(market_id)
            return deepcopy(book) if book else {}

    def has_market(self, market_id: str) -> bool:
        with self._lock:
            return self._normalize_market_id(market_id) in self._books

    def clear_market(self, market_id: str) -> None:
        with self._lock:
            self._books.pop(self._normalize_market_id(market_id), None)

    def clear_all(self) -> None:
        with self._lock:
            self._books.clear()

    # =========================================================
    # RUNNER / LADDER ACCESS
    # =========================================================
    def get_runner(self, market_id: str, selection_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            runner = self._get_runner_ref(self._normalize_market_id(market_id), int(selection_id))
            return deepcopy(runner) if runner else None

    def get_available_to_back(self, market_id: str, selection_id: int) -> List[Dict[str, float]]:
        with self._lock:
            runner = self._get_runner_ref(self._normalize_market_id(market_id), int(selection_id))
            if not runner:
                return []
            return deepcopy(self._safe_ladder(runner, "availableToBack"))

    def get_available_to_lay(self, market_id: str, selection_id: int) -> List[Dict[str, float]]:
        with self._lock:
            runner = self._get_runner_ref(self._normalize_market_id(market_id), int(selection_id))
            if not runner:
                return []
            return deepcopy(self._safe_ladder(runner, "availableToLay"))

    def get_best_back(self, market_id: str, selection_id: int) -> Optional[Dict[str, float]]:
        ladder = self.get_available_to_back(market_id, selection_id)
        return ladder[0] if ladder else None

    def get_best_lay(self, market_id: str, selection_id: int) -> Optional[Dict[str, float]]:
        ladder = self.get_available_to_lay(market_id, selection_id)
        return ladder[0] if ladder else None

    def get_opposite_ladder(self, market_id: str, selection_id: int, order_side: str) -> List[Dict[str, float]]:
        """
        Per un ordine:
        - BACK matcha contro availableToLay
        - LAY  matcha contro availableToBack
        """
        side = str(order_side or "BACK").upper().strip()
        if side == "BACK":
            return self.get_available_to_lay(market_id, selection_id)
        return self.get_available_to_back(market_id, selection_id)

    # =========================================================
    # LIQUIDITY CONSUMPTION
    # =========================================================
    def consume_liquidity(
        self,
        market_id: str,
        selection_id: int,
        order_side: str,
        matched_price: float,
        matched_size: float,
    ) -> Dict[str, Any]:
        """
        Consuma liquidità dal lato opposto del book:
        - BACK consuma availableToLay
        - LAY consuma availableToBack
        """
        with self._lock:
            market_id = self._normalize_market_id(market_id)
            selection_id = int(selection_id)
            side = str(order_side or "BACK").upper().strip()
            matched_price = float(matched_price or 0.0)
            matched_size = float(matched_size or 0.0)

            if matched_price <= 0.0 or matched_size <= 0.0:
                return {
                    "ok": False,
                    "reason": "invalid_match_values",
                }

            runner = self._get_runner_ref(market_id, selection_id)
            if not runner:
                return {
                    "ok": False,
                    "reason": "runner_not_found",
                }

            ex = runner.setdefault("ex", {})
            book_key = "availableToLay" if side == "BACK" else "availableToBack"
            ladder = ex.get(book_key) or []

            remaining = matched_size
            consumed = 0.0

            for level in ladder:
                try:
                    level_price = float(level.get("price", 0.0) or 0.0)
                    level_size = float(level.get("size", 0.0) or 0.0)
                except Exception:
                    continue

                if level_price != matched_price:
                    continue
                if level_size <= 0.0:
                    continue

                take = min(level_size, remaining)
                level["size"] = max(0.0, level_size - take)
                consumed += take
                remaining -= take

                if remaining <= 0.0:
                    break

            # pulizia livelli a zero
            ex[book_key] = [
                {
                    "price": float(level.get("price", 0.0) or 0.0),
                    "size": float(level.get("size", 0.0) or 0.0),
                }
                for level in ladder
                if float(level.get("size", 0.0) or 0.0) > 0.0
            ]

            return {
                "ok": consumed > 0.0,
                "consumed": float(consumed),
                "requested": float(matched_size),
                "remaining_unfilled": float(max(0.0, matched_size - consumed)),
                "book_key": book_key,
            }

    # =========================================================
    # SNAPSHOT
    # =========================================================
    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "markets_cached": len(self._books),
                "market_ids": list(self._books.keys()),
            }