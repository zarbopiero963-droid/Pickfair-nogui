from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


class MarketTracker:
    """
    Market tracker unificato.

    Responsabilità:
    - mantiene snapshot locali dei market book
    - aggiorna cache per market_id
    - inoltra i market book al SimulationBroker quando simulation_mode è attivo
    - pubblica eventi sul bus senza contenere logica di trading

    Eventi pubblicati:
    - MARKET_BOOK_UPDATED
    - MARKET_TRACKER_ERROR
    """

    def __init__(
        self,
        bus=None,
        betfair_service=None,
        *,
        max_cache_size: int = 500,
    ):
        self.bus = bus
        self.betfair_service = betfair_service
        self.max_cache_size = max(10, int(max_cache_size or 500))

        self._lock = threading.RLock()
        self._market_books: Dict[str, Dict[str, Any]] = {}
        self._market_order: List[str] = []
        self._last_update_ts: Dict[str, float] = {}

    # =========================================================
    # INTERNAL
    # =========================================================
    def _publish(self, event_name: str, payload: Dict[str, Any]) -> None:
        if not self.bus:
            return
        try:
            self.bus.publish(event_name, payload)
        except Exception:
            logger.exception("Errore publish %s", event_name)

    def _normalize_market_id(self, market_id: Any) -> str:
        return str(market_id or "").strip()

    def _extract_market_id(self, market_book: Dict[str, Any]) -> str:
        return self._normalize_market_id(
            market_book.get("market_id")
            or market_book.get("marketId")
            or market_book.get("id")
            or ""
        )

    def _trim_cache_if_needed(self) -> None:
        while len(self._market_order) > self.max_cache_size:
            oldest = self._market_order.pop(0)
            self._market_books.pop(oldest, None)
            self._last_update_ts.pop(oldest, None)

    def _touch_market(self, market_id: str) -> None:
        if market_id in self._market_order:
            self._market_order.remove(market_id)
        self._market_order.append(market_id)
        self._last_update_ts[market_id] = time.time()
        self._trim_cache_if_needed()

    # =========================================================
    # PUBLIC CACHE API
    # =========================================================
    def update_market_book(self, market_book: Dict[str, Any]) -> Dict[str, Any]:
        """
        Aggiorna cache locale e, se simulation attiva, inoltra il book al SimulationBroker.
        """
        market_book = dict(market_book or {})
        market_id = self._extract_market_id(market_book)

        if not market_id:
            error = "market_id mancante nel market_book"
            self._publish(
                "MARKET_TRACKER_ERROR",
                {
                    "error": error,
                    "market_book": market_book,
                },
            )
            return {
                "ok": False,
                "error": error,
            }

        with self._lock:
            self._market_books[market_id] = market_book
            self._touch_market(market_id)

        simulation_forward = None
        if self.betfair_service and hasattr(self.betfair_service, "update_simulation_market_book"):
            try:
                simulation_forward = self.betfair_service.update_simulation_market_book(
                    market_id,
                    market_book,
                )
            except Exception as exc:
                logger.exception("Errore update_simulation_market_book: %s", exc)
                simulation_forward = {
                    "ok": False,
                    "reason": str(exc),
                    "simulated": False,
                }

        payload = {
            "market_id": market_id,
            "market_book": market_book,
            "simulation_forward": simulation_forward,
            "updated_at": time.time(),
        }
        self._publish("MARKET_BOOK_UPDATED", payload)

        return {
            "ok": True,
            "market_id": market_id,
            "simulation_forward": simulation_forward,
        }

    def bulk_update_market_books(self, market_books: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        updated = 0
        failed = 0
        last_error = ""

        for book in market_books or []:
            try:
                result = self.update_market_book(book)
                if result.get("ok"):
                    updated += 1
                else:
                    failed += 1
                    last_error = str(result.get("error", ""))
            except Exception as exc:
                failed += 1
                last_error = str(exc)
                logger.exception("Errore bulk update market book: %s", exc)

        return {
            "ok": failed == 0,
            "updated": updated,
            "failed": failed,
            "last_error": last_error,
        }

    def get_market_book(self, market_id: str) -> Optional[Dict[str, Any]]:
        market_id = self._normalize_market_id(market_id)
        with self._lock:
            market_book = self._market_books.get(market_id)
            return dict(market_book) if market_book else None

    def get_all_market_books(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {mid: dict(book) for mid, book in self._market_books.items()}

    def get_last_update_age_sec(self, market_id: str) -> Optional[float]:
        market_id = self._normalize_market_id(market_id)
        with self._lock:
            ts = self._last_update_ts.get(market_id)
            if ts is None:
                return None
            return max(0.0, time.time() - ts)

    def has_market(self, market_id: str) -> bool:
        market_id = self._normalize_market_id(market_id)
        with self._lock:
            return market_id in self._market_books

    def clear_market(self, market_id: str) -> None:
        market_id = self._normalize_market_id(market_id)
        with self._lock:
            self._market_books.pop(market_id, None)
            self._last_update_ts.pop(market_id, None)
            if market_id in self._market_order:
                self._market_order.remove(market_id)

    def clear_all(self) -> None:
        with self._lock:
            self._market_books.clear()
            self._last_update_ts.clear()
            self._market_order.clear()

    # =========================================================
    # RUNNER HELPERS
    # =========================================================
    def get_runner_book(self, market_id: str, selection_id: int) -> Optional[Dict[str, Any]]:
        market_book = self.get_market_book(market_id)
        if not market_book:
            return None

        for runner in market_book.get("runners", []) or []:
            try:
                if int(runner.get("selectionId")) == int(selection_id):
                    return dict(runner)
            except Exception:
                continue
        return None

    def get_best_back(self, market_id: str, selection_id: int) -> Optional[Dict[str, float]]:
        runner = self.get_runner_book(market_id, selection_id)
        if not runner:
            return None

        ladder = ((runner.get("ex") or {}).get("availableToBack") or [])
        if not ladder:
            return None

        level = ladder[0] or {}
        try:
            return {
                "price": float(level.get("price", 0.0) or 0.0),
                "size": float(level.get("size", 0.0) or 0.0),
            }
        except Exception:
            return None

    def get_best_lay(self, market_id: str, selection_id: int) -> Optional[Dict[str, float]]:
        runner = self.get_runner_book(market_id, selection_id)
        if not runner:
            return None

        ladder = ((runner.get("ex") or {}).get("availableToLay") or [])
        if not ladder:
            return None

        level = ladder[0] or {}
        try:
            return {
                "price": float(level.get("price", 0.0) or 0.0),
                "size": float(level.get("size", 0.0) or 0.0),
            }
        except Exception:
            return None

    # =========================================================
    # DEBUG / SNAPSHOT
    # =========================================================
    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "markets_cached": len(self._market_books),
                "market_ids": list(self._market_order),
                "last_update_ts": dict(self._last_update_ts),
            }