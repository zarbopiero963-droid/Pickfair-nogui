from __future__ import annotations

import logging
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


class MarketTracker:
    """
    Tracker mercati live.

    Responsabilità:
    - ricevere market_book (stream/polling)
    - mantenere cache
    - aggiornare simulation automaticamente
    - pubblicare su EventBus
    """

    def __init__(self, bus=None, betfair_service=None):
        self.bus = bus
        self.betfair_service = betfair_service

        self._markets: Dict[str, Dict[str, Any]] = {}

    # =========================================================
    # ENTRYPOINT
    # =========================================================
    def on_market_book(self, market_book: Dict[str, Any]) -> None:
        if not isinstance(market_book, dict):
            return

        market_id = str(market_book.get("marketId") or "")
        if not market_id:
            return

        # cache locale
        self._markets[market_id] = market_book

        # 🔥 aggiorna simulation
        self._forward_to_simulation(market_book)

        # 🔥 evento globale (PnL, UI, engine)
        self._publish_market_update(market_book)

    # =========================================================
    # SIMULATION
    # =========================================================
    def _forward_to_simulation(self, market_book: Dict[str, Any]) -> None:
        if not self.betfair_service:
            return

        try:
            status = self.betfair_service.status()

            if not status.get("simulation_mode"):
                return

            self.betfair_service.update_simulation_market_book(market_book)

        except Exception:
            logger.exception("Errore update simulation market book")

    # =========================================================
    # EVENT BUS
    # =========================================================
    def _publish_market_update(self, market_book: Dict[str, Any]) -> None:
        if not self.bus:
            return

        try:
            self.bus.publish("MARKET_BOOK_UPDATE", market_book)
        except Exception:
            logger.exception("Errore publish MARKET_BOOK_UPDATE")

    # =========================================================
    # HELPERS
    # =========================================================
    def get_market(self, market_id: str) -> Optional[Dict[str, Any]]:
        return self._markets.get(str(market_id))

    def snapshot(self) -> Dict[str, Any]:
        return {
            "tracked_markets": len(self._markets),
        }