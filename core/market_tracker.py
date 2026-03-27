from __future__ import annotations

import logging
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


class MarketTracker:
    """
    Tracker mercati live / simulation.

    Responsabilità:
    - ricevere market_book (stream/polling)
    - mantenere cache locale
    - inoltrare a SimulationBroker
    - pubblicare evento su EventBus
    """

    def __init__(self, bus=None, betfair_service=None):
        self.bus = bus
        self.betfair_service = betfair_service

        # cache ultimo stato mercati
        self._markets: Dict[str, Dict[str, Any]] = {}

    # =========================================================
    # ENTRYPOINT PRINCIPALE
    # =========================================================
    def on_market_book(self, market_book: Dict[str, Any]) -> None:
        """
        Chiamato da:
        - streaming
        - polling API
        """

        if not isinstance(market_book, dict):
            return

        market_id = str(market_book.get("marketId") or "")
        if not market_id:
            return

        # salva snapshot locale
        self._markets[market_id] = market_book

        # 🔥 1. AGGIORNA SIMULATION
        self._forward_to_simulation(market_book)

        # 🔥 2. PUBBLICA EVENTO PER PNL / UI / ENGINE
        self._publish_market_update(market_book)

    # =========================================================
    # SIMULATION HOOK
    # =========================================================
    def _forward_to_simulation(self, market_book: Dict[str, Any]) -> None:
        """
        Invia dati al SimulationBroker se attivo.
        """

        if not self.betfair_service:
            return

        try:
            status = self.betfair_service.status()

            # solo se siamo in simulation
            if not status.get("simulation_mode"):
                return

            self.betfair_service.update_simulation_market_book(market_book)

        except Exception:
            logger.exception("Errore update simulation market book")

    # =========================================================
    # EVENT BUS
    # =========================================================
    def _publish_market_update(self, market_book: Dict[str, Any]) -> None:
        """
        Pubblica update per:
        - PnL Engine
        - UI
        - altri moduli
        """

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

    def get_best_prices(self, market_id: str, selection_id: int) -> Dict[str, float]:
        """
        Utility utile per debug / UI.
        """
        market = self._markets.get(str(market_id))
        if not market:
            return {"back": 0.0, "lay": 0.0}

        for r in market.get("runners", []):
            if int(r.get("selectionId")) != int(selection_id):
                continue

            ex = r.get("ex", {})

            back = (ex.get("availableToBack") or [{}])[0].get("price")
            lay = (ex.get("availableToLay") or [{}])[0].get("price")

            return {
                "back": float(back or 0.0),
                "lay": float(lay or 0.0),
            }

        return {"back": 0.0, "lay": 0.0}

    def snapshot(self) -> Dict[str, Any]:
        return {
            "tracked_markets": len(self._markets),
        }