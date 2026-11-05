from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, Optional


logger = logging.getLogger(__name__)


class MarketTracker:
    """
    Tracker mercati live.

    Responsabilità:
    - ricevere market_book da stream/polling
    - mantenere cache locale
    - inoltrare il market book al broker simulato quando la simulation è attiva
    - pubblicare MARKET_BOOK_UPDATE sul bus per UI / debug / altri moduli

    Non contiene logica di chiusura automatica posizioni.
    """

    def __init__(self, bus=None, betfair_service=None, *, monotonic: Callable[[], float] = time.monotonic):
        self.bus = bus
        self.betfair_service = betfair_service
        self._markets: Dict[str, Dict[str, Any]] = {}
        # Istante MONOTONO di ricezione dell'ultimo book per mercato: la freschezza del book e' dimostrabile senza
        # dipendere da `lastMatchTime` dell'exchange (richiesta Control API). Mai l'orologio di sistema.
        self._monotonic = monotonic
        self._received_at: Dict[str, float] = {}

    # =========================================================
    # HELPERS
    # =========================================================
    def _extract_market_id(self, market_book: Dict[str, Any]) -> str:
        return str(
            market_book.get("marketId")
            or market_book.get("market_id")
            or ""
        ).strip()

    def _normalize_market_book(self, market_book: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalizzazione minima per evitare incompatibilità tra marketId/market_id.
        """
        normalized = dict(market_book or {})
        market_id = self._extract_market_id(normalized)
        if market_id:
            normalized["marketId"] = market_id
            normalized["market_id"] = market_id
        return normalized

    # =========================================================
    # ENTRYPOINT
    # =========================================================
    def on_market_book(self, market_book: Dict[str, Any]) -> None:
        if not isinstance(market_book, dict):
            return

        normalized = self._normalize_market_book(market_book)
        market_id = self._extract_market_id(normalized)
        if not market_id:
            return

        self._markets[market_id] = normalized
        self._received_at[market_id] = float(self._monotonic())

        self._forward_to_simulation(normalized)
        self._publish_market_update(normalized)

    # =========================================================
    # SIMULATION FORWARD
    # =========================================================
    def _forward_to_simulation(self, market_book: Dict[str, Any]) -> None:
        if not self.betfair_service:
            return

        try:
            status = self.betfair_service.status() or {}

            # compatibilità doppia:
            # - status()["simulation_mode"]
            # - status()["simulated"]
            simulation_enabled = bool(
                status.get("simulation_mode", status.get("simulated", False))
            )
            if not simulation_enabled:
                return

            # chiama la nuova firma unificata del service
            self.betfair_service.update_simulation_market_book(market_book)

        except Exception:
            logger.exception("Errore update simulation market book")

    # =========================================================
    # BUS PUBLISH
    # =========================================================
    def _publish_market_update(self, market_book: Dict[str, Any]) -> None:
        if not self.bus:
            return

        try:
            self.bus.publish("MARKET_BOOK_UPDATE", market_book)
        except Exception:
            logger.exception("Errore publish MARKET_BOOK_UPDATE")

    # =========================================================
    # HELPERS PUBBLICI
    # =========================================================
    def get_market(self, market_id: str) -> Optional[Dict[str, Any]]:
        return self._markets.get(str(market_id))

    def received_at(self, market_id: str) -> Optional[float]:
        """Istante monotono di ricezione dell'ultimo book del mercato, ``None`` se mai ricevuto."""
        return self._received_at.get(str(market_id))

    def book_age_s(self, market_id: str) -> Optional[float]:
        """Eta' in secondi dell'ultimo book del mercato; ``None`` se mai ricevuto o se l'orologio non e' coerente
        (eta' negativa): un'eta' non dimostrabile non vale mai "fresco"."""
        at = self.received_at(market_id)
        if at is None:
            return None
        age = float(self._monotonic()) - at
        return age if age >= 0.0 else None

    def snapshot(self) -> Dict[str, Any]:
        return {
            "tracked_markets": len(self._markets),
            "market_ids": list(self._markets.keys()),
            "received_at": dict(self._received_at),
        }