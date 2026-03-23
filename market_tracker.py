"""
Market Tracker - Cache + Delta Detection per Market Data.

Features:
    - Cache market book con TTL configurabile
    - Delta detection: aggiorna solo se cambiato
    - Riduce chiamate API Betfair
    - Metriche: hit rate, API calls risparmiate
    - Thread-safe
"""

import logging
import threading
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class MarketCache:
    """
    Cache per market book con TTL e delta detection.

    Riduce chiamate API Betfair cachando i dati recenti.
    """

    def __init__(self, ttl: float = 1.0, max_size: int = 100):
        """
        Args:
            ttl: Time-to-live in secondi (default 1.0s)
            max_size: Numero massimo di market in cache
        """
        self._cache: Dict[str, Dict] = {}
        self._timestamps: Dict[str, float] = {}
        self._lock = threading.RLock()
        self.ttl = ttl
        self.max_size = max_size

        self._hits = 0
        self._misses = 0
        self._api_calls_saved = 0

    def get(self, market_id: str) -> Optional[Dict]:
        """
        Recupera market book dalla cache se valido.

        Returns:
            Market book dict o None se scaduto/non presente
        """
        with self._lock:
            if market_id not in self._cache:
                self._misses += 1
                return None

            ts = self._timestamps.get(market_id, 0)
            if time.time() - ts > self.ttl:
                del self._cache[market_id]
                del self._timestamps[market_id]
                self._misses += 1
                return None

            self._hits += 1
            self._api_calls_saved += 1
            return self._cache[market_id].copy()

    def set(self, market_id: str, data: Dict):
        """Salva market book in cache."""
        with self._lock:
            # FIX #29: evict only when the key is NEW and would push the cache
            # past its capacity.  The old condition `len >= max_size` triggered
            # on every update of an EXISTING key, evicting a different entry
            # even though the size had not actually grown.
            is_new_key = market_id not in self._cache
            if is_new_key and len(self._cache) >= self.max_size and self._timestamps:
                oldest_key = min(self._timestamps, key=self._timestamps.get)
                self._cache.pop(oldest_key, None)
                self._timestamps.pop(oldest_key, None)

            self._cache[market_id] = data.copy()
            self._timestamps[market_id] = time.time()

    def invalidate(self, market_id: str):
        """Invalida entry specifica."""
        with self._lock:
            self._cache.pop(market_id, None)
            self._timestamps.pop(market_id, None)

    def clear(self):
        """Svuota cache."""
        with self._lock:
            self._cache.clear()
            self._timestamps.clear()

    def get_stats(self) -> Dict:
        """Statistiche cache."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = round(self._hits / max(1, total) * 100, 1)
            return {
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
                "api_calls_saved": self._api_calls_saved,
                "cache_size": len(self._cache),
                "max_size": self.max_size,
                "ttl": self.ttl,
            }

    def reset_stats(self):
        """Reset statistiche."""
        with self._lock:
            self._hits = 0
            self._misses = 0
            self._api_calls_saved = 0


class DeltaDetector:
    """
    Rileva cambiamenti significativi nei prezzi.

    Evita aggiornamenti UI/logica per variazioni minime.
    """

    def __init__(self, min_price_change: float = 0.01, min_volume_change: float = 1.0):
        """
        Args:
            min_price_change: Variazione minima prezzo per trigger (default 1 tick)
            min_volume_change: Variazione minima volume (default 1 unita)
        """
        self._last_prices: Dict[str, Dict[int, Dict]] = defaultdict(dict)
        self._lock = threading.RLock()
        self.min_price_change = min_price_change
        self.min_volume_change = min_volume_change

        self._changes_detected = 0
        self._changes_skipped = 0

    def has_changed(
        self,
        market_id: str,
        selection_id: int,
        back_price: float,
        lay_price: float,
        back_size: float = 0,
        lay_size: float = 0,
    ) -> Tuple[bool, str]:
        """
        Verifica se i prezzi sono cambiati significativamente.

        Returns:
            (changed, reason)
        """
        with self._lock:
            key = f"{market_id}_{selection_id}"
            last = self._last_prices.get(market_id, {}).get(selection_id, {})

            if not last:
                self._last_prices[market_id][selection_id] = {
                    "back": back_price,
                    "lay": lay_price,
                    "back_size": back_size,
                    "lay_size": lay_size,
                    "ts": time.time(),
                }
                self._changes_detected += 1
                return True, "Prima lettura"

            back_diff = abs(back_price - last.get("back", 0))
            lay_diff = abs(lay_price - last.get("lay", 0))
            back_vol_diff = abs(back_size - last.get("back_size", 0))
            lay_vol_diff = abs(lay_size - last.get("lay_size", 0))

            price_changed = (
                back_diff >= self.min_price_change or lay_diff >= self.min_price_change
            )
            volume_changed = (
                back_vol_diff >= self.min_volume_change
                or lay_vol_diff >= self.min_volume_change
            )

            if price_changed or volume_changed:
                self._last_prices[market_id][selection_id] = {
                    "back": back_price,
                    "lay": lay_price,
                    "back_size": back_size,
                    "lay_size": lay_size,
                    "ts": time.time(),
                }
                self._changes_detected += 1

                if price_changed:
                    return True, f"Prezzo: BACK Δ{back_diff:.2f}, LAY Δ{lay_diff:.2f}"
                return (
                    True,
                    f"Volume: BACK Δ{back_vol_diff:.1f}, LAY Δ{lay_vol_diff:.1f}",
                )

            self._changes_skipped += 1
            return False, "Nessun cambiamento significativo"

    def get_last_price(self, market_id: str, selection_id: int) -> Optional[Dict]:
        """Ultimo prezzo registrato."""
        with self._lock:
            return self._last_prices.get(market_id, {}).get(selection_id)

    def clear_market(self, market_id: str):
        """Pulisce dati per un market."""
        with self._lock:
            self._last_prices.pop(market_id, None)

    def get_stats(self) -> Dict:
        """Statistiche delta detection."""
        total = self._changes_detected + self._changes_skipped
        return {
            "changes_detected": self._changes_detected,
            "changes_skipped": self._changes_skipped,
            "skip_rate": round(self._changes_skipped / max(1, total) * 100, 1),
            "markets_tracked": len(self._last_prices),
        }

    def reset_stats(self):
        """Reset statistiche."""
        self._changes_detected = 0
        self._changes_skipped = 0


class MarketTracker:
    """
    Tracker completo per market con cache e delta detection.

    Combina MarketCache + DeltaDetector per ottimizzare
    sia le chiamate API che gli aggiornamenti UI.
    """

    def __init__(
        self, betfair_client, cache_ttl: float = 1.0, min_price_change: float = 0.01
    ):
        self.client = betfair_client
        self.cache = MarketCache(ttl=cache_ttl)
        self.delta = DeltaDetector(min_price_change=min_price_change)
        self._lock = threading.RLock()

        self._active_markets: Dict[str, Dict] = {}
        self._last_refresh: Dict[str, float] = {}

    def get_market_book(
        self, market_id: str, force_refresh: bool = False
    ) -> Optional[Dict]:
        """
        Recupera market book con caching intelligente.

        Args:
            market_id: ID mercato
            force_refresh: Forza refresh dalla API

        Returns:
            Market book dict o None se errore
        """
        if not force_refresh:
            cached = self.cache.get(market_id)
            if cached:
                return cached

        try:
            data = self.client.get_market_book(market_id)
            if data:
                self.cache.set(market_id, data)
                self._last_refresh[market_id] = time.time()
            return data
        except Exception as e:
            logger.error(f"[MARKET_TRACKER] Error fetching {market_id}: {e}")
            return None

    def get_best_prices(self, market_id: str) -> Dict[int, Dict]:
        """
        Estrae best back/lay per ogni selezione.

        Returns:
            {selection_id: {'back': price, 'lay': price, 'back_size': size, 'lay_size': size}}
        """
        book = self.get_market_book(market_id)
        if not book:
            return {}

        prices = {}
        runners = book.get("runners", [])

        for runner in runners:
            sel_id = runner.get("selectionId")

            back_prices = runner.get("ex", {}).get("availableToBack", [])
            lay_prices = runner.get("ex", {}).get("availableToLay", [])

            best_back = back_prices[0] if back_prices else {"price": 0, "size": 0}
            best_lay = lay_prices[0] if lay_prices else {"price": 0, "size": 0}

            prices[sel_id] = {
                "back": best_back.get("price", 0),
                "lay": best_lay.get("price", 0),
                "back_size": best_back.get("size", 0),
                "lay_size": best_lay.get("size", 0),
            }

        return prices

    def get_changed_prices(self, market_id: str) -> Dict[int, Dict]:
        """
        Recupera solo i prezzi che sono cambiati significativamente.

        Usa delta detection per filtrare variazioni minime.
        """
        all_prices = self.get_best_prices(market_id)
        changed = {}

        for sel_id, price_data in all_prices.items():
            has_changed, reason = self.delta.has_changed(
                market_id,
                sel_id,
                price_data["back"],
                price_data["lay"],
                price_data["back_size"],
                price_data["lay_size"],
            )
            if has_changed:
                changed[sel_id] = {**price_data, "change_reason": reason}

        return changed

    def track_market(self, market_id: str, metadata: Dict = None):
        """Inizia tracking di un market."""
        with self._lock:
            self._active_markets[market_id] = {
                "added": time.time(),
                "metadata": metadata or {},
            }

    def untrack_market(self, market_id: str):
        """Ferma tracking di un market."""
        with self._lock:
            self._active_markets.pop(market_id, None)
            self.cache.invalidate(market_id)
            self.delta.clear_market(market_id)

    def get_active_markets(self) -> List[str]:
        """Lista market attivi."""
        with self._lock:
            return list(self._active_markets.keys())

    def get_stats(self) -> Dict:
        """Statistiche complete tracker."""
        return {
            "cache": self.cache.get_stats(),
            "delta": self.delta.get_stats(),
            "active_markets": len(self._active_markets),
            "last_refresh": dict(self._last_refresh),
        }

    def reset(self):
        """Reset completo tracker."""
        self.cache.clear()
        self.cache.reset_stats()
        self.delta.reset_stats()
        self._active_markets.clear()
        self._last_refresh.clear()


_global_cache = None
_global_delta = None


def get_market_cache(ttl: float = 1.0) -> MarketCache:
    """Singleton cache globale."""
    global _global_cache
    if _global_cache is None:
        _global_cache = MarketCache(ttl=ttl)
    return _global_cache


def get_delta_detector() -> DeltaDetector:
    """Singleton delta detector globale."""
    global _global_delta
    if _global_delta is None:
        _global_delta = DeltaDetector()
    return _global_delta

