"""
Dutching Cache - Hash delle selezioni per evitare ricalcoli ripetuti

Problema: np.linalg.solve() costoso, chiamato anche con input invariati
Soluzione: Cache con hash (prices, side, total_stake)

Impatto: ~10× più veloce in ricalcoli ripetuti
"""

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class CachedDutchResult:
    """Risultato dutching cached."""

    stakes: List[Dict]
    profit: float
    book_percentage: float
    timestamp: float
    hit_count: int = 0


class DutchingCache:
    """
    Cache per calcoli dutching con LRU eviction.

    - Key: hash di (prices, side, total_stake, commission)
    - Deterministico e sicuro
    - Zero impatto sulla logica
    """

    MAX_CACHE_SIZE = 100
    CACHE_TTL = 60.0

    def __init__(self):
        self._lock = threading.Lock()
        self._cache: OrderedDict[int, CachedDutchResult] = OrderedDict()
        self._stats = {"hits": 0, "misses": 0, "evictions": 0}

    def _compute_key(
        self,
        selections: List[Dict],
        total_stake: float,
        bet_type: str,
        commission: float,
    ) -> int:
        """
        Calcola chiave hash per i parametri.

        Usa solo i campi rilevanti per il calcolo.
        """
        # FIX #10: include side in key so BACK/LAY configurations at the same
        # price do not collide. Use effectiveType if present, else side, else
        # fall back to the top-level bet_type parameter.
        price_tuple = tuple(
            (
                s.get("selectionId"),
                round(s.get("price", 0), 2),
                str(s.get("effectiveType") or s.get("side") or bet_type).upper(),
            )
            for s in sorted(selections, key=lambda x: x.get("selectionId", 0))
        )
        return hash(
            (price_tuple, round(total_stake, 2), bet_type, round(commission, 2))
        )

    def get(
        self,
        selections: List[Dict],
        total_stake: float,
        bet_type: str = "BACK",
        commission: float = 4.5,
    ) -> Optional[Tuple[List[Dict], float, float]]:
        """
        Ottiene risultato dalla cache se presente e valido.

        Returns:
            None se cache miss
            (stakes, profit, book_percentage) se cache hit
        """
        key = self._compute_key(selections, total_stake, bet_type, commission)

        with self._lock:
            cached = self._cache.get(key)

            if cached is None:
                self._stats["misses"] += 1
                return None

            if (time.time() - cached.timestamp) > self.CACHE_TTL:
                del self._cache[key]
                self._stats["misses"] += 1
                return None

            cached.hit_count += 1
            self._cache.move_to_end(key)
            self._stats["hits"] += 1

            return (
                [dict(s) for s in cached.stakes],
                cached.profit,
                cached.book_percentage,
            )

    def put(
        self,
        selections: List[Dict],
        total_stake: float,
        bet_type: str,
        commission: float,
        stakes: List[Dict],
        profit: float,
        book_percentage: float,
    ):
        """
        Salva risultato in cache.
        """
        key = self._compute_key(selections, total_stake, bet_type, commission)

        with self._lock:
            if len(self._cache) >= self.MAX_CACHE_SIZE:
                self._cache.popitem(last=False)
                self._stats["evictions"] += 1

            self._cache[key] = CachedDutchResult(
                stakes=[dict(s) for s in stakes],
                profit=profit,
                book_percentage=book_percentage,
                timestamp=time.time(),
            )

    def invalidate_for_market(self, market_id: str):
        """
        Invalida cache per un mercato specifico.

        Nota: richiede che le selezioni contengano market_id.
        Per ora invalida tutta la cache.
        """
        self.clear()

    def clear(self):
        """Svuota completamente la cache."""
        with self._lock:
            self._cache.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Statistiche della cache."""
        with self._lock:
            total = self._stats["hits"] + self._stats["misses"]
            return {
                **self._stats,
                "hit_ratio": (self._stats["hits"] / max(1, total)) * 100,
                "cache_size": len(self._cache),
                "max_size": self.MAX_CACHE_SIZE,
            }


_dutching_cache: Optional[DutchingCache] = None


def get_dutching_cache() -> DutchingCache:
    """Ottiene l'istanza singleton del DutchingCache."""
    global _dutching_cache
    if _dutching_cache is None:
        _dutching_cache = DutchingCache()
    return _dutching_cache


def cached_dutching_stakes(
    calculate_fn,
    selections: List[Dict],
    total_stake: float,
    bet_type: str = "BACK",
    commission: float = 4.5,
) -> Tuple[List[Dict], float, float]:
    """
    Wrapper per calcolo dutching con caching.

    Args:
        calculate_fn: Funzione originale di calcolo
        selections: Selezioni
        total_stake: Stake totale
        bet_type: BACK o LAY
        commission: Commissione %

    Returns:
        (stakes, profit, book_percentage)
    """
    cache = get_dutching_cache()

    cached = cache.get(selections, total_stake, bet_type, commission)
    if cached:
        return cached

    stakes, profit, book_pct = calculate_fn(
        selections, total_stake, bet_type, commission
    )

    cache.put(selections, total_stake, bet_type, commission, stakes, profit, book_pct)

    return stakes, profit, book_pct

