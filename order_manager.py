"""
Order Manager - Compatibility Layer (Current Pickfair State)
-----------------------------------------------------------
Versione compatibile con lo stato attuale del progetto.

Obiettivo:
- NON chiamare mai direttamente il client / broker
- NON pubblicare eventi che l'architettura attuale non gestisce
- Lasciare attivi solo comportamenti sicuri e realmente supportati

Stato attuale supportato:
- OMS gestisce quick bet / dutching / cashout via EventBus
- Cancel / Replace OMS NON risultano ancora cablati end-to-end

Quindi questo modulo:
- mantiene una piccola cache locale opzionale
- espone metodi legacy senza rompere la UI
- rifiuta in modo esplicito cancel/replace finché non esiste il wiring OMS completo
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional


logger = logging.getLogger("OrderManager")


class OrderManager:
    def __init__(self, app: Any = None, bus: Any = None, db: Any = None):
        self.app = app
        self.bus = bus if bus is not None else getattr(app, "bus", None)
        self.db = db if db is not None else getattr(app, "db", None)

        # Cache locale puramente legacy / diagnostica
        self._local_cache: Dict[str, Dict[str, Any]] = {}

    # =========================================================
    # LEGACY SAFE API
    # =========================================================

    def cancel_order(self, market_id: str, bet_id: str) -> bool:
        """
        Compatibilità legacy.

        ATTUALE STATO ARCHITETTURA:
        cancel OMS non è ancora cablato end-to-end.
        Quindi NON pubblichiamo eventi fantasma tipo REQ_CANCEL_ORDER.

        Ritorna False in modo esplicito e logga il motivo.
        """
        logger.warning(
            "[OrderManager] cancel_order richiesto ma CANCEL OMS non è cablato "
            "(market_id=%s, bet_id=%s). Operazione non eseguita.",
            market_id,
            bet_id,
        )
        return False

    def replace_order(self, market_id: str, bet_id: str, new_price: float) -> bool:
        """
        Compatibilità legacy.

        ATTUALE STATO ARCHITETTURA:
        replace OMS non è ancora cablato end-to-end.
        Quindi NON pubblichiamo eventi fantasma tipo REQ_REPLACE_ORDER.

        Ritorna False in modo esplicito e logga il motivo.
        """
        logger.warning(
            "[OrderManager] replace_order richiesto ma REPLACE OMS non è cablato "
            "(market_id=%s, bet_id=%s, new_price=%s). Operazione non eseguita.",
            market_id,
            bet_id,
            new_price,
        )
        return False

    # =========================================================
    # CACHE / HOUSEKEEPING
    # =========================================================

    def remember(self, key: str, payload: Optional[Dict[str, Any]] = None) -> None:
        """
        Memorizza temporaneamente info legacy utili a UI/debug.
        Nessun valore operativo lato OMS.
        """
        self._local_cache[str(key)] = {
            "payload": payload or {},
            "ts": time.time(),
        }

    def forget(self, key: str) -> None:
        self._local_cache.pop(str(key), None)

    def get_cached(self, key: str, default: Optional[Any] = None) -> Any:
        item = self._local_cache.get(str(key))
        if item is None:
            return default
        return item.get("payload", default)

    def cleanup_old(self, max_age_seconds: float = 3600) -> None:
        """
        Pulizia cache locale legacy.
        Non tocca DB, OMS o Betfair.
        """
        now = time.time()
        to_delete = []

        for key, item in self._local_cache.items():
            ts = float(item.get("ts", now))
            if now - ts > max_age_seconds:
                to_delete.append(key)

        for key in to_delete:
            self._local_cache.pop(key, None)

        if to_delete:
            logger.info(
                "[OrderManager] cleanup_old rimosse %s entry legacy dalla cache.",
                len(to_delete),
            )

    def clear(self) -> None:
        self._local_cache.clear()

    # =========================================================
    # DIAGNOSTICA
    # =========================================================

    def get_status(self) -> Dict[str, Any]:
        return {
            "bus_available": self.bus is not None,
            "db_available": self.db is not None,
            "cached_items": len(self._local_cache),
            "cancel_supported": False,
            "replace_supported": False,
            "mode": "compatibility_layer_only",
        }

