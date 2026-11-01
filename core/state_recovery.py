from __future__ import annotations

import logging
from typing import Any, Dict, List


logger = logging.getLogger(__name__)


class StateRecovery:
    """
    Recupera stato dopo crash/restart.
    """

    def __init__(self, db=None, bus=None, reconciliation_engine=None, duplication_guard=None):
        self.db = db
        self.bus = bus
        self.reconciliation_engine = reconciliation_engine
        self.duplication_guard = duplication_guard

    def recover_pending_orders(self):
        if not self.db or not hasattr(self.db, "get_pending_sagas"):
            return []

        try:
            sagas = self.db.get_pending_sagas() or []
        except Exception:
            logger.exception("Errore recupero saghe")
            return []

        recovered = []

        for saga in sagas:
            try:
                if saga.get("status") != "PENDING":
                    continue

                payload = saga.get("payload") or {}

                if self.bus:
                    self.bus.publish("RECOVER_ORDER", payload)

                recovered.append(saga)

            except Exception:
                logger.exception("Errore recovery saga")

        return recovered

    def recover(self) -> Dict[str, Any]:
        recovered_sagas = self.recover_pending_orders()
        startup = self._reconcile_startup_ghost_orders()

        if startup.get("required") and not startup.get("completed"):
            return {
                "ok": False,
                "reason": "STARTUP_RECONCILIATION_REQUIRED",
                "pending_sagas": len(recovered_sagas),
                "ghost_orders_recovered": 0,
                "startup_mode": "BLOCKED",
            }

        ghost_count = int(startup.get("ghost_orders_recovered", 0))
        # F-08 (Codex P1 su 1b22a4e): un ghost gia' persistito a un avvio precedente e ancora vivo sull'exchange non
        # e' "recuperato" di nuovo, ma l'avvio NON e' piatto: la modalita' segue gli ordini ghost ancora attivi.
        ghost_active = int(startup.get("ghost_orders_active", ghost_count))
        startup_mode = "LIVE_WITH_RECOVERED_GHOSTS" if (ghost_count or ghost_active) else "LIVE_FLAT"

        return {
            "ok": True,
            "reason": "RECOVERED",
            "pending_sagas": len(recovered_sagas),
            "ghost_orders_recovered": ghost_count,
            "ghost_orders_active": ghost_active,
            "startup_mode": startup_mode,
        }

    def _reconcile_startup_ghost_orders(self) -> Dict[str, Any]:
        fetch_remote = getattr(self.reconciliation_engine, "fetch_startup_active_orders", None)
        merge_remote = getattr(self.reconciliation_engine, "merge_startup_active_orders", None)

        if not callable(fetch_remote):
            return {"required": False, "completed": False, "ghost_orders_recovered": 0}

        try:
            remote_orders = fetch_remote() or []
        except Exception:
            logger.exception("Errore fetch ordini attivi startup")
            return {"required": True, "completed": False, "ghost_orders_recovered": 0}

        if callable(merge_remote):
            try:
                merge_result = merge_remote(remote_orders) or {}
                remote_orders = merge_result.get("orders", remote_orders)
            except Exception:
                logger.exception("Errore merge ordini startup")
                return {"required": True, "completed": False, "ghost_orders_recovered": 0}

        recovered = []
        active = 0
        for order in remote_orders:
            try:
                missing = self._is_missing_in_db(order)
                if missing:
                    self._persist_ghost_order(order)
                    recovered.append(order)
                # F-08 (Codex P1 su 1b22a4e): l'ordine e' ancora vivo sull'exchange, quindi va seminato nella guardia
                # a OGNI avvio (il runtime ne crea una nuova a ogni START), non solo quando la riga e' nuova:
                # altrimenti al secondo avvio il duplicato dello stesso intento verrebbe accettato.
                self._seed_duplication_guard(order)
                active += 1
            except Exception:
                # F-08: lookup o persistenza del ghost falliti => riconciliazione NON completata (fail-closed).
                # Prima l'errore del DB valeva "gia' presente" e il ghost era perso con recover() ok=True.
                logger.exception("Ordine attivo dell'exchange non riconciliabile con il DB locale")
                return {"required": True, "completed": False, "ghost_orders_recovered": len(recovered),
                        "ghost_orders_active": active}

        return {
            "required": True,
            "completed": True,
            "ghost_orders_recovered": len(recovered),
            "ghost_orders_active": active,
        }

    def _is_missing_in_db(self, order: Dict[str, Any]) -> bool:
        if not self.db:
            return True

        exists = getattr(self.db, "startup_order_exists", None)
        if callable(exists):
            return not bool(exists(order))      # F-08: un errore del DB si propaga (fail-closed nel chiamante)

        order_id = str(order.get("order_id") or order.get("bet_id") or "")
        if not order_id:
            return True

        load = getattr(self.db, "load_startup_orders", None)
        if callable(load):
            current = load() or []              # F-08: un errore del DB si propaga (fail-closed nel chiamante)
            ids = {
                str(x.get("order_id") or x.get("bet_id") or "")
                for x in current
                if isinstance(x, dict)
            }
            return order_id not in ids

        return True

    def _persist_ghost_order(self, order: Dict[str, Any]) -> None:
        if not self.db:
            return

        upsert = getattr(self.db, "upsert_startup_order", None)
        if callable(upsert):
            upsert(order)

        if self.bus:
            self.bus.publish("STARTUP_GHOST_ORDER_RECOVERED", order)

    def _seed_duplication_guard(self, order: Dict[str, Any]) -> None:
        if not self.duplication_guard:
            return

        seed = getattr(self.duplication_guard, "register_startup_order", None)
        if callable(seed):
            seed(order)
            return

        event_key = str(order.get("event_key") or "").strip()
        if event_key:
            self.duplication_guard.acquire(event_key)
