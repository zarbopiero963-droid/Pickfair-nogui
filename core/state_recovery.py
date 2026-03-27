from __future__ import annotations

import logging


logger = logging.getLogger(__name__)


class StateRecovery:
    """
    Recupera stato dopo crash/restart.
    """

    def __init__(self, db=None, bus=None):
        self.db = db
        self.bus = bus

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