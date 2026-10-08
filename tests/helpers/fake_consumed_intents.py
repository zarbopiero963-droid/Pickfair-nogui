"""Doppio in memoria del de-dup durevole degli intenti (#461 PR26).

Dalla PR26 il ramo LIVE dell'engine non invia se il DB non sa marcare
durevolmente il `customer_ref` prima del trasporto
(`DURABLE_DEDUPE_UNAVAILABLE`). I doppi di DB dei test LIVE ereditano questo
mixin: stesso contratto di `Database.consume_order_intent` /
`release_order_intent` / `is_order_intent_consumed`, senza SQLite.
"""
from __future__ import annotations

from typing import Dict


class FakeConsumedIntentsMixin:
    """Una riga per `customer_ref`; si cancella solo con lo stesso `attempt_id`."""

    def _consumati(self) -> Dict[str, str]:
        righe = self.__dict__.get("_fake_consumed_intents")
        if righe is None:
            righe = {}
            self.__dict__["_fake_consumed_intents"] = righe
        return righe

    def consume_order_intent(self, customer_ref, *, attempt_id, correlation_id=""):
        righe = self._consumati()
        if customer_ref in righe:
            return False
        righe[customer_ref] = attempt_id
        return True

    def release_order_intent(self, customer_ref, *, attempt_id):
        righe = self._consumati()
        if righe.get(customer_ref) != attempt_id:
            return False
        del righe[customer_ref]
        return True

    def is_order_intent_consumed(self, customer_ref):
        return customer_ref in self._consumati()
