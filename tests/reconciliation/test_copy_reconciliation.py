from __future__ import annotations

from core.reconciliation_engine import ReasonCode, ReconcileConfig, ReconciliationEngine
from tests.fixtures.fake_batch_manager import FakeBatchManager


class FakeDB:
    def persist_decision_log(self, batch_id, entries):
        return None

    def get_pending_sagas(self):
        return []

    def get_reconcile_marker(self, batch_id):
        return None

    def set_reconcile_marker(self, batch_id, value):
        return None


class FakeBus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload):
        self.events.append((name, payload))


class FakeClient:
    def __init__(self, orders):
        self._orders = orders

    def get_current_orders(self, market_ids):
        return list(self._orders)


def test_copy_reconciliation_detects_ghosts_and_converges_remote_vs_local():
    bm = FakeBatchManager()
    batch_id = "B-COPY-1"
    bm.seed_batch(
        batch_id,
        {"batch_id": batch_id, "status": "LIVE", "market_id": "1.500"},
        [{"leg_index": 0, "status": "PLACED", "customer_ref": "CREF-1", "market_id": "1.500", "selection_id": 11}],
    )

    remote_orders = [
        {"customerOrderRef": "CREF-1", "betId": "BET-OK", "status": "EXECUTION_COMPLETE", "sizeMatched": 10.0},
        {"customerOrderRef": "GHOST-REF", "betId": "BET-GHOST", "status": "EXECUTION_COMPLETE", "sizeMatched": 5.0},
    ]
    bus = FakeBus()

    engine = ReconciliationEngine(
        db=FakeDB(),
        bus=bus,
        batch_manager=bm,
        client_getter=lambda: FakeClient(remote_orders),
        config=ReconcileConfig(max_transient_retries=0, convergence_sleep_secs=0.0),
    )

    result = engine.reconcile_batch(batch_id)
    leg = bm.get_batch_legs(batch_id)[0]

    assert result["ok"] is True
    assert result["reason_code"] == ReasonCode.CONVERGED.value
    assert leg["status"] == "MATCHED"

    published_names = [name for name, _ in bus.events]
    assert "RECONCILIATION_GHOST_ORDERS" in published_names
