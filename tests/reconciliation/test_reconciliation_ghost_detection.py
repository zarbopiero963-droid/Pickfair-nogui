from __future__ import annotations

from core.reconciliation_engine import ReconciliationEngine
from tests.fixtures.fake_batch_manager import FakeBatchManager


class FakeDB:
    def persist_decision_log(self, batch_id, entries):
        return None


class FakeBus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload):
        self.events.append((name, payload))


def make_engine():
    return ReconciliationEngine(
        db=FakeDB(),
        bus=FakeBus(),
        batch_manager=FakeBatchManager(),
        client_getter=lambda: None,
    )


def test_order_matched_by_bet_id_not_ghost():
    eng = make_engine()
    legs = [{"leg_index": 0, "customer_ref": "R1", "bet_id": "BET1"}]
    remote = [{"customerOrderRef": "XXX", "betId": "BET1", "status": "EXECUTION_COMPLETE"}]

    by_ref, by_bet, by_sel = eng._build_exchange_indices(remote)
    ghosts = eng._detect_ghost_orders("B1", legs, remote, by_ref, by_bet, by_sel)

    assert ghosts == []


def test_order_matched_by_customer_ref_not_ghost():
    eng = make_engine()
    legs = [{"leg_index": 0, "customer_ref": "R1", "bet_id": ""}]
    remote = [{"customerOrderRef": "R1", "betId": "BETX", "status": "EXECUTION_COMPLETE"}]

    by_ref, by_bet, by_sel = eng._build_exchange_indices(remote)
    ghosts = eng._detect_ghost_orders("B1", legs, remote, by_ref, by_bet, by_sel)

    assert ghosts == []


def test_unmatched_remote_order_is_ghost():
    eng = make_engine()
    legs = [{"leg_index": 0, "customer_ref": "R1", "bet_id": "BET1"}]
    remote = [{"customerOrderRef": "R2", "betId": "BET2", "status": "EXECUTION_COMPLETE"}]

    by_ref, by_bet, by_sel = eng._build_exchange_indices(remote)
    ghosts = eng._detect_ghost_orders("B1", legs, remote, by_ref, by_bet, by_sel)

    assert len(ghosts) == 1
    assert ghosts[0]["customer_ref"] == "R2"
    assert ghosts[0]["bet_id"] == "BET2"