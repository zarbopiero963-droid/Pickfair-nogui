from __future__ import annotations

from core.reconciliation_engine import ReconciliationEngine
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


def test_reconcile_metadata_isolated_between_copy_and_pattern_sources():
    engine = ReconciliationEngine(
        db=FakeDB(),
        bus=None,
        batch_manager=FakeBatchManager(),
        client_getter=lambda: None,
    )

    remote_orders = [
        {
            "customerOrderRef": "COPY-REF-1",
            "betId": "BET-COPY",
            "marketId": "1.101",
            "selectionId": 10,
            "meta": {"source": "copy", "copy_group_id": "CG-1"},
        },
        {
            "customerOrderRef": "PATTERN-REF-1",
            "betId": "BET-PAT",
            "marketId": "1.101",
            "selectionId": 10,
            "meta": {"source": "pattern", "pattern_id": "PT-1"},
        },
    ]

    by_ref, by_bet, by_sel = engine._build_exchange_indices(remote_orders)

    copy_leg = {"customer_ref": "COPY-REF-1", "market_id": "1.101", "selection_id": 10}
    pattern_leg = {"customer_ref": "PATTERN-REF-1", "market_id": "1.101", "selection_id": 10}

    copy_match = engine._lookup_remote_order(copy_leg, by_ref, by_bet, by_sel)
    pattern_match = engine._lookup_remote_order(pattern_leg, by_ref, by_bet, by_sel)

    assert copy_match["meta"]["source"] == "copy"
    assert pattern_match["meta"]["source"] == "pattern"
    assert "pattern_id" not in copy_match["meta"]
    assert "copy_group_id" not in pattern_match["meta"]


def test_merge_startup_active_orders_preserves_copy_pattern_lineage_metadata():
    engine = ReconciliationEngine(
        db=FakeDB(),
        bus=None,
        batch_manager=FakeBatchManager(),
        client_getter=lambda: None,
    )

    remote_orders = [
        {
            "customerOrderRef": "COPY-REF-2",
            "order_origin": "COPY",
            "copy_meta": {"copy_group_id": "CG-22", "action_id": "A-22"},
            "meta": {"source": "copy"},
        },
        {
            "customerOrderRef": "PATTERN-REF-2",
            "order_origin": "PATTERN",
            "pattern_meta": {"pattern_id": "PT-22", "pattern_label": "late-over"},
            "meta": {"source": "pattern"},
        },
    ]

    merged = engine.merge_startup_active_orders(remote_orders)

    assert merged["count"] == 2
    assert merged["orders"][0]["order_origin"] == "COPY"
    assert merged["orders"][0]["copy_meta"]["copy_group_id"] == "CG-22"
    assert merged["orders"][1]["order_origin"] == "PATTERN"
    assert merged["orders"][1]["pattern_meta"]["pattern_id"] == "PT-22"
