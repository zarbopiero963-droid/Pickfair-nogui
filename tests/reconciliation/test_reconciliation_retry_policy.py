from __future__ import annotations

from core.reconciliation_engine import ReconciliationEngine, ReconcileConfig


class FakeDB:
    pass


class FakeClient:
    def __init__(self, side_effects):
        self.side_effects = list(side_effects)
        self.calls = 0

    def get_current_orders(self, market_ids=None):
        self.calls += 1
        effect = self.side_effects.pop(0)
        if isinstance(effect, Exception):
            raise effect
        return effect


def test_transient_timeout_retried_until_success():
    client = FakeClient([TimeoutError("timeout"), []])
    eng = ReconciliationEngine(
        db=FakeDB(),
        client_getter=lambda: client,
        config=ReconcileConfig(max_transient_retries=2, transient_retry_base_delay=0.0),
    )
    orders = eng._fetch_current_orders_by_market("1.1")
    assert orders == []
    assert client.calls == 2


def test_permanent_error_not_retried_if_classifier_added():
    client = FakeClient([RuntimeError("INVALID_MARKET_ID")])
    eng = ReconciliationEngine(
        db=FakeDB(),
        client_getter=lambda: client,
        config=ReconcileConfig(max_transient_retries=2, transient_retry_base_delay=0.0),
    )
    orders = eng._fetch_current_orders_by_market("1.1")
    assert orders == []
    assert client.calls >= 1