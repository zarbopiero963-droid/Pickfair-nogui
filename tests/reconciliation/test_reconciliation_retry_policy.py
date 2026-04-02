from __future__ import annotations

from core.reconciliation_engine import ReconciliationEngine, ReconcileConfig, ReasonCode


class FakeDB:
    def persist_decision_log(self, batch_id, entries):
        return None

    def get_pending_sagas(self):
        return []

    def get_reconcile_marker(self, batch_id):
        return None

    def set_reconcile_marker(self, batch_id, value):
        return None


class FakeBatchManager:
    def get_batch(self, batch_id):
        return {"batch_id": batch_id, "market_id": "1.1", "status": "LIVE"}

    def get_batch_legs(self, batch_id):
        return []

    def update_leg_status(
        self,
        batch_id,
        leg_index,
        status,
        bet_id=None,
        raw_response=None,
        error_text=None,
    ):
        return None

    def recompute_batch_status(self, batch_id):
        return {"batch_id": batch_id, "status": "LIVE"}

    def release_runtime_artifacts(self, **kwargs):
        return None


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


def make_engine(client):
    return ReconciliationEngine(
        db=FakeDB(),
        batch_manager=FakeBatchManager(),
        client_getter=lambda: client,
        config=ReconcileConfig(
            max_transient_retries=2,
            transient_retry_base_delay=0.0,
            transient_retry_max_delay=0.0,
        ),
    )


def test_transient_timeout_retried_until_success():
    client = FakeClient([TimeoutError("timeout"), []])
    eng = make_engine(client)

    orders, failure_reason = eng._fetch_current_orders_by_market("1.1")

    assert orders == []
    assert failure_reason is None
    assert client.calls == 2


def test_permanent_error_not_retried_if_classifier_added():
    client = FakeClient([RuntimeError("INVALID_MARKET_ID")])
    eng = make_engine(client)

    orders, failure_reason = eng._fetch_current_orders_by_market("1.1")

    assert orders == []
    assert failure_reason == ReasonCode.FETCH_PERMANENT_FAILURE
    assert client.calls == 1