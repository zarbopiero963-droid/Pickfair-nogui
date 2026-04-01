import pytest

from core.trading_engine import TradingEngine


class FakeBus:
    def __init__(self):
        self.subscriptions = {}
        self.events = []

    def subscribe(self, topic, handler):
        self.subscriptions[topic] = handler

    def publish(self, name, payload):
        self.events.append((name, payload))


class FakeDB:
    def __init__(self):
        self.orders = {}
        self.audit_events = []
        self.seq = 0

    def is_ready(self):
        return True

    def insert_order(self, payload):
        self.seq += 1
        order_id = f"ORD-{self.seq}"
        self.orders[order_id] = dict(payload)
        return order_id

    def update_order(self, order_id, update):
        self.orders.setdefault(order_id, {})
        self.orders[order_id].update(update)

    def insert_audit_event(self, event):
        self.audit_events.append(event)

    def order_exists_inflight(self, *, customer_ref, correlation_id):
        return False

    def load_pending_customer_refs(self):
        return []

    def load_pending_correlation_ids(self):
        return []


class InlineExecutor:
    def is_ready(self):
        return True

    def submit(self, _name, fn):
        return fn()


class TimeoutOrderManager:
    def submit(self, payload):
        raise TimeoutError("broker timeout")


@pytest.mark.invariant
def test_ambiguous_result_keeps_customer_ref_locked_in_inflight_keys():
    bus = FakeBus()
    db = FakeDB()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    engine.order_manager = TimeoutOrderManager()

    customer_ref = "AMB-LOCK-1"
    correlation_id = "AMB-CID-1"

    result = engine.submit_quick_bet(
        {
            "market_id": "1.200",
            "selection_id": 2,
            "price": 2.1,
            "stake": 6.0,
            "side": "BACK",
            "customer_ref": customer_ref,
            "correlation_id": correlation_id,
        }
    )

    assert result["ok"] is False
    assert result["status"] == "AMBIGUOUS"
    assert result["ambiguity_reason"] == "SUBMIT_TIMEOUT"

    # Questo è il punto chiave:
    # su AMBIGUOUS il customer_ref NON deve essere rilasciato.
    assert customer_ref in engine._inflight_keys

    # E anche il correlation_id deve restare nel seen set.
    assert correlation_id in engine._seen_correlation_ids