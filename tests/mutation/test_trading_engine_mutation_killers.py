import pytest


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
        self.audit_events = []
        self.orders = {}
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


class OkOrderManager:
    def submit(self, payload):
        return {"bet_id": "BET-OK", "payload": payload}


class TimeoutOrderManager:
    def submit(self, payload):
        raise TimeoutError("timeout waiting broker confirmation")


@pytest.mark.mutation
def test_mutation_killer_submitted_must_map_to_accepted_for_processing():
    from core.trading_engine import TradingEngine

    engine = TradingEngine(
        bus=FakeBus(),
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    engine.order_manager = OkOrderManager()

    result = engine.submit_quick_bet(
        {
            "market_id": "1.300",
            "selection_id": 1,
            "price": 2.0,
            "stake": 5.0,
            "side": "BACK",
            "customer_ref": "MAP-1",
            "correlation_id": "CID-MAP-1",
        }
    )

    assert result["ok"] is True
    assert result["status"] == "ACCEPTED_FOR_PROCESSING"


@pytest.mark.mutation
def test_mutation_killer_timeout_must_become_ambiguous_not_failed():
    from core.trading_engine import TradingEngine

    engine = TradingEngine(
        bus=FakeBus(),
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    engine.order_manager = TimeoutOrderManager()

    result = engine.submit_quick_bet(
        {
            "market_id": "1.301",
            "selection_id": 2,
            "price": 2.2,
            "stake": 5.0,
            "side": "BACK",
            "customer_ref": "TIMEOUT-1",
            "correlation_id": "CID-TIMEOUT-1",
        }
    )

    assert result["status"] == "AMBIGUOUS"
    assert result["outcome"] == "AMBIGUOUS"
    assert result["ambiguity_reason"] == "SUBMIT_TIMEOUT"


@pytest.mark.mutation
def test_mutation_killer_duplicate_path_must_not_call_finalize_completed_mapping():
    from core.trading_engine import TradingEngine

    engine = TradingEngine(
        bus=FakeBus(),
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    with engine._lock:
        engine._inflight_keys.add("DUP-KILL")

    result = engine.submit_quick_bet(
        {
            "market_id": "1.302",
            "selection_id": 3,
            "price": 2.4,
            "stake": 6.0,
            "side": "BACK",
            "customer_ref": "DUP-KILL",
            "correlation_id": "CID-DUP-KILL",
        }
    )

    assert result["ok"] is True
    assert result["status"] == "DUPLICATE_BLOCKED"
    assert result["reason"] == "DUPLICATE_BLOCKED"