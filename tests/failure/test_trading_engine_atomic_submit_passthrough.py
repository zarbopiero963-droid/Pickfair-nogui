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


class RuntimeExplodingOrderManager:
    def submit(self, payload):
        raise RuntimeError("BROKER_SUBMIT_FAILED")


class TimeoutExplodingOrderManager:
    def submit(self, payload):
        raise TimeoutError("submit timeout while waiting broker ack")


@pytest.mark.failure
def test_atomic_submit_failed_path_keeps_extra_fields():
    from core.trading_engine import TradingEngine

    engine = TradingEngine(
        bus=FakeBus(),
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    engine.order_manager = RuntimeExplodingOrderManager()

    result = engine.submit_quick_bet(
        {
            "market_id": "1.110",
            "selection_id": 11,
            "price": 2.0,
            "stake": 5.0,
            "side": "BACK",
            "customer_ref": "FAIL-X1",
            "correlation_id": "CID-FAIL-X1",
            "simulation_mode": True,
            "event_key": "1.110:11:BACK",
        }
    )

    assert result["ok"] is False
    assert result["status"] == "FAILED"
    assert result["reason"] == "SUBMIT_FAILED"
    assert result["error"] == "BROKER_SUBMIT_FAILED"
    assert result["simulation_mode"] is True
    assert result["event_key"] == "1.110:11:BACK"


@pytest.mark.failure
def test_atomic_submit_ambiguous_timeout_path_keeps_extra_fields():
    from core.trading_engine import TradingEngine

    engine = TradingEngine(
        bus=FakeBus(),
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    engine.order_manager = TimeoutExplodingOrderManager()

    result = engine.submit_quick_bet(
        {
            "market_id": "1.111",
            "selection_id": 12,
            "price": 2.2,
            "stake": 6.0,
            "side": "BACK",
            "customer_ref": "AMB-X1",
            "correlation_id": "CID-AMB-X1",
            "simulation_mode": True,
            "event_key": "1.111:12:BACK",
        }
    )

    assert result["ok"] is False
    assert result["status"] == "AMBIGUOUS"
    assert result["outcome"] == "AMBIGUOUS"
    assert result["ambiguity_reason"] == "SUBMIT_TIMEOUT"
    assert result["simulation_mode"] is True
    assert result["event_key"] == "1.111:12:BACK"