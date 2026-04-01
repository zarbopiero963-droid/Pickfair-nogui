import pytest


class FakeBus:
    def __init__(self):
        self.events = []
        self.subscriptions = {}

    def subscribe(self, event_name, handler):
        self.subscriptions[event_name] = handler

    def publish(self, event_name, payload=None):
        self.events.append((event_name, payload))


class FakeDB:
    def __init__(self):
        self.orders = {}
        self.audit_events = []
        self.seq = 0

    def is_ready(self):
        return True

    def insert_order(self, payload):
        self.seq += 1
        oid = f"ORD-{self.seq}"
        self.orders[oid] = dict(payload)
        return oid

    def update_order(self, order_id, update):
        self.orders.setdefault(order_id, {})
        self.orders[order_id].update(update)

    def insert_audit_event(self, event):
        self.audit_events.append(event)

    def load_pending_customer_refs(self):
        return []

    def load_pending_correlation_ids(self):
        return []

    def order_exists_inflight(self, *, customer_ref, correlation_id):
        return False


class InlineExecutor:
    def is_ready(self):
        return True

    def submit(self, _name, fn):
        return fn()


class SafeModeOn:
    def is_enabled(self):
        return True

    def is_ready(self):
        return True


class RiskBlocker:
    """Risk middleware that blocks all orders."""
    def check(self, payload):
        return {"allowed": False, "reason": "RISK_BLOCKED", "payload": payload}

    def is_ready(self):
        return True


class RiskMutator:
    """Risk middleware that mutates stake to 12.5."""
    def check(self, payload):
        out = dict(payload)
        out["stake"] = 12.5
        return {"allowed": True, "reason": None, "payload": out}

    def is_ready(self):
        return True


class CapturingOrderManager:
    def __init__(self):
        self.payloads = []

    def submit(self, payload):
        self.payloads.append(dict(payload))
        return {"ok": True, "bet_id": "BET-RISK-1"}


@pytest.mark.unit
@pytest.mark.failure
def test_safe_mode_blocks_order():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    engine = TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
        safe_mode=SafeModeOn(),
    )

    result = engine.submit_quick_bet(
        {
            "market_id": "1.400",
            "selection_id": 44,
            "price": 2.0,
            "stake": 5.0,
            "side": "BACK",
            "customer_ref": "SM1",
        }
    )

    assert result["ok"] is False
    assert result["status"] == "DENIED"
    assert result["reason"] == "SAFE_MODE_ACTIVE"


@pytest.mark.unit
@pytest.mark.failure
def test_risk_middleware_can_block_order():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    engine = TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
        risk_middleware=RiskBlocker(),
    )

    result = engine.submit_quick_bet(
        {
            "market_id": "1.401",
            "selection_id": 45,
            "price": 2.0,
            "stake": 5.0,
            "side": "BACK",
            "customer_ref": "RB1",
        }
    )

    assert result["ok"] is False
    assert result["status"] == "DENIED"


@pytest.mark.unit
@pytest.mark.invariant
def test_risk_middleware_can_mutate_payload_before_async_order():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    order_manager = CapturingOrderManager()

    engine = TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
        risk_middleware=RiskMutator(),
    )
    engine.order_manager = order_manager

    result = engine.submit_quick_bet(
        {
            "market_id": "1.402",
            "selection_id": 46,
            "price": 2.0,
            "stake": 5.0,
            "side": "BACK",
            "customer_ref": "RM1",
        }
    )

    assert result["ok"] is True
    assert result["status"] == "ACCEPTED_FOR_PROCESSING"

    assert len(order_manager.payloads) == 1
    assert order_manager.payloads[0]["stake"] == 12.5
