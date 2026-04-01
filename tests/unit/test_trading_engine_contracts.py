import pytest


class FakeBus:
    def __init__(self):
        self.subscriptions = {}
        self.events = []

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


@pytest.mark.unit
@pytest.mark.guardrail
def test_engine_subscribes_expected_events():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    names = set(bus.subscriptions.keys())

    assert "CMD_QUICK_BET" in names
    assert "REQ_QUICK_BET" in names
    assert "RECONCILE_NOW" in names
    assert "RECOVER_PENDING" in names


@pytest.mark.unit
@pytest.mark.guardrail
def test_quick_bet_result_contract_on_failure():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    engine = TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    result = engine.submit_quick_bet({"market_id": "1.1"})

    assert result["ok"] is False
    assert result["status"] == "FAILED"
    assert "error" in result


@pytest.mark.unit
@pytest.mark.guardrail
def test_quick_bet_duplicate_contract_shape():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    engine = TradingEngine(
        bus=bus,
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    key = "dup-key"
    with engine._lock:
        engine._inflight_keys.add(key)

    result = engine.submit_quick_bet(
        {
            "market_id": "1.100",
            "selection_id": 11,
            "price": 2.0,
            "stake": 5.0,
            "side": "BACK",
            "customer_ref": key,
        }
    )

    assert result["ok"] is True
    assert result["status"] == "DUPLICATE_BLOCKED"
    assert result["reason"] == "DUPLICATE_BLOCKED"
    assert result["error"] is None
    assert result["ambiguity_reason"] is None
