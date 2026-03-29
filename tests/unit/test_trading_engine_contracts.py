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
    pass


class InlineExecutor:
    def submit(self, _name, fn=None, *args, **kwargs):
        if fn is None and callable(_name):
            return _name(*args, **kwargs)
        return fn(*args, **kwargs)


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
    assert result["dedup_key"] == key