import pytest


class DummyBus:
    def __init__(self):
        self.subscriptions = {}
        self.events = []

    def subscribe(self, event_name, handler):
        self.subscriptions[event_name] = handler

    def publish(self, event_name, payload=None):
        self.events.append((event_name, payload))


class DummyDB:
    pass


class DummyExecutor:
    def submit(self, _name, fn, *args, **kwargs):
        return fn(*args, **kwargs)


@pytest.mark.unit
@pytest.mark.guardrail
def test_import_trading_engine():
    from core.trading_engine import TradingEngine  # noqa: F401


@pytest.mark.unit
@pytest.mark.guardrail
def test_constructor_smoke_and_bus_subscription():
    from core.trading_engine import TradingEngine

    bus = DummyBus()
    engine = TradingEngine(
        bus=bus,
        db=DummyDB(),
        client_getter=lambda: None,
        executor=DummyExecutor(),
    )

    assert engine is not None
    assert "CMD_QUICK_BET" in bus.subscriptions
    assert "REQ_QUICK_BET" in bus.subscriptions
    assert "RECONCILE_NOW" in bus.subscriptions
    assert "RECOVER_PENDING" in bus.subscriptions


@pytest.mark.unit
@pytest.mark.guardrail
def test_submit_quick_bet_alias_calls_main_handler():
    from core.trading_engine import TradingEngine

    bus = DummyBus()
    engine = TradingEngine(
        bus=bus,
        db=DummyDB(),
        client_getter=lambda: None,
        executor=DummyExecutor(),
    )

    result = engine.submit_quick_bet(
        {
            "market_id": "1.100",
            "selection_id": 11,
            "price": 2.0,
            "stake": 5.0,
            "side": "BACK",
        }
    )

    assert isinstance(result, dict)
    assert "ok" in result