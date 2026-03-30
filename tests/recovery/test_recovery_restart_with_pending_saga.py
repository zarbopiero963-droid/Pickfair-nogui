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
        self.sagas = {
            "PEND-1": {
                "customer_ref": "PEND-1",
                "status": "EXECUTING",
                "market_id": "1.200",
                "selection_id": "22",
                "bet_type": "BACK",
                "price": 2.2,
                "stake": 10.0,
            },
            "PEND-2": {
                "customer_ref": "PEND-2",
                "status": "ROLLBACK_REQUIRED",
                "market_id": "1.201",
                "selection_id": "23",
                "bet_type": "LAY",
                "price": 3.0,
                "stake": 8.0,
            },
        }

    def get_pending_sagas(self):
        return list(self.sagas.values())


class InlineExecutor:
    def submit(self, _name, fn=None, *args, **kwargs):
        target = fn if fn is not None else _name
        return target(*args, **kwargs)


@pytest.mark.recovery
def test_restart_restores_inflight_keys_from_pending_sagas():
    from core.trading_engine import TradingEngine

    bus = FakeBus()
    db = FakeDB()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    result = engine.recover_after_restart()

    assert result["ok"] is True
    assert result["status"] == "RECOVERY_TRIGGERED"
    assert result["restored_inflight"] == 2

    with engine._lock:
        assert "PEND-1" in engine._inflight_keys
        assert "PEND-2" in engine._inflight_keys