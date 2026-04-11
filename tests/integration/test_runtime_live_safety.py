import pytest

from core.trading_engine import TradingEngine
from tests.helpers.fake_exchange import FakeExchange


class _Bus:
    def __init__(self):
        self.subscriptions = {}

    def subscribe(self, topic, handler):
        self.subscriptions[topic] = handler

    def publish(self, _topic, _payload=None):
        return None


class _DB:
    def __init__(self):
        self.orders = {}
        self.seq = 0

    def is_ready(self):
        return True

    def insert_order(self, payload):
        self.seq += 1
        oid = f"ORD-{self.seq}"
        self.orders[oid] = dict(payload)
        return oid

    def update_order(self, order_id, update):
        self.orders.setdefault(order_id, {}).update(dict(update or {}))

    def get_order(self, order_id):
        return self.orders.get(order_id)

    def insert_audit_event(self, _event):
        return None

    def load_pending_customer_refs(self):
        return []

    def load_pending_correlation_ids(self):
        return []

    def order_exists_inflight(self, **_kwargs):
        return False


class _Executor:
    def is_ready(self):
        return True

    def submit(self, _name, fn):
        return fn()


class _RuntimeController:
    def __init__(self, mode="SIMULATION", allowed=False):
        self.mode = mode
        self.allowed = allowed

    def get_effective_execution_mode(self):
        return self.mode

    def is_live_allowed(self):
        return self.allowed


class _SimulationBroker:
    def __init__(self):
        self.exchange = FakeExchange()
        self.calls = 0

    def execute(self, payload):
        self.calls += 1
        mapped = {
            "customer_ref": payload["customer_ref"],
            "market_id": payload["market_id"],
            "selection_id": payload["selection_id"],
            "side": payload.get("side") or payload.get("bet_type") or "BACK",
            "price": payload["price"],
            "size": payload.get("size", payload.get("stake")),
        }
        return self.exchange.place_order(mapped)


class _LiveClient:
    def __init__(self, *, fail=False):
        self.exchange = FakeExchange()
        self.calls = 0
        self.fail = fail

    def place_order(self, payload):
        self.calls += 1
        if self.fail:
            raise RuntimeError("live client failure")
        mapped = {
            "customer_ref": payload["customer_ref"],
            "market_id": payload["market_id"],
            "selection_id": payload["selection_id"],
            "side": payload.get("side") or payload.get("bet_type") or "BACK",
            "price": payload["price"],
            "size": payload.get("size", payload.get("stake")),
        }
        return self.exchange.place_order(mapped)


def _make_engine(runtime, sim_broker, live_client):
    engine = TradingEngine(
        bus=_Bus(),
        db=_DB(),
        client_getter=lambda: None,
        executor=_Executor(),
    )
    engine.runtime_controller = runtime
    engine.simulation_broker = sim_broker
    engine.betfair_client = live_client
    return engine


def _submit(engine, customer_ref):
    return engine.submit_quick_bet(
        {
            "customer_ref": customer_ref,
            "market_id": "1.100",
            "selection_id": 100,
            "price": 2.0,
            "stake": 5.0,
            "side": "BACK",
        }
    )


@pytest.mark.integration
def test_live_blocked_never_calls_real_execution():
    runtime = _RuntimeController(mode="SIMULATION", allowed=False)
    sim_broker = _SimulationBroker()
    live_client = _LiveClient()
    engine = _make_engine(runtime, sim_broker, live_client)

    result = _submit(engine, "REF-SAFE-1")

    assert result["status"] == "ACCEPTED_FOR_PROCESSING"
    assert sim_broker.calls == 1
    assert live_client.calls == 0


@pytest.mark.integration
def test_forced_live_bypass_is_still_blocked():
    runtime = _RuntimeController(mode="LIVE", allowed=False)
    sim_broker = _SimulationBroker()
    live_client = _LiveClient()
    engine = _make_engine(runtime, sim_broker, live_client)

    result = _submit(engine, "REF-SAFE-2")

    assert result["status"] == "FAILED"
    assert "LIVE_EXECUTION_BLOCKED" in str(result.get("error") or "")
    assert sim_broker.calls == 0
    assert live_client.calls == 0


@pytest.mark.integration
def test_live_exception_is_handled_safely():
    runtime = _RuntimeController(mode="LIVE", allowed=True)
    sim_broker = _SimulationBroker()
    live_client = _LiveClient(fail=True)
    engine = _make_engine(runtime, sim_broker, live_client)

    result = _submit(engine, "REF-SAFE-3")

    assert result["status"] == "FAILED"
    assert live_client.calls == 1


@pytest.mark.integration
def test_no_accidental_live_execution_when_simulation_mode():
    runtime = _RuntimeController(mode="SIMULATION", allowed=False)
    sim_broker = _SimulationBroker()
    live_client = _LiveClient()
    engine = _make_engine(runtime, sim_broker, live_client)

    for idx in range(3):
        res = _submit(engine, f"REF-SAFE-SIM-{idx}")
        assert res["status"] == "ACCEPTED_FOR_PROCESSING"

    assert sim_broker.calls == 3
    assert live_client.calls == 0
