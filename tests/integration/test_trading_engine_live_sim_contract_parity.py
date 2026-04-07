import pytest

from core.trading_engine import TradingEngine


class _Bus:
    def __init__(self):
        self.subscriptions = {}
        self.events = []

    def subscribe(self, topic, handler):
        self.subscriptions[topic] = handler

    def publish(self, topic, payload=None):
        self.events.append((topic, payload or {}))


class _DB:
    def __init__(self):
        self.orders = {}
        self.audit = []
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

    def insert_audit_event(self, event):
        self.audit.append(dict(event or {}))

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


class _OM:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def place_order(self, payload):
        self.calls.append(dict(payload))
        return dict(self.response)


def _run(simulation_mode, response):
    bus, db = _Bus(), _DB()
    engine = TradingEngine(bus=bus, db=db, client_getter=lambda: None, executor=_Executor())
    om = _OM(response)
    engine.order_manager = om

    payload = {
        "market_id": "1.100",
        "selection_id": 123,
        "price": 2.2,
        "stake": 8.0,
        "side": "BACK",
        "customer_ref": "REF-PARITY",
        "event_key": "evt:123",
        "simulation_mode": simulation_mode,
        "order_origin": "COPY",
        "copy_meta": {"master_id": "m1", "copy_mode": "mirror"},
    }
    result = engine.submit_quick_bet(payload)
    return result, om.calls[0], db.audit


@pytest.mark.integration
@pytest.mark.parametrize(
    "response,semantic",
    [
        ({"ok": True, "status": "PLACED", "reason_code": "PLACED_OK"}, "inflight"),
        ({"ok": False, "status": "FAILED", "reason_code": "BROKER_REJECTED", "error": "x"}, "fail"),
        ({"ok": False, "status": "AMBIGUOUS", "reason_code": "UNKNOWN", "error_class": "AMBIGUOUS"}, "ambiguous"),
    ],
)
def test_trading_engine_live_sim_contract_parity(response, semantic):
    live_result, live_call, live_audit = _run(False, response)
    sim_result, sim_call, sim_audit = _run(True, response)

    assert set(live_result.keys()) == set(sim_result.keys())
    assert (live_result["status"] == "ACCEPTED_FOR_PROCESSING") == (sim_result["status"] == "ACCEPTED_FOR_PROCESSING")
    assert live_result.get("reason") == sim_result.get("reason")
    assert isinstance(live_result.get("audit"), dict) and isinstance(sim_result.get("audit"), dict)
    assert isinstance(live_result["audit"].get("events"), list) and isinstance(sim_result["audit"].get("events"), list)

    assert live_call["copy_meta"] == sim_call["copy_meta"]
    assert live_call.get("pattern_meta") == sim_call.get("pattern_meta")
    assert live_call["order_origin"] == sim_call["order_origin"] == "COPY"

    # semantic status class parity (ack / terminal)
    assert live_result["is_terminal"] == sim_result["is_terminal"]
    if semantic == "inflight":
        assert live_result["lifecycle_stage"] == sim_result["lifecycle_stage"] == "accepted"
    else:
        assert live_result["lifecycle_stage"] == sim_result["lifecycle_stage"]

    # audit shape parity (do not require exact timestamps)
    assert len(live_audit) == len(sim_audit)
    assert [sorted(row.keys()) for row in live_audit] == [sorted(row.keys()) for row in sim_audit]
