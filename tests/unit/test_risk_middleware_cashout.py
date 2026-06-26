"""Residui 2.1-A: shape CASHOUT_FAILED (dict) + selection_id>0 in RiskMiddleware (B2.5)."""

from core.risk_middleware import RiskMiddleware


class _Bus:
    def __init__(self):
        self.published = []
        self.subs = []

    def subscribe(self, topic, handler):
        self.subs.append((topic, handler))

    def publish(self, topic, payload=None):
        self.published.append((topic, payload))

    def last(self, topic):
        for t, p in reversed(self.published):
            if t == topic:
                return p
        return None

    def topics(self):
        return [t for t, _ in self.published]


def _payload(**over):
    p = {"market_id": "1.2", "selection_id": 55, "side": "LAY",
         "stake": 4.0, "price": 1.8, "green_up": 2.5}
    p.update(over)
    return p


def test_valid_cashout_forwards_cmd():
    bus = _Bus()
    RiskMiddleware(bus)._handle_cashout(_payload())
    cmd = bus.last("CMD_EXECUTE_CASHOUT")
    assert cmd is not None and cmd["selection_id"] == 55
    assert "CASHOUT_FAILED" not in bus.topics()


def _assert_structured_failed(bus, market_id="1.2"):
    assert "CMD_EXECUTE_CASHOUT" not in bus.topics()
    failed = bus.last("CASHOUT_FAILED")
    assert isinstance(failed, dict)  # non più una stringa (residuo 2.1-A)
    assert failed["status"] == "REJECTED"
    assert failed["bet_id"] is None
    assert failed["matched"] == 0.0
    assert failed["market_id"] == market_id
    assert "reason" in failed


def test_cashout_failed_is_structured_dict_on_malformed():
    bus = _Bus()
    RiskMiddleware(bus)._handle_cashout(_payload(selection_id="abc"))
    _assert_structured_failed(bus)


def test_zero_selection_id_rejected():
    bus = _Bus()
    RiskMiddleware(bus)._handle_cashout(_payload(selection_id=0))
    _assert_structured_failed(bus)


def test_negative_selection_id_rejected():
    bus = _Bus()
    RiskMiddleware(bus)._handle_cashout(_payload(selection_id=-5))
    _assert_structured_failed(bus)
