import pytest

from auto_throttle import AutoThrottle


class FakeLegacyFlow:
    def __init__(self):
        self.throttle = AutoThrottle(max_calls=1, period=60.0)
        self.sent = []

    def trigger(self, payload):
        if not self.throttle.allow_call():
            return {"status": "BLOCKED", "payload": payload}
        self.sent.append(payload)
        return {"status": "SENT", "payload": payload}


@pytest.mark.e2e
def test_legacy_flow_blocks_second_immediate_trigger():
    flow = FakeLegacyFlow()

    first = flow.trigger({"id": 1})
    second = flow.trigger({"id": 2})

    assert first["status"] == "SENT"
    assert second["status"] == "BLOCKED"
    assert len(flow.sent) == 1