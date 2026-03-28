import pytest

from auto_throttle import AutoThrottle


class LegacyCaller:
    def __init__(self, throttle):
        self.throttle = throttle
        self.executed = 0

    def click(self):
        if self.throttle.allow_call():
            self.executed += 1
            return True
        return False


@pytest.mark.integration
def test_legacy_caller_uses_auto_throttle_to_gate_calls():
    throttle = AutoThrottle(max_calls=2, period=60.0)
    caller = LegacyCaller(throttle)

    assert caller.click() is True
    assert caller.click() is True
    assert caller.click() is False
    assert caller.executed == 2