import pytest

from auto_throttle import AutoThrottle


class LegacyCaller:
    def __init__(self, throttle: AutoThrottle):
        self.throttle = throttle
        self.calls = 0

    def click(self) -> bool:
        if not self.throttle.allow_call():
            return False
        self.calls += 1
        return True


@pytest.mark.integration
def test_legacy_caller_uses_auto_throttle_to_gate_calls():
    throttle = AutoThrottle(max_calls=2, period=60.0)
    caller = LegacyCaller(throttle)

    assert caller.click() is True
    assert caller.click() is True
    assert caller.click() is False
    assert caller.calls == 2


@pytest.mark.integration
def test_auto_throttle_canonical_import_path_contract():
    assert AutoThrottle.__module__ == "auto_throttle"
