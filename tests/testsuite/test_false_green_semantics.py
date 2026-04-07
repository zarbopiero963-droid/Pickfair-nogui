import pytest

from core.event_bus import EventBus


@pytest.mark.testsuite
def test_shutdown_semantics_are_explicit_and_not_false_green():
    bus = EventBus(workers=1)
    captured = []

    bus.subscribe("evt", lambda payload: captured.append(payload))
    bus.publish("evt", "x")
    bus.stop()

    assert captured == ["x"], "Default shutdown must drain queued events"


@pytest.mark.testsuite
def test_lossy_mode_requires_explicit_opt_in():
    bus = EventBus(workers=1)
    assert hasattr(bus, "stop_lossy")
    assert callable(bus.stop_lossy)
