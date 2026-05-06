import pytest

from auto_throttle import AutoThrottle
from executor_manager import ExecutorManager
from tick_dispatcher import TickData, TickDispatcher


@pytest.mark.unit
def test_tick_valid_path_kept():
    dispatcher = TickDispatcher()
    seen = []
    dispatcher.register_storage_callback(lambda tick: seen.append(tick.market_id))
    dispatcher.dispatch_tick(TickData(market_id="1.1", selection_id=1, timestamp=1.0))
    assert seen == ["1.1"]


@pytest.mark.unit
def test_tick_invalid_is_counted():
    dispatcher = TickDispatcher()
    dispatcher.dispatch_tick(None)
    stats = dispatcher.get_stats()
    assert stats["invalid_ticks"] == 1


@pytest.mark.unit
def test_throttle_update_returns_bool():
    throttle = AutoThrottle(max_calls=1, period=60)
    changed = throttle.update(api_calls_min=120)
    assert changed is True
    assert throttle.max_calls == 120
    bad = throttle.update(api_calls_min="x")
    assert bad is False


@pytest.mark.unit
def test_throttle_limited_explicit_false():
    throttle = AutoThrottle(max_calls=1, period=60)
    assert throttle.allow_call() is True
    assert throttle.allow_call() is False


@pytest.mark.unit
def test_exec_repeat_shutdown_stable():
    manager = ExecutorManager(max_workers=1)
    manager.shutdown(wait=True)
    manager.shutdown(wait=True)
    assert manager.status()["shutdown"] is True


@pytest.mark.unit
def test_exec_submit_after_shutdown_raises():
    manager = ExecutorManager(max_workers=1)
    manager.shutdown(wait=True)
    with pytest.raises(RuntimeError):
        manager.submit("x", lambda: 1)


@pytest.mark.unit
def test_exec_future_error_explicit():
    manager = ExecutorManager(max_workers=1)
    fut = manager.submit("boom", lambda: (_ for _ in ()).throw(ValueError("err")))
    with pytest.raises(ValueError):
        fut.result(timeout=2)
    manager.shutdown(wait=True)
