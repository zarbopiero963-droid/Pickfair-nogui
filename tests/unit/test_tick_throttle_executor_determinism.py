"""Focused PR4 deterministic tests for tick/throttle/executor blockers."""

import threading
import unittest
from typing import Any, cast

from auto_throttle import AutoThrottle
from executor_manager import ExecutorManager
from tick_dispatcher import TickData, TickDispatcher, get_tick_dispatcher


class TestPR4Determinism(unittest.TestCase):
    """Review-fix tests for deterministic behavior."""

    def test_tick_valid_kept(self) -> None:
        """Storage callback sees valid ticks."""
        dispatcher = TickDispatcher()
        seen: list[str] = []
        dispatcher.register_storage_callback(lambda tick: seen.append(tick.market_id))
        dispatcher.dispatch_tick(TickData(market_id="1.1", selection_id=1, timestamp=1.0))
        self.assertEqual(seen, ["1.1"])

    def test_tick_invalid_counted(self) -> None:
        """Malformed tick path increments invalid counter."""
        dispatcher = TickDispatcher()
        dispatcher.dispatch_tick(cast(Any, None))
        stats = dispatcher.get_stats()
        self.assertEqual(stats["invalid_ticks"], 1)

    def test_throttle_update_ok(self) -> None:
        """Positive api_calls_min updates max_calls and returns True."""
        throttle = AutoThrottle(max_calls=1, period=60)
        changed = throttle.update(api_calls_min=120)
        self.assertTrue(changed)
        self.assertEqual(throttle.max_calls, 120)

    def test_throttle_update_nonpos(self) -> None:
        """Non-positive api_calls_min is deterministically rejected."""
        throttle = AutoThrottle(max_calls=1, period=60)
        self.assertFalse(throttle.update(api_calls_min=0))
        self.assertFalse(throttle.update(api_calls_min=-5))

    def test_throttle_update_unblock(self) -> None:
        """Successful update unblocks limiter."""
        throttle = AutoThrottle(max_calls=1, period=60)
        throttle._blocked = True
        self.assertFalse(throttle.allow_call())
        self.assertTrue(throttle.update(api_calls_min=60))
        self.assertFalse(throttle.is_blocked())
        self.assertTrue(throttle.allow_call())

    def test_exec_shutdown_repeat(self) -> None:
        """Repeated shutdown remains deterministic."""
        manager = ExecutorManager(max_workers=1)
        manager.shutdown(wait=True)
        manager.shutdown(wait=True)
        self.assertTrue(manager.status()["shutdown"])

    def test_exec_sub_after_down(self) -> None:
        """Submit-after-shutdown raises RuntimeError."""
        manager = ExecutorManager(max_workers=1)
        manager.shutdown(wait=True)
        with self.assertRaises(RuntimeError):
            manager.submit("x", lambda: 1)

    def test_exec_fast_named_wait(self) -> None:
        """Named fast future remains waitable after completion."""
        manager = ExecutorManager(max_workers=1)
        future = manager.submit("fast", lambda: 7)
        self.assertEqual(future.result(timeout=2), 7)
        self.assertEqual(manager.wait("fast", timeout=2), 7)
        manager.shutdown(wait=True)

    def test_exec_error_explicit(self) -> None:
        """Future exceptions propagate deterministically."""
        manager = ExecutorManager(max_workers=1)
        future = manager.submit("boom", lambda: (_ for _ in ()).throw(ValueError("err")))
        with self.assertRaises(ValueError):
            future.result(timeout=2)
        manager.shutdown(wait=True)

    def test_tick_ui_auto_indep(self) -> None:
        """UI and automation pending buffers do not clear each other."""
        dispatcher = TickDispatcher()
        dispatcher._last_ui_update = 0.0
        import time
        dispatcher._last_automation_check = time.time()
        ui_seen: list[int] = []
        auto_seen: list[int] = []
        dispatcher.register_ui_callback(lambda ticks: ui_seen.append(len(ticks)))
        dispatcher.register_automation_callback(lambda ticks: auto_seen.append(len(ticks)))
        dispatcher.dispatch_tick(TickData(market_id="1.2", selection_id=9, timestamp=1.0))
        self.assertEqual(ui_seen, [1])
        self.assertEqual(auto_seen, [])
        dispatcher._last_automation_check = 0.0
        dispatcher.dispatch_tick(TickData(market_id="1.2", selection_id=9, timestamp=2.0))
        self.assertEqual(auto_seen, [1])

    def test_singleton_serialized(self) -> None:
        """Singleton init is guarded and returns one instance."""
        instances: list[TickDispatcher] = []

        def _target() -> None:
            instances.append(get_tick_dispatcher())

        t1 = threading.Thread(target=_target)
        t2 = threading.Thread(target=_target)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertEqual(len(instances), 2)
        self.assertIs(instances[0], instances[1])


if __name__ == "__main__":
    unittest.main()
