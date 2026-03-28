import threading
import time

import pytest

from core.duplication_guard import DuplicationGuard


@pytest.mark.core
@pytest.mark.chaos
@pytest.mark.concurrency
def test_duplication_guard_massive_competition_same_key():
    guard = DuplicationGuard()

    if not hasattr(guard, "acquire"):
        pytest.skip("Chaos test richiede DuplicationGuard con acquire atomico")

    key = guard.build_event_key(
        {
            "market_id": "1.900",
            "selection_id": 99,
            "bet_type": "BACK",
            "strategy": "stress",
        }
    )

    results = []
    lock = threading.Lock()

    def worker():
        acquired = guard.acquire(key)
        with lock:
            results.append(acquired)

    threads = [threading.Thread(target=worker) for _ in range(50)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(True) == 1, "Anche con alta competizione, una sola acquire deve passare"
    assert results.count(False) == 49, "Tutti gli altri thread devono essere bloccati come duplicati"


@pytest.mark.core
@pytest.mark.chaos
@pytest.mark.invariant
def test_duplication_guard_repeated_release_does_not_break_state():
    guard = DuplicationGuard()

    if hasattr(guard, "acquire"):
        assert guard.acquire("X") is True
    else:
        guard.register("X")

    for _ in range(20):
        guard.release("X")

    snap = guard.snapshot()
    keys = {item["event_key"] for item in snap["active_keys"]}
    assert "X" not in keys, "Release ripetuti non devono lasciare stato sporco"


@pytest.mark.core
@pytest.mark.chaos
@pytest.mark.invariant
def test_duplication_guard_many_keys_clear_resets_everything():
    guard = DuplicationGuard()

    if hasattr(guard, "acquire"):
        for i in range(200):
            guard.acquire(f"K{i}")
    else:
        for i in range(200):
            guard.register(f"K{i}")

    snap_before = guard.snapshot()
    assert snap_before["active_count"] == 200

    guard.clear()

    snap_after = guard.snapshot()
    assert snap_after["active_count"] == 0
    assert snap_after["active_keys"] == []