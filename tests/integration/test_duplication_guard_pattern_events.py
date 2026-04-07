from __future__ import annotations

import threading

from core.duplication_guard import DuplicationGuard


def test_pattern_trigger_keys_are_deduplicated():
    guard = DuplicationGuard(ttl_seconds=120)
    payload = {
        "market_id": "1.444",
        "selection_id": "200",
        "bet_type": "LAY",
        "source": "pattern",
    }
    key = guard.build_event_key(payload)

    assert guard.acquire(key) is True
    assert guard.acquire(key) is False


def test_no_duplicate_pattern_execution_under_concurrency():
    guard = DuplicationGuard(ttl_seconds=120)
    key = "1.444:200:LAY:pattern"
    barrier = threading.Barrier(6)
    executions = []

    def worker():
        barrier.wait()
        executions.append(guard.acquire(key))

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert executions.count(True) == 1
    assert executions.count(False) == 5
