from __future__ import annotations

import threading

from core.duplication_guard import DuplicationGuard


def test_duplication_guard_blocks_duplicate_action_id_and_sequence_for_copy_events():
    guard = DuplicationGuard(ttl_seconds=120)

    payload = {
        "market_id": "1.333",
        "selection_id": "100",
        "bet_type": "BACK",
        "source": "copy",
        "action_id": "ACT-1",
        "action_seq": 7,
    }
    key = f"{guard.build_event_key(payload)}:{payload['action_id']}:{payload['action_seq']}"

    assert guard.acquire(key) is True
    assert guard.acquire(key) is False


def test_duplication_guard_deduplicates_concurrent_identical_copy_events():
    guard = DuplicationGuard(ttl_seconds=120)
    key = "1.333:100:BACK:copy:ACT-77:12"
    barrier = threading.Barrier(8)
    outcomes = []

    def worker():
        barrier.wait()
        outcomes.append(guard.acquire(key))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert outcomes.count(True) == 1
    assert outcomes.count(False) == 7
