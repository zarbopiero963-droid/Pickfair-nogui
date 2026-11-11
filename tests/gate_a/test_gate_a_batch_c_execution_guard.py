"""Gate A batch C: core/execution_guard.py (ExecutionGuard acquire/release)."""
from __future__ import annotations

import threading

from core.execution_guard import ExecutionGuard


def test_trg_628_b_0972():
    """ID: TRG-628-B-0972 / PF-B1-R2-GUARD-ACQUIRE

    Under the internal threading.Lock only the first acquire of a key is
    admitted; every later acquire of the same key (also concurrent) is refused,
    while a different key is independent.
    """
    guard = ExecutionGuard()
    assert guard.acquire("order-1") is True
    assert guard.acquire("order-1") is False
    assert guard.acquire("order-1") is False
    assert guard.acquire("order-2") is True
    assert sorted(guard.snapshot()) == ["order-1", "order-2"]

    # Concurrent contenders on a fresh key: exactly one winner.
    guard2 = ExecutionGuard()
    barrier = threading.Barrier(16)
    results = []
    lock = threading.Lock()

    def _contend():
        barrier.wait()
        ok = guard2.acquire("same-key")
        with lock:
            results.append(ok)

    threads = [threading.Thread(target=_contend) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(5)
    assert results.count(True) == 1
    assert results.count(False) == 15


def test_trg_628_b_0973():
    """ID: TRG-628-B-0973 / PF-B1-R2-GUARD-RELEASE

    release() discards the key under the lock: the key becomes acquirable
    again; releasing an unknown key is a silent no-op (discard, not remove).
    """
    guard = ExecutionGuard()
    assert guard.acquire("k") is True
    guard.release("k")
    assert guard.snapshot() == []
    assert guard.acquire("k") is True
    guard.release("never-acquired")          # discard semantics: no KeyError
    assert guard.snapshot() == ["k"]
