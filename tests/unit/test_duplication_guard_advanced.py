import threading

import pytest

from core.duplication_guard import DuplicationGuard


def _key_for_payload(guard):
    return guard.build_event_key(
        {
            "market_id": "1.100",
            "selection_id": 200,
            "bet_type": "BACK",
            "strategy": "telegram",
        }
    )


@pytest.mark.core
@pytest.mark.invariant
def test_duplication_guard_key_is_stable_for_equivalent_payloads():
    guard = DuplicationGuard()

    p1 = {"market_id": "1.1", "selection_id": 9, "bet_type": "back"}
    p2 = {"marketId": "1.1", "selectionId": 9, "side": "BACK"}

    k1 = guard.build_event_key(p1)
    k2 = guard.build_event_key(p2)

    assert k1.startswith("1.1:9:BACK"), "La chiave deve normalizzare snake/camel e lato"
    assert k2.startswith("1.1:9:BACK"), "La chiave deve normalizzare snake/camel e lato"


@pytest.mark.core
@pytest.mark.concurrency
@pytest.mark.invariant
def test_duplication_guard_only_one_thread_can_acquire_same_key():
    guard = DuplicationGuard()
    key = _key_for_payload(guard)
    results = []

    def worker():
        results.append(guard.acquire(key))

    threads = []
    for _ in range(10):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    assert results.count(True) == 1, "Solo un thread deve acquisire la stessa key"
    assert results.count(False) == 9, "Gli altri thread devono essere bloccati come duplicati"


@pytest.mark.core
@pytest.mark.invariant
def test_duplication_guard_release_unlocks_key_again():
    guard = DuplicationGuard()
    key = _key_for_payload(guard)

    guard.register(key)
    assert guard.is_duplicate(key) is True
    guard.release(key)
    assert guard.is_duplicate(key) is False


@pytest.mark.core
@pytest.mark.invariant
def test_duplication_guard_clear_resets_state():
    guard = DuplicationGuard()

    guard.register("a")
    guard.register("b")

    guard.clear()
    snap = guard.snapshot()

    assert snap["active_count"] == 0
    assert snap["active_keys"] == []


@pytest.mark.core
@pytest.mark.invariant
def test_register_and_is_duplicate_match_acquire_semantics():
    """
    register() + is_duplicate() deve dare stesso risultato di acquire() sulla stessa chiave.
    Verifica che le due interfacce leggano lo stesso stato.
    """
    guard = DuplicationGuard()
    key = "3.3:30:BACK"

    # Via register/is_duplicate
    assert guard.is_duplicate(key) is False
    guard.register(key)
    assert guard.is_duplicate(key) is True
    guard.release(key)

    # Via acquire
    assert guard.acquire(key) is True   # not duplicate → can proceed
    assert guard.acquire(key) is False  # duplicate → blocked
    assert guard.is_duplicate(key) is True  # cross-check
    guard.release(key)
    assert guard.is_duplicate(key) is False
