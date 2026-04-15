from __future__ import annotations

from core.duplication_guard import DuplicationGuard


class FakeStore:
    def __init__(self):
        self.keys = set()

    def save(self, key):
        self.keys.add(key)

    def load(self):
        return sorted(self.keys)


def _boot_guard_from_store(store: FakeStore) -> DuplicationGuard:
    guard = DuplicationGuard(ttl_seconds=999999)
    for key in store.load():
        guard.acquire(key)
    return guard


def test_pattern_trigger_is_not_reexecuted_after_restart():
    store = FakeStore()
    payload = {
        "market_id": "1.100",
        "selection_id": "22",
        "bet_type": "BACK",
        "source": "pattern",
    }

    guard_before = _boot_guard_from_store(store)
    key = guard_before.build_event_key(payload)
    assert guard_before.acquire(key) is True
    store.save(key)

    guard_after = _boot_guard_from_store(store)
    assert guard_after.acquire(key) is False


def test_duplication_guard_derives_copy_pattern_strategy_from_metadata_when_source_missing():
    guard = DuplicationGuard(ttl_seconds=999999)
    base = {"market_id": "1.100", "selection_id": "22", "bet_type": "BACK"}

    copy_key = guard.build_event_key({**base, "copy_meta": {"copy_group_id": "CG-7"}})
    pattern_key = guard.build_event_key({**base, "pattern_meta": {"pattern_id": "PT-7"}})

    assert copy_key.endswith(":copy")
    assert pattern_key.endswith(":pattern")
    assert copy_key != pattern_key
