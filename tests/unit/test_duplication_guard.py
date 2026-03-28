import pytest

from core.duplication_guard import DuplicationGuard


def _register_or_acquire(guard, key):
    if hasattr(guard, "register"):
        guard.register(key)
        return True
    if hasattr(guard, "acquire"):
        return guard.acquire(key)
    raise AssertionError("DuplicationGuard non espone né register né acquire")


def _is_duplicate(guard, key):
    if hasattr(guard, "is_duplicate"):
        return guard.is_duplicate(key)
    if hasattr(guard, "acquire"):
        first = guard.acquire(key)
        if first:
            guard.release(key)
            return False
        return True
    raise AssertionError("DuplicationGuard non espone un metodo per verificare duplicati")


@pytest.mark.unit
@pytest.mark.guardrail
def test_build_event_key_prefers_snake_case_fields():
    guard = DuplicationGuard()

    key = guard.build_event_key(
        {
            "market_id": "1.234",
            "selection_id": 99,
            "bet_type": "lay",
        }
    )

    assert key.startswith("1.234:99:LAY"), "build_event_key deve normalizzare i campi principali"


@pytest.mark.unit
@pytest.mark.guardrail
def test_build_event_key_supports_camel_case_aliases():
    guard = DuplicationGuard()

    key = guard.build_event_key(
        {
            "marketId": "1.777",
            "selectionId": 12,
            "side": "back",
        }
    )

    assert key.startswith("1.777:12:BACK"), "build_event_key deve supportare alias camelCase"


@pytest.mark.unit
@pytest.mark.guardrail
def test_register_then_is_duplicate_then_release():
    guard = DuplicationGuard()
    key = "1.2:55:BACK"

    assert _is_duplicate(guard, key) is False, "una chiave nuova non deve risultare duplicata"

    _register_or_acquire(guard, key)
    assert _is_duplicate(guard, key) is True, "la chiave registrata deve risultare duplicata"

    guard.release(key)
    assert _is_duplicate(guard, key) is False, "release deve rimuovere la chiave attiva"


@pytest.mark.unit
@pytest.mark.guardrail
def test_register_ignores_empty_key():
    guard = DuplicationGuard()

    if hasattr(guard, "register"):
        guard.register("")
        guard.register(None)
    else:
        assert guard.acquire("") is False
        assert guard.acquire(None) is False

    snapshot = guard.snapshot()
    assert snapshot["active_count"] == 0, "chiavi vuote non devono essere registrate"


@pytest.mark.unit
@pytest.mark.guardrail
def test_clear_removes_everything():
    guard = DuplicationGuard()
    _register_or_acquire(guard, "a")
    _register_or_acquire(guard, "b")

    guard.clear()

    snapshot = guard.snapshot()
    assert snapshot["active_count"] == 0, "clear deve svuotare tutte le chiavi attive"
    assert snapshot["active_keys"] == [], "clear deve svuotare il dettaglio snapshot"


@pytest.mark.unit
@pytest.mark.guardrail
def test_snapshot_contains_registered_keys_and_timestamps():
    guard = DuplicationGuard()
    _register_or_acquire(guard, "m1:s1:BACK")
    _register_or_acquire(guard, "m2:s2:LAY")

    snapshot = guard.snapshot()

    assert snapshot["active_count"] == 2, "snapshot deve riportare il numero corretto di chiavi attive"

    keys = {item["event_key"] for item in snapshot["active_keys"]}
    assert keys == {"m1:s1:BACK", "m2:s2:LAY"}, "snapshot deve riportare tutte le chiavi"

    for item in snapshot["active_keys"]:
        assert item["registered_at"], "ogni chiave registrata deve avere un timestamp"