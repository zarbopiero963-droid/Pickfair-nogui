import pytest

from core.duplication_guard import DuplicationGuard


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

    assert guard.is_duplicate(key) is False, "una chiave nuova non deve risultare duplicata"

    guard.register(key)
    assert guard.is_duplicate(key) is True, "la chiave registrata deve risultare duplicata"

    guard.release(key)
    assert guard.is_duplicate(key) is False, "release deve rimuovere la chiave attiva"


@pytest.mark.unit
@pytest.mark.guardrail
def test_register_ignores_empty_key():
    guard = DuplicationGuard()

    guard.register("")
    guard.register(None)

    snapshot = guard.snapshot()
    assert snapshot["active_count"] == 0, "chiavi vuote non devono essere registrate"


@pytest.mark.unit
@pytest.mark.guardrail
def test_clear_removes_everything():
    guard = DuplicationGuard()
    guard.register("a")
    guard.register("b")

    guard.clear()

    snapshot = guard.snapshot()
    assert snapshot["active_count"] == 0, "clear deve svuotare tutte le chiavi attive"
    assert snapshot["active_keys"] == [], "clear deve svuotare il dettaglio snapshot"


@pytest.mark.unit
@pytest.mark.guardrail
def test_snapshot_contains_registered_keys_and_timestamps():
    guard = DuplicationGuard()
    guard.register("m1:s1:BACK")
    guard.register("m2:s2:LAY")

    snapshot = guard.snapshot()

    assert snapshot["active_count"] == 2, "snapshot deve riportare il numero corretto di chiavi attive"

    keys = {item["event_key"] for item in snapshot["active_keys"]}
    assert keys == {"m1:s1:BACK", "m2:s2:LAY"}, "snapshot deve riportare tutte le chiavi"

    for item in snapshot["active_keys"]:
        assert item["registered_at"], "ogni chiave registrata deve avere un timestamp"


@pytest.mark.unit
@pytest.mark.guardrail
def test_is_duplicate_returns_false_for_unknown_key():
    guard = DuplicationGuard()
    assert guard.is_duplicate("non:esiste:BACK") is False


@pytest.mark.unit
@pytest.mark.guardrail
def test_is_duplicate_empty_key_returns_false():
    guard = DuplicationGuard()
    assert guard.is_duplicate("") is False
    assert guard.is_duplicate(None) is False


@pytest.mark.unit
@pytest.mark.guardrail
def test_register_is_idempotent():
    guard = DuplicationGuard()
    key = "1.1:10:BACK"
    guard.register(key)
    guard.register(key)  # second register must not raise and must not add a second entry

    snapshot = guard.snapshot()
    assert snapshot["active_count"] == 1, "register idempotente non deve duplicare la chiave"


@pytest.mark.unit
@pytest.mark.guardrail
def test_acquire_and_is_duplicate_are_consistent():
    """acquire() e is_duplicate() devono leggere lo stesso stato interno."""
    guard = DuplicationGuard()
    key = "2.2:20:LAY"

    # acquire registers atomically
    assert guard.acquire(key) is True
    # is_duplicate must now agree the key is active
    assert guard.is_duplicate(key) is True

    guard.release(key)
    assert guard.is_duplicate(key) is False
