from core.duplication_guard import DuplicationGuard


def test_build_event_key_prefers_snake_case_fields():
    guard = DuplicationGuard()

    key = guard.build_event_key(
        {
            "market_id": "1.234",
            "selection_id": 99,
            "bet_type": "lay",
        }
    )

    assert key == "1.234:99:LAY", "build_event_key deve usare market_id/selection_id/bet_type e normalizzare il lato"


def test_build_event_key_supports_camel_case_aliases():
    guard = DuplicationGuard()

    key = guard.build_event_key(
        {
            "marketId": "1.777",
            "selectionId": 12,
            "side": "back",
        }
    )

    assert key == "1.777:12:BACK", "build_event_key deve supportare anche gli alias camelCase"


def test_register_then_is_duplicate_then_release():
    guard = DuplicationGuard()
    key = "1.2:55:BACK"

    assert guard.is_duplicate(key) is False, "una chiave nuova non deve risultare duplicata"

    guard.register(key)
    assert guard.is_duplicate(key) is True, "register deve segnare la chiave come attiva"

    guard.release(key)
    assert guard.is_duplicate(key) is False, "release deve rimuovere la chiave attiva"


def test_register_ignores_empty_key():
    guard = DuplicationGuard()

    guard.register("")
    guard.register(None)

    snapshot = guard.snapshot()
    assert snapshot["active_count"] == 0, "chiavi vuote non devono essere registrate"


def test_clear_removes_everything():
    guard = DuplicationGuard()
    guard.register("a")
    guard.register("b")

    guard.clear()

    snapshot = guard.snapshot()
    assert snapshot["active_count"] == 0, "clear deve svuotare tutte le chiavi attive"
    assert snapshot["active_keys"] == [], "clear deve svuotare anche il dettaglio snapshot"


def test_snapshot_contains_registered_keys_and_timestamps():
    guard = DuplicationGuard()
    guard.register("m1:s1:BACK")
    guard.register("m2:s2:LAY")

    snapshot = guard.snapshot()

    assert snapshot["active_count"] == 2, "snapshot deve riportare il numero corretto di chiavi attive"

    keys = {item["event_key"] for item in snapshot["active_keys"]}
    assert keys == {"m1:s1:BACK", "m2:s2:LAY"}, "snapshot deve riportare tutte le chiavi registrate"

    for item in snapshot["active_keys"]:
        assert item["registered_at"], "ogni chiave registrata deve avere un timestamp"