"""2.1-C: colonna signal_patterns.action (QUICK_BET/CASHOUT/CASHOUT_ALL) + migrazione."""

import sqlite3

import pytest

from database import Database


@pytest.fixture()
def db():
    return Database(":memory:")


def test_save_and_get_action(db):
    db.save_signal_pattern(pattern="x", label="L", action="CASHOUT")
    pats = db.get_signal_patterns()
    assert pats and pats[0]["action"] == "CASHOUT"


def test_default_action_is_quick_bet(db):
    db.save_signal_pattern(pattern="x", label="L")
    assert db.get_signal_patterns()[0]["action"] == "QUICK_BET"


def test_invalid_action_normalized_to_quick_bet(db):
    db.save_signal_pattern(pattern="x", label="L", action="bogus")
    assert db.get_signal_patterns()[0]["action"] == "QUICK_BET"


def test_action_is_case_insensitive(db):
    db.save_signal_pattern(pattern="x", label="L", action="cashout_all")
    assert db.get_signal_patterns()[0]["action"] == "CASHOUT_ALL"


def test_update_action(db):
    pid = db.save_signal_pattern(pattern="x", label="L", action="QUICK_BET")
    db.update_signal_pattern(pid, action="CASHOUT")
    assert db.get_signal_patterns()[0]["action"] == "CASHOUT"


def test_column_action_overrides_extra_json_action(db):
    # La colonna è autoritativa: una chiave "action" legacy in extra_json NON
    # deve sovrascrivere la colonna (settata dopo item.update(extra)).
    db.save_signal_pattern(pattern="x", label="L", action="CASHOUT_ALL",
                           extra={"action": "QUICK_BET"})
    assert db.get_signal_patterns()[0]["action"] == "CASHOUT_ALL"


def test_migration_adds_action_column_to_legacy_db():
    # DB legacy: signal_patterns SENZA colonna action.
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE signal_patterns (id INTEGER PRIMARY KEY, label TEXT)")
    cols = {r[1] for r in conn.execute("PRAGMA table_info(signal_patterns)").fetchall()}
    assert "action" not in cols

    Database._migrate_signal_pattern_action(conn)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(signal_patterns)").fetchall()}
    assert "action" in cols

    # Idempotente: una seconda chiamata non solleva e non duplica.
    Database._migrate_signal_pattern_action(conn)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(signal_patterns)").fetchall()]
    assert cols.count("action") == 1
