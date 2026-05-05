import tempfile
import threading
from pathlib import Path

import pytest

from database import Database


@pytest.mark.unit
@pytest.mark.guardrail
def test_init_creates_db_file_and_schema():
    with tempfile.TemporaryDirectory() as td:
        db_path = Path(td) / "db.sqlite"
        db = Database(str(db_path))

        assert db_path.exists()

        rows = db._execute(
            "SELECT name FROM sqlite_master WHERE type='table'",
            fetch=True,
            commit=False,
        )
        names = {row["name"] for row in rows}
        assert "settings" in names
        assert "order_saga" in names
        assert "simulation_bets" in names


@pytest.mark.unit
@pytest.mark.guardrail
def test_settings_crud():
    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))

        db.save_settings({"a": "1", "b": "2"})
        settings = db.get_settings()

        assert settings["a"] == "1"
        assert settings["b"] == "2"


@pytest.mark.unit
@pytest.mark.guardrail
def test_signal_pattern_crud():
    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))

        pattern_id = db.save_signal_pattern(
            label="L1",
            pattern="OVER",
            enabled=True,
            market_type="OVER_UNDER",
            priority=5,
            extra={"x": 1},
        )

        rows = db.get_signal_patterns()
        assert len(rows) == 1
        assert rows[0]["id"] == pattern_id
        assert rows[0]["label"] == "L1"
        assert rows[0]["market_type"] == "OVER_UNDER"
        assert rows[0]["x"] == 1

        db.update_signal_pattern(pattern_id, label="L2", enabled=False, extra={"y": 2})
        rows2 = db.get_signal_patterns()
        assert rows2[0]["label"] == "L2"
        assert rows2[0]["enabled"] is False
        assert rows2[0]["y"] == 2

        new_state = db.toggle_signal_pattern(pattern_id)
        assert new_state is True

        db.delete_signal_pattern(pattern_id)
        assert db.get_signal_patterns() == []


@pytest.mark.unit
@pytest.mark.guardrail
def test_order_saga_upsert_and_payload_decode():
    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))

        db.create_order_saga(
            customer_ref="C1",
            batch_id="B1",
            event_key="E1",
            table_id=1,
            market_id="1.100",
            selection_id=10,
            bet_type="BACK",
            price=2.0,
            stake=5.0,
            payload={"hello": "world"},
        )

        row = db.get_order_saga("C1")
        assert row is not None
        assert row["customer_ref"] == "C1"
        assert row["payload"] == {"hello": "world"}

        db.update_order_saga(customer_ref="C1", status="PLACED", bet_id="BET1")
        row2 = db.get_order_saga("C1")
        assert row2["status"] == "PLACED"
        assert row2["bet_id"] == "BET1"

def test_apply_durability_pragmas_allowlist_and_fail_closed():
    db = object.__new__(Database)
    db._durability_profile = "live_safe"

    class Conn:
        def __init__(self):
            self.calls = []

        def execute(self, sql):
            self.calls.append(sql)
            return self

    conn = Conn()
    db._apply_durability_pragmas(conn)
    assert conn.calls == ["PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL"]

    db._durability_profile = "balanced"
    conn2 = Conn()
    db._apply_durability_pragmas(conn2)
    assert conn2.calls == ["PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL"]

    bad = object.__new__(Database)
    bad._durability_profile = "x"
    try:
        _ = bad._apply_durability_pragmas(conn2)
        assert False, "expected KeyError for unknown profile"
    except KeyError:
        pass


def test_transaction_nested_uses_static_savepoint_and_restores_depth():
    class FakeConn:
        def __init__(self):
            self.calls = []

        def execute(self, sql):
            self.calls.append(sql)
            return self

        def commit(self):
            self.calls.append("COMMIT")

        def rollback(self):
            self.calls.append("ROLLBACK")

    db = object.__new__(Database)
    db._local = threading.local()
    db._write_lock = threading.RLock()
    db._local.tx_depth = 1
    conn = FakeConn()
    db._get_connection = lambda: conn

    with db.transaction():
        pass

    assert "SAVEPOINT sp_nested_tx" in conn.calls
    assert "RELEASE SAVEPOINT sp_nested_tx" in conn.calls
    assert db._get_tx_depth() == 1


def test_transaction_nested_rollback_uses_static_sql_and_restores_depth():
    class FakeConn:
        def __init__(self):
            self.calls = []

        def execute(self, sql):
            self.calls.append(sql)
            return self

        def commit(self):
            self.calls.append("COMMIT")

        def rollback(self):
            self.calls.append("ROLLBACK")

    db = object.__new__(Database)
    db._local = threading.local()
    db._write_lock = threading.RLock()
    db._local.tx_depth = 1
    conn = FakeConn()
    db._get_connection = lambda: conn

    with pytest.raises(RuntimeError):
        with db.transaction():
            raise RuntimeError("boom")

    assert "SAVEPOINT sp_nested_tx" in conn.calls
    assert "ROLLBACK TO SAVEPOINT sp_nested_tx" in conn.calls
    assert "RELEASE SAVEPOINT sp_nested_tx" in conn.calls
    assert db._get_tx_depth() == 1
