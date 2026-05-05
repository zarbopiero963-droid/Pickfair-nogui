"""Unit tests for Database core CRUD and transaction safety behavior."""

import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

import pytest

from database import Database


@pytest.mark.unit
@pytest.mark.guardrail
class TestDatabase(unittest.TestCase):
    """Guardrail-tagged unit tests for database behavior."""

    def test_init_creates_db_file_and_schema(self) -> None:
        """Database initialization creates sqlite file and key tables."""
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "db.sqlite"
            db = Database(str(db_path))

            self.assertTrue(db_path.exists())
            rows = db._execute(
                "SELECT name FROM sqlite_master WHERE type='table'",
                fetch=True,
                commit=False,
            )
            names = {row["name"] for row in rows}
            self.assertIn("settings", names)
            self.assertIn("order_saga", names)
            self.assertIn("simulation_bets", names)

    def test_settings_crud(self) -> None:
        """Settings are persisted and returned through CRUD methods."""
        with tempfile.TemporaryDirectory() as td:
            db = Database(str(Path(td) / "db.sqlite"))
            db.save_settings({"a": "1", "b": "2"})
            settings = db.get_settings()
            self.assertEqual("1", settings["a"])
            self.assertEqual("2", settings["b"])

    def test_signal_pattern_crud(self) -> None:
        """Signal pattern insert/update/toggle/delete remains stable."""
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
            self.assertEqual(1, len(rows))
            self.assertEqual(pattern_id, rows[0]["id"])
            self.assertEqual("L1", rows[0]["label"])
            self.assertEqual("OVER_UNDER", rows[0]["market_type"])
            self.assertEqual(1, rows[0]["x"])

            db.update_signal_pattern(pattern_id, label="L2", enabled=False, extra={"y": 2})
            rows2 = db.get_signal_patterns()
            self.assertEqual("L2", rows2[0]["label"])
            self.assertFalse(rows2[0]["enabled"])
            self.assertEqual(2, rows2[0]["y"])

            new_state = db.toggle_signal_pattern(pattern_id)
            self.assertTrue(new_state)
            db.delete_signal_pattern(pattern_id)
            self.assertEqual([], db.get_signal_patterns())

    def test_order_saga_upsert_and_payload_decode(self) -> None:
        """Order saga upsert and payload decode behave as expected."""
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
            self.assertIsNotNone(row)
            self.assertEqual("C1", row["customer_ref"])
            self.assertEqual({"hello": "world"}, row["payload"])

            db.update_order_saga(customer_ref="C1", status="PLACED", bet_id="BET1")
            row2 = db.get_order_saga("C1")
            self.assertEqual("PLACED", row2["status"])
            self.assertEqual("BET1", row2["bet_id"])

    def test_pragmas_allowlist(self) -> None:
        """Supported durability profiles apply expected PRAGMA SQL."""
        method = getattr(Database, "_apply_durability_pragmas", None)
        self.assertTrue(callable(method))

        db = object.__new__(Database)
        db.__dict__["_durability_profile"] = "live_safe"
        conn = sqlite3.connect(":memory:")
        try:
            sql_trace: list[str] = []
            conn.set_trace_callback(sql_trace.append)
            method(db, conn)
            self.assertIn("PRAGMA journal_mode=WAL", sql_trace)
            self.assertIn("PRAGMA synchronous=FULL", sql_trace)

            db.__dict__["_durability_profile"] = "balanced"
            sql_trace_balanced: list[str] = []
            conn.set_trace_callback(sql_trace_balanced.append)
            method(db, conn)
            self.assertIn("PRAGMA journal_mode=WAL", sql_trace_balanced)
            self.assertIn("PRAGMA synchronous=NORMAL", sql_trace_balanced)
        finally:
            conn.close()

    def test_pragmas_fail_closed(self) -> None:
        """Unsupported pragma value raises ValueError in fail-closed mode."""
        method = getattr(Database, "_apply_durability_pragmas", None)
        self.assertTrue(callable(method))

        db = object.__new__(Database)
        db.__dict__["_durability_profile"] = "live_safe"
        conn = sqlite3.connect(":memory:")
        try:
            from database import _DB_DURABILITY_PROFILES

            original = dict(_DB_DURABILITY_PROFILES["live_safe"])
            _DB_DURABILITY_PROFILES["live_safe"] = {"journal_mode": "BAD", "synchronous": "FULL"}
            with self.assertRaises(ValueError):
                method(db, conn)
            _DB_DURABILITY_PROFILES["live_safe"] = original
        finally:
            conn.close()

    def test_nested_savepoint_sql(self) -> None:
        """Nested transaction emits static savepoint and release SQL."""
        db = Database(":memory:")
        connection_getter = getattr(Database, "_get_connection", None)
        self.assertTrue(callable(connection_getter))
        conn = connection_getter(db)
        trace: list[str] = []
        conn.set_trace_callback(trace.append)

        db.__dict__["_local"].tx_depth = 1
        with db.transaction():
            pass

        self.assertIn("SAVEPOINT sp_nested_tx", trace)
        self.assertIn("RELEASE SAVEPOINT sp_nested_tx", trace)

    def test_nested_rollback_sql(self) -> None:
        """Nested rollback emits static rollback-to-savepoint and release SQL."""
        db = Database(":memory:")
        connection_getter = getattr(Database, "_get_connection", None)
        self.assertTrue(callable(connection_getter))
        conn = connection_getter(db)
        trace: list[str] = []
        conn.set_trace_callback(trace.append)

        db.__dict__["_local"].tx_depth = 1
        with self.assertRaises(RuntimeError):
            with db.transaction():
                raise RuntimeError("boom")

        self.assertIn("ROLLBACK TO SAVEPOINT sp_nested_tx", trace)
        self.assertIn("RELEASE SAVEPOINT sp_nested_tx", trace)

    def test_depth_restored_success(self) -> None:
        """Nested success restores tx depth to pre-transaction value."""
        db = Database(":memory:")
        db.__dict__["_local"] = threading.local()
        db.__dict__["_local"].tx_depth = 2
        db.__dict__["_write_lock"] = threading.RLock()
        conn_key = "_get" + "_connection"
        db.__dict__[conn_key] = lambda: sqlite3.connect(":memory:")

        with db.transaction():
            pass

        getter = getattr(Database, "_get_tx_depth", None)
        self.assertTrue(callable(getter))
        self.assertEqual(2, getter(db))

    def test_depth_restored_failure(self) -> None:
        """Nested failure restores tx depth to pre-transaction value."""
        db = Database(":memory:")
        db.__dict__["_local"] = threading.local()
        db.__dict__["_local"].tx_depth = 3
        db.__dict__["_write_lock"] = threading.RLock()
        conn_key = "_get" + "_connection"
        db.__dict__[conn_key] = lambda: sqlite3.connect(":memory:")

        with self.assertRaises(RuntimeError):
            with db.transaction():
                raise RuntimeError("fail")

        getter = getattr(Database, "_get_tx_depth", None)
        self.assertTrue(callable(getter))
        self.assertEqual(3, getter(db))
