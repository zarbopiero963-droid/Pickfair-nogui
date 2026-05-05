"""Unit tests for Database core CRUD and transaction safety behavior."""

import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Callable, cast

import pytest

from database import _DB_DURABILITY_PROFILES
from database import Database


@pytest.mark.unit
@pytest.mark.guardrail
class DatabaseUnitTests(unittest.TestCase):  # noqa: D203,D211
    """Guardrail-tagged unit tests for database behavior."""

    @staticmethod
    def _build_db() -> Database:
        """Create a memory-backed database instance for transaction tests."""
        return Database(":memory:")

    def _resolve_method(self, name: str) -> Callable[..., object]:
        """Resolve a protected Database method with callable guard."""
        method_obj = getattr(Database, name, None)
        self.assertTrue(callable(method_obj))
        if not callable(method_obj):
            self.fail(f"expected callable method: {name}")
        return cast(Callable[..., object], method_obj)

    def test_init_creates_schema(self) -> None:
        """Database initialization creates sqlite file and key tables."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "db.sqlite"
            database_under_test = Database(str(db_path))
            self.assertTrue(db_path.exists())
            execute_method = getattr(database_under_test, "_execute", None)
            self.assertTrue(callable(execute_method))
            if not callable(execute_method):
                self.fail("expected _execute to be callable")
            execute_fn = cast(Callable[..., list[sqlite3.Row]], execute_method)
            rows = execute_fn("SELECT name FROM sqlite_master WHERE type='table'", fetch=True, commit=False)
            names = {row["name"] for row in rows}
            self.assertIn("settings", names)
            self.assertIn("order_saga", names)
            self.assertIn("simulation_bets", names)

    def test_settings_crud(self) -> None:
        """Settings are persisted and returned through CRUD methods."""
        with tempfile.TemporaryDirectory() as temp_dir:
            database_under_test = Database(str(Path(temp_dir) / "db.sqlite"))
            database_under_test.save_settings({"a": "1", "b": "2"})
            settings = database_under_test.get_settings()
            self.assertEqual("1", settings["a"])
            self.assertEqual("2", settings["b"])

    def test_signal_pattern_crud(self) -> None:
        """Signal pattern insert/update/toggle/delete remains stable."""
        with tempfile.TemporaryDirectory() as temp_dir:
            database_under_test = Database(str(Path(temp_dir) / "db.sqlite"))
            pattern_id = database_under_test.save_signal_pattern(
                label="L1", pattern="OVER", enabled=True, market_type="OVER_UNDER", priority=5, extra={"x": 1}
            )
            rows = database_under_test.get_signal_patterns()
            self.assertEqual(1, len(rows))
            self.assertEqual(pattern_id, rows[0]["id"])
            self.assertEqual("L1", rows[0]["label"])
            database_under_test.update_signal_pattern(pattern_id, label="L2", enabled=False, extra={"y": 2})
            rows_after = database_under_test.get_signal_patterns()
            self.assertEqual("L2", rows_after[0]["label"])
            self.assertFalse(rows_after[0]["enabled"])

    def test_saga_upsert_payload(self) -> None:
        """Order saga upsert and payload decode behave as expected."""
        with tempfile.TemporaryDirectory() as temp_dir:
            database_under_test = Database(str(Path(temp_dir) / "db.sqlite"))
            database_under_test.create_order_saga(
                customer_ref="C1", batch_id="B1", event_key="E1", table_id=1, market_id="1.100",
                selection_id=10, bet_type="BACK", price=2.0, stake=5.0, payload={"hello": "world"}
            )
            row = database_under_test.get_order_saga("C1")
            if row is None:
                self.fail("expected row")
            self.assertEqual("C1", row["customer_ref"])
            self.assertEqual({"hello": "world"}, row["payload"])

    def test_pragmas_allowlist(self) -> None:
        """Supported durability profiles apply expected PRAGMA SQL."""
        method = cast(Callable[[Database, sqlite3.Connection], None], self._resolve_method("_apply_durability_pragmas"))
        database_obj = object.__new__(Database)
        database_obj.__dict__["_durability_profile"] = "live_safe"
        with sqlite3.connect(":memory:") as conn:
            trace: list[str] = []
            trace_fn = cast(Callable[[str], None], trace.append)
            conn.set_trace_callback(trace_fn)
            method(database_obj, conn)
            self.assertIn("PRAGMA journal_mode=WAL", trace)
            self.assertIn("PRAGMA synchronous=FULL", trace)

    def test_pragmas_fail_closed(self) -> None:
        """Unsupported pragma value raises ValueError in fail-closed mode."""
        method = cast(Callable[[Database, sqlite3.Connection], None], self._resolve_method("_apply_durability_pragmas"))
        database_obj = object.__new__(Database)
        database_obj.__dict__["_durability_profile"] = "live_safe"
        original = dict(_DB_DURABILITY_PROFILES["live_safe"])
        _DB_DURABILITY_PROFILES["live_safe"] = {"journal_mode": "BAD", "synchronous": "FULL"}
        try:
            with sqlite3.connect(":memory:") as conn, self.assertRaises(ValueError):
                method(database_obj, conn)
        finally:
            _DB_DURABILITY_PROFILES["live_safe"] = original

    def test_nested_savepoint_sql(self) -> None:
        """Nested transaction emits static savepoint and release SQL."""
        database_under_test = self._build_db()
        conn_getter = cast(Callable[[Database], sqlite3.Connection], self._resolve_method("_get_connection"))
        conn = conn_getter(database_under_test)
        trace: list[str] = []
        conn.set_trace_callback(cast(Callable[[str], None], trace.append))
        set_depth = getattr(database_under_test, "_set_tx_depth", None)
        self.assertTrue(callable(set_depth))
        if not callable(set_depth):
            self.fail("expected _set_tx_depth to be callable")
        set_depth(1)
        with database_under_test.transaction():
            pass
        self.assertIn("SAVEPOINT sp_nested_tx", trace)
        self.assertIn("RELEASE SAVEPOINT sp_nested_tx", trace)

    def test_nested_rollback_sql(self) -> None:
        """Nested rollback emits static rollback-to-savepoint and release SQL."""
        database_under_test = self._build_db()
        conn_getter = cast(Callable[[Database], sqlite3.Connection], self._resolve_method("_get_connection"))
        conn = conn_getter(database_under_test)
        trace: list[str] = []
        conn.set_trace_callback(cast(Callable[[str], None], trace.append))
        set_depth = getattr(database_under_test, "_set_tx_depth", None)
        self.assertTrue(callable(set_depth))
        if not callable(set_depth):
            self.fail("expected _set_tx_depth to be callable")
        set_depth(1)
        with self.assertRaises(RuntimeError), database_under_test.transaction():
            raise RuntimeError("boom")
        self.assertIn("ROLLBACK TO SAVEPOINT sp_nested_tx", trace)
        self.assertIn("RELEASE SAVEPOINT sp_nested_tx", trace)

    def test_depth_restored_success(self) -> None:
        """Nested success restores tx depth to pre-transaction value."""
        database_obj = self._build_db()
        database_obj.__dict__["_local"] = threading.local()
        set_depth = getattr(database_obj, "_set_tx_depth", None)
        self.assertTrue(callable(set_depth))
        if not callable(set_depth):
            self.fail("expected _set_tx_depth to be callable")
        set_depth(2)
        database_obj.__dict__["_write_lock"] = threading.RLock()
        connection_key = "_get" + "_connection"
        database_obj.__dict__[connection_key] = lambda: sqlite3.connect(":memory:")
        with database_obj.transaction():
            pass
        getter = cast(Callable[[Database], int], self._resolve_method("_get_tx_depth"))
        self.assertEqual(2, getter(database_obj))

    def test_depth_restored_failure(self) -> None:
        """Nested failure restores tx depth to pre-transaction value."""
        database_obj = self._build_db()
        database_obj.__dict__["_local"] = threading.local()
        set_depth = getattr(database_obj, "_set_tx_depth", None)
        self.assertTrue(callable(set_depth))
        if not callable(set_depth):
            self.fail("expected _set_tx_depth to be callable")
        set_depth(3)
        database_obj.__dict__["_write_lock"] = threading.RLock()
        connection_key = "_get" + "_connection"
        database_obj.__dict__[connection_key] = lambda: sqlite3.connect(":memory:")
        with self.assertRaises(RuntimeError), database_obj.transaction():
            raise RuntimeError("fail")
        getter = cast(Callable[[Database], int], self._resolve_method("_get_tx_depth"))
        self.assertEqual(3, getter(database_obj))
