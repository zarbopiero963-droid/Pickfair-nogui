import tempfile
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