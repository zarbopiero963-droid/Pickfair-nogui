import tempfile
from pathlib import Path

import pytest

from database import Database


@pytest.mark.integration
def test_received_signal_and_settings_flow():
    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))

        db.save_telegram_settings(
            {
                "api_id": "123",
                "api_hash": "hash",
                "enabled": True,
                "auto_stake": 2.5,
            }
        )
        tg = db.get_telegram_settings()
        assert tg["api_id"] == "123"
        assert tg["enabled"] is True
        assert tg["auto_stake"] == 2.5

        db.save_received_signal(
            {
                "selection": "Over 2.5",
                "action": "BACK",
                "price": 1.85,
                "stake": 10,
                "status": "NEW",
            }
        )

        rows = db.get_received_signals(limit=10)
        assert len(rows) == 1
        assert rows[0]["selection"] == "Over 2.5"
        assert rows[0]["action"] == "BACK"


@pytest.mark.integration
def test_database_copy_state_persistence_roundtrip():
    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))
        state_key = "copy_state"
        copy_state = {
            "copy_group_id": "CG-01",
            "action_seq": 14,
            "positions": [
                {"position_id": "P-1", "status": "OPEN"},
                {"position_id": "P-2", "status": "PARTIAL"},
            ],
            "meta": {"source": "copy", "version": 2},
        }

        db.save_simulation_state(state_key, copy_state)
        restored = db.get_simulation_state(state_key)
        loaded = db.load_simulation_state(state_key)

        assert restored == copy_state
        assert loaded == copy_state


@pytest.mark.integration
def test_database_pattern_state_persistence_roundtrip():
    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))
        state_key = "pattern_state"
        pattern_state = {
            "order_origin": "PATTERN",
            "pattern_meta": {
                "pattern_id": "PT-01",
                "pattern_label": "late-goal",
                "event_context": {"league": "SERIE_A"},
            },
            "positions": [
                {"position_id": "PX-1", "status": "OPEN"},
            ],
        }

        db.save_simulation_state(state_key, pattern_state)
        restored = db.get_simulation_state(state_key)
        loaded = db.load_simulation_state(state_key)

        assert restored == pattern_state
        assert loaded == pattern_state
