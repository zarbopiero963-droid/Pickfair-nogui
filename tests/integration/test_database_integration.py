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


@pytest.mark.integration
def test_database_cycle_recovery_checkpoint_roundtrip():
    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))
        key = "batch-4a|evt-4a|1|bet-1"
        payload = {
            "settlement_correlation_id": "corr-4a",
            "cycle_id": "cycle-4a",
            "table_id": 1,
            "strategy_context": {"mode": "restore_only"},
            "checkpoint_stage": "MM_DECISION_DONE",
            "bankroll_sync_status": "SYNC_SUCCESS",
            "money_management_status": "MM_CONTINUE_ALLOWED",
            "cycle_active": True,
            "progression_allowed": True,
            "next_stake": 7.5,
            "step_index": 2,
            "round_index": 0,
            "next_trade_submission_status": "ATTEMPTED",
            "idempotency_key": key,
            "reason": "mm_decision_done",
            "is_ambiguous": True,
        }
        db.upsert_cycle_recovery_checkpoint(key, payload)
        loaded = db.get_cycle_recovery_checkpoint(key)
        state = db.get_cycle_recovery_state(key)

        assert loaded is not None
        assert loaded["settlement_correlation_id"] == "corr-4a"
        assert loaded["checkpoint_stage"] == "MM_DECISION_DONE"
        assert loaded["next_trade_submission_status"] == "ATTEMPTED"
        assert loaded["strategy_context"]["mode"] == "restore_only"
        assert state["exists"] is True
        assert state["processed"] is True
        assert state["ambiguous"] is True


@pytest.mark.integration
def test_database_cycle_recovery_checkpoint_monotonic_merge_preserves_stronger_truth():
    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))
        key = "batch-4b|evt-4b|1|bet-1"
        db.upsert_cycle_recovery_checkpoint(
            key,
            {
                "settlement_correlation_id": "corr-4b",
                "checkpoint_stage": "NEXT_TRADE_SUBMIT_CONFIRMED",
                "next_trade_submission_status": "SUBMITTED",
                "bankroll_sync_status": "SYNC_SUCCESS",
                "reason": "submitted",
                "is_ambiguous": False,
            },
        )
        db.upsert_cycle_recovery_checkpoint(
            key,
            {
                "settlement_correlation_id": "corr-4b-reentry",
                "checkpoint_stage": "BANKROLL_SYNC_DONE",
                "next_trade_submission_status": "NOT_ATTEMPTED",
                "bankroll_sync_status": "SYNC_SUCCESS",
                "reason": "attempted_downgrade",
                "is_ambiguous": False,
            },
        )

        loaded = db.get_cycle_recovery_checkpoint(key)
        assert loaded is not None
        assert loaded["checkpoint_stage"] == "NEXT_TRADE_SUBMIT_CONFIRMED"
        assert loaded["next_trade_submission_status"] == "SUBMITTED"
