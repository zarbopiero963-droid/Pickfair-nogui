from __future__ import annotations

import tempfile
from pathlib import Path

from core.runtime_controller import RuntimeController
from core.system_state import RoserpinaConfig, RuntimeMode
from database import Database


class _Bus:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def subscribe(self, *_args) -> None:
        return None

    def publish(self, topic, payload=None) -> None:
        self.events.append((topic, dict(payload or {})))


class _Settings:
    def load_roserpina_config(self):
        cfg = RoserpinaConfig()
        cfg.anti_duplication_enabled = False
        return cfg

    def load_market_data_config(self):
        return {"market_data_mode": "poll", "enabled": False, "market_ids": []}


class _Telegram:
    def start(self):
        return {"started": True}

    def stop(self):
        return None

    def status(self):
        return {"connected": True}


class _Betfair:
    def __init__(self, responses):
        self._responses = list(responses)

    def set_simulation_mode(self, _enabled):
        return None

    def get_account_funds(self):
        if not self._responses:
            return {"available": 0.0, "ok": True}
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def status(self):
        return {"connected": True}

    def get_live_client(self):
        return object()

    def get_market_book_snapshot(self, _market_id):
        return None

    def ensure_stream_session_ready(self):
        return True


def _close_payload(**overrides):
    payload = {
        "event_key": "evt-it-cyc-1",
        "table_id": 1,
        "batch_id": "batch-it-cyc-1",
        "correlation_id": "corr-it-cyc-1",
        "pnl": 5.0,
        "auto_trade_enabled": True,
        "cycle_executor_enabled": True,
        "mm_context": {
            "cycle_active": True,
            "cycle_id": "cycle-it-1",
            "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
            "next_signal": {"market_id": "1.234", "selection_id": 42, "price": 2.0, "bet_type": "BACK"},
        },
    }
    payload.update(overrides)
    return payload


def _make_controller(*, db, responses):
    bus = _Bus()
    rc = RuntimeController(
        bus=bus,
        db=db,
        settings_service=_Settings(),
        betfair_service=_Betfair(responses),
        telegram_service=_Telegram(),
    )
    rc.mode = RuntimeMode.ACTIVE
    return rc, bus


def test_runtime_controller_cycle_recovery_checkpoint_roundtrip_and_restore_only():
    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))
        rc, bus = _make_controller(db=db, responses=[{"available": 150.0}])
        payload = _close_payload()

        rc._on_close_position(payload)
        key = rc._build_bankroll_sync_key(payload)
        checkpoint = db.get_cycle_recovery_checkpoint(key)

        assert checkpoint is not None
        assert checkpoint["checkpoint_stage"] == "NEXT_TRADE_SUBMIT_CONFIRMED"
        assert checkpoint["next_trade_submission_status"] == "SUBMITTED"
        assert checkpoint["bankroll_sync_status"] == "SYNC_SUCCESS"
        assert rc._last_cycle_executor_result["recovery_status"] in {"RECOVERY_NO_STATE", "RECOVERY_STATE_LOADED", "RECOVERY_SKIPPED_DUPLICATE", "RECOVERY_READY_NO_SUBMIT"}
        assert len([event for event in bus.events if event[0] == "CMD_QUICK_BET"]) == 1


def test_runtime_controller_cycle_recovery_ambiguous_checkpoint_fails_closed_no_submit():
    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))
        rc, bus = _make_controller(db=db, responses=[{"available": 150.0}])
        payload = _close_payload(correlation_id="corr-it-amb", event_key="evt-it-amb", batch_id="batch-it-amb")
        key = rc._build_bankroll_sync_key(payload)
        db.upsert_cycle_recovery_checkpoint(
            key,
            {
                "settlement_correlation_id": "corr-it-amb",
                "cycle_id": "cycle-it-amb",
                "table_id": 1,
                "checkpoint_stage": "NEXT_TRADE_SUBMIT_ATTEMPTED",
                "bankroll_sync_status": "SYNC_SUCCESS",
                "money_management_status": "MM_CONTINUE_ALLOWED",
                "cycle_active": True,
                "progression_allowed": True,
                "next_stake": 5.0,
                "step_index": 1,
                "round_index": 0,
                "next_trade_submission_status": "ATTEMPTED",
                "idempotency_key": key,
                "reason": "submit_attempt",
                "is_ambiguous": True,
            },
        )

        rc._on_close_position(payload)

        assert rc._last_cycle_executor_result["cycle_executor_status"] == "CYCLE_AMBIGUOUS"
        assert rc._last_cycle_executor_result["recovery_status"] == "RECOVERY_STATE_AMBIGUOUS"
        assert len([event for event in bus.events if event[0] == "CMD_QUICK_BET"]) == 0
