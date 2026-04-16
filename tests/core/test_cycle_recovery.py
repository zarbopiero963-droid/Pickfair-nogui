from __future__ import annotations

from core.runtime_controller import RuntimeController
from core.system_state import RoserpinaConfig, RuntimeMode


class _Bus:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def subscribe(self, *_args) -> None:
        return None

    def publish(self, topic, payload=None) -> None:
        self.events.append((topic, dict(payload or {})))


class _CheckpointDB:
    def __init__(self) -> None:
        self.records: dict[str, dict] = {}

    def _execute(self, *_args, **_kwargs):
        return None

    def upsert_cycle_recovery_checkpoint(self, settlement_key: str, payload: dict) -> None:
        current = dict(self.records.get(settlement_key, {}))
        current.update(dict(payload))
        self.records[settlement_key] = current

    def get_cycle_recovery_state(self, settlement_key: str) -> dict:
        item = self.records.get(settlement_key)
        if item is None:
            return {"exists": False, "processed": False, "bankroll_synced": False, "submit_attempted": False, "submit_confirmed": False, "ambiguous": False}
        submit_status = str(item.get("next_trade_submission_status") or "NOT_ATTEMPTED")
        stage = str(item.get("checkpoint_stage") or "")
        return {
            "exists": True,
            "processed": stage != "",
            "bankroll_synced": str(item.get("bankroll_sync_status") or "") == "SYNC_SUCCESS",
            "submit_attempted": submit_status in {"ATTEMPTED", "SUBMITTED", "AMBIGUOUS"},
            "submit_confirmed": submit_status in {"SUBMITTED", "CONFIRMED"},
            "ambiguous": bool(item.get("is_ambiguous", False)),
            "stage": stage,
            "checkpoint": dict(item),
        }

    def get_cycle_recovery_checkpoint(self, settlement_key: str):
        item = self.records.get(settlement_key)
        return dict(item) if isinstance(item, dict) else None


class _NoCheckpointDB:
    def _execute(self, *_args, **_kwargs):
        return None

    def upsert_cycle_recovery_checkpoint(self, _settlement_key: str, _payload: dict) -> None:
        return None

    def get_cycle_recovery_state(self, _settlement_key: str) -> dict:
        return {
            "exists": False,
            "processed": False,
            "bankroll_synced": False,
            "submit_attempted": False,
            "submit_confirmed": False,
            "ambiguous": False,
        }

    def get_cycle_recovery_checkpoint(self, _settlement_key: str):
        return None


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


def _make_controller(*, responses):
    bus = _Bus()
    db = _CheckpointDB()
    rc = RuntimeController(
        bus=bus,
        db=db,
        settings_service=_Settings(),
        betfair_service=_Betfair(responses),
        telegram_service=_Telegram(),
    )
    rc.mode = RuntimeMode.ACTIVE
    return rc, bus, db


def _make_controller_no_checkpoint(*, responses):
    bus = _Bus()
    rc = RuntimeController(
        bus=bus,
        db=_NoCheckpointDB(),
        settings_service=_Settings(),
        betfair_service=_Betfair(responses),
        telegram_service=_Telegram(),
    )
    rc.mode = RuntimeMode.ACTIVE
    return rc, bus


def _close_payload(**overrides):
    payload = {
        "event_key": "evt-cyc-1",
        "table_id": 1,
        "batch_id": "batch-cyc-1",
        "correlation_id": "corr-cyc-1",
        "pnl": 5.0,
        "auto_trade_enabled": True,
        "cycle_executor_enabled": True,
        "resume_submit_enabled": False,
        "mm_context": {
            "cycle_active": True,
            "cycle_id": "cycle-1",
            "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
            "next_signal": {"market_id": "1.234", "selection_id": 42, "price": 2.0, "bet_type": "BACK"},
        },
    }
    payload.update(overrides)
    return payload


def test_cycle_recovery_no_prior_state_defaults_to_no_state():
    rc, _, _ = _make_controller(responses=[{"available": 150.0}])
    probe = rc._read_cycle_recovery_state("missing|key")
    assert probe["status"] == "RECOVERY_NO_STATE"


def test_restore_only_blocks_submit_when_resume_disabled_and_checkpoint_missing():
    rc, bus, _ = _make_controller(responses=[{"available": 150.0}])
    payload = _close_payload(correlation_id="corr-no-state", event_key="evt-no-state", batch_id="batch-no-state")

    rc._on_close_position(payload)

    assert rc._last_cycle_executor_result["submitted"] is False
    assert rc._last_cycle_executor_result["recovery_status"] == "RECOVERY_READY_NO_SUBMIT"
    assert rc._last_cycle_executor_result["reason"] == "resume_submit_disabled"
    assert not any(topic == "CMD_QUICK_BET" for topic, _ in bus.events)


def test_restore_only_blocks_submit_when_checkpoint_persistence_unavailable():
    rc, bus = _make_controller_no_checkpoint(responses=[{"available": 150.0}])
    payload = _close_payload(correlation_id="corr-no-db", event_key="evt-no-db", batch_id="batch-no-db")

    rc._on_close_position(payload)

    assert rc._last_cycle_executor_result["submitted"] is False
    assert rc._last_cycle_executor_result["recovery_status"] == "RECOVERY_READY_NO_SUBMIT"
    assert rc._last_cycle_executor_result["reason"] == "resume_submit_disabled"
    assert not any(topic == "CMD_QUICK_BET" for topic, _ in bus.events)


def test_cycle_recovery_checkpoint_progression_and_duplicate_marker():
    rc, _, db = _make_controller(responses=[{"available": 150.0}, {"available": 999.0}])
    payload = _close_payload(resume_submit_enabled=True)
    rc._on_close_position(payload)
    key = rc._build_bankroll_sync_key(payload)
    assert db.records[key]["checkpoint_stage"] == "NEXT_TRADE_SUBMIT_CONFIRMED"
    assert db.records[key]["next_trade_submission_status"] == "SUBMITTED"

    rc._on_close_position(payload)
    assert rc._last_cycle_executor_result["auto_trade_status"] == "AUTO_TRADE_SKIPPED_DUPLICATE"


def test_cycle_recovery_ambiguous_state_fails_closed_without_submit():
    rc, bus, db = _make_controller(responses=[{"available": 150.0}])
    payload = _close_payload(correlation_id="corr-amb", event_key="evt-amb", batch_id="batch-amb", resume_submit_enabled=True)
    key = rc._build_bankroll_sync_key(payload)
    db.upsert_cycle_recovery_checkpoint(
        key,
        {
            "checkpoint_stage": "NEXT_TRADE_SUBMIT_ATTEMPTED",
            "next_trade_submission_status": "ATTEMPTED",
            "bankroll_sync_status": "SYNC_SUCCESS",
            "is_ambiguous": True,
        },
    )

    rc._on_close_position(payload)

    assert rc._last_cycle_executor_result["cycle_executor_status"] == "CYCLE_AMBIGUOUS"
    assert rc._last_cycle_executor_result["submitted"] is False
    assert not any(topic == "CMD_QUICK_BET" for topic, _ in bus.events)


def test_cycle_reentry_does_not_downgrade_confirmed_checkpoint():
    rc, _, db = _make_controller(responses=[{"available": 999.0}])
    payload = _close_payload(correlation_id="corr-confirmed", event_key="evt-confirmed", batch_id="batch-confirmed")
    key = rc._build_bankroll_sync_key(payload)
    db.upsert_cycle_recovery_checkpoint(
        key,
        {
            "checkpoint_stage": "NEXT_TRADE_SUBMIT_CONFIRMED",
            "next_trade_submission_status": "SUBMITTED",
            "bankroll_sync_status": "SYNC_SUCCESS",
            "reason": "submitted",
            "is_ambiguous": False,
        },
    )

    rc._on_close_position(payload)
    stored = db.get_cycle_recovery_checkpoint(key)
    assert stored["checkpoint_stage"] == "NEXT_TRADE_SUBMIT_CONFIRMED"
    assert stored["next_trade_submission_status"] == "SUBMITTED"
    assert rc._last_cycle_executor_result["recovery_status"] == "RECOVERY_SKIPPED_ALREADY_SUBMITTED"


def test_cycle_reentry_does_not_downgrade_attempted_checkpoint():
    rc, _, db = _make_controller(responses=[{"available": 180.0}])
    payload = _close_payload(correlation_id="corr-attempted", event_key="evt-attempted", batch_id="batch-attempted")
    key = rc._build_bankroll_sync_key(payload)
    db.upsert_cycle_recovery_checkpoint(
        key,
        {
            "checkpoint_stage": "NEXT_TRADE_SUBMIT_ATTEMPTED",
            "next_trade_submission_status": "ATTEMPTED",
            "bankroll_sync_status": "SYNC_SUCCESS",
            "reason": "submit_attempt",
            "is_ambiguous": True,
        },
    )

    rc._on_close_position(payload)
    stored = db.get_cycle_recovery_checkpoint(key)
    assert stored["checkpoint_stage"] == "CYCLE_AMBIGUOUS"
    assert stored["next_trade_submission_status"] == "AMBIGUOUS"


def test_recovery_checkpoint_defaults_to_restore_only_without_resume_submit():
    rc, bus, db = _make_controller(responses=[{"available": 150.0}])
    payload = _close_payload(correlation_id="corr-restore", event_key="evt-restore", batch_id="batch-restore")
    key = rc._build_bankroll_sync_key(payload)
    db.upsert_cycle_recovery_checkpoint(
        key,
        {
            "checkpoint_stage": "BANKROLL_SYNC_DONE",
            "next_trade_submission_status": "NOT_ATTEMPTED",
            "bankroll_sync_status": "SYNC_SUCCESS",
            "is_ambiguous": False,
        },
    )

    rc._on_close_position(payload)
    assert rc._last_cycle_executor_result["recovery_status"] == "RECOVERY_READY_NO_SUBMIT"
    assert rc._last_cycle_executor_result["submitted"] is False
    assert not any(topic == "CMD_QUICK_BET" for topic, _ in bus.events)


def test_startup_restore_only_never_emits_cmd_quick_bet_without_explicit_resume_enable():
    rc, bus, db = _make_controller(responses=[{"available": 150.0}])
    payload = _close_payload(
        correlation_id="corr-startup-no-autofire",
        event_key="evt-startup-no-autofire",
        batch_id="batch-startup-no-autofire",
        resume_submit_enabled=False,
    )
    key = rc._build_bankroll_sync_key(payload)
    db.upsert_cycle_recovery_checkpoint(
        key,
        {
            "checkpoint_stage": "BANKROLL_SYNC_DONE",
            "next_trade_submission_status": "NOT_ATTEMPTED",
            "bankroll_sync_status": "SYNC_SUCCESS",
            "is_ambiguous": False,
        },
    )

    rc._on_close_position(payload)

    assert rc._last_cycle_executor_result["recovery_status"] == "RECOVERY_READY_NO_SUBMIT"
    assert rc._last_cycle_executor_result["submitted"] is False
    assert rc._last_cycle_executor_result["reason"] == "resume_submit_disabled"
    assert not any(topic == "CMD_QUICK_BET" for topic, _ in bus.events)


def test_recovery_checkpoint_resume_submit_submits_one_step_when_enabled():
    rc, bus, db = _make_controller(responses=[{"available": 150.0}])
    payload = _close_payload(
        correlation_id="corr-resume",
        event_key="evt-resume",
        batch_id="batch-resume",
        resume_submit_enabled=True,
    )
    key = rc._build_bankroll_sync_key(payload)
    db.upsert_cycle_recovery_checkpoint(
        key,
        {
            "checkpoint_stage": "BANKROLL_SYNC_DONE",
            "next_trade_submission_status": "NOT_ATTEMPTED",
            "bankroll_sync_status": "SYNC_SUCCESS",
            "is_ambiguous": False,
        },
    )

    rc._on_close_position(payload)
    assert rc._last_cycle_executor_result["recovery_status"] == "RECOVERY_STEP_SUBMITTED"
    assert rc._last_cycle_executor_result["submitted"] is True
    assert len([event for event in bus.events if event[0] == "CMD_QUICK_BET"]) == 1


def test_recovery_resume_submit_invalid_stake_blocks_submit():
    rc, bus, db = _make_controller(responses=[{"available": 150.0}])
    payload = _close_payload(
        correlation_id="corr-invalid-stake",
        event_key="evt-invalid-stake",
        batch_id="batch-invalid-stake",
        resume_submit_enabled=True,
        mm_context={
            "cycle_active": True,
            "cycle_id": "cycle-1",
            "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
            "next_signal": {"market_id": "1.234", "selection_id": 42, "price": 2.0, "bet_type": "BACK"},
            "target_reached": True,
        },
    )
    key = rc._build_bankroll_sync_key(payload)
    db.upsert_cycle_recovery_checkpoint(
        key,
        {
            "checkpoint_stage": "BANKROLL_SYNC_DONE",
            "next_trade_submission_status": "NOT_ATTEMPTED",
            "bankroll_sync_status": "SYNC_SUCCESS",
            "is_ambiguous": False,
        },
    )

    rc._on_close_position(payload)
    assert rc._last_cycle_executor_result["submitted"] is False
    assert rc._last_cycle_executor_result["recovery_status"] in {"RECOVERY_STOPPED_TARGET_REACHED", "RECOVERY_SKIPPED_MM_BLOCKED"}
    assert not any(topic == "CMD_QUICK_BET" for topic, _ in bus.events)
