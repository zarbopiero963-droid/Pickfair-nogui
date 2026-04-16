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


class _DB:
    def _execute(self, *_args, **_kwargs):
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


class _SafeMode:
    def __init__(self, enabled: bool):
        self.enabled = enabled


def _make_controller(*, responses, safe_mode=None):
    bus = _Bus()
    rc = RuntimeController(
        bus=bus,
        db=_DB(),
        settings_service=_Settings(),
        betfair_service=_Betfair(responses),
        telegram_service=_Telegram(),
        safe_mode=safe_mode,
    )
    return rc, bus


def _close_payload(**overrides):
    payload = {
        "event_key": "evt-1",
        "table_id": 1,
        "batch_id": "batch-1",
        "correlation_id": "corr-1",
        "pnl": 5.0,
        "auto_trade_enabled": True,
        "cycle_executor_enabled": True,
        "mm_context": {
            "cycle_active": True,
            "cycle_id": "cycle-1",
            "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
            "next_signal": {"market_id": "1.234", "selection_id": 42, "price": 2.0, "bet_type": "BACK"},
        },
    }
    payload.update(overrides)
    return payload


def test_cycle_executor_disabled_by_default():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc._on_close_position(_close_payload(cycle_executor_enabled=False))

    assert rc._last_cycle_executor_result["cycle_executor_status"] == "CYCLE_EXECUTOR_DISABLED"
    assert rc._last_cycle_executor_result["submitted"] is False


def test_cycle_executor_happy_path_single_step_submit():
    rc, bus = _make_controller(responses=[{"available": 150.0}, {"available": 175.0}])
    rc.mode = RuntimeMode.ACTIVE

    rc._on_close_position(_close_payload())

    assert rc._last_cycle_executor_result["cycle_executor_status"] == "CYCLE_STEP_SUBMITTED"
    assert rc._last_cycle_executor_result["submitted"] is True
    assert len([e for e in bus.events if e[0] == "CMD_QUICK_BET"]) == 1


def test_cycle_executor_skips_sync_failed():
    rc, _ = _make_controller(responses=[RuntimeError("boom")])
    rc.mode = RuntimeMode.ACTIVE

    rc._on_close_position(_close_payload())

    assert rc._last_cycle_executor_result["cycle_executor_status"] == "CYCLE_SKIPPED_SYNC_FAILED"
    assert rc._last_cycle_executor_result["submitted"] is False


def test_cycle_executor_skips_mm_blocked():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE

    rc._on_close_position(
        _close_payload(
            mm_context={
                "cycle_active": False,
                "cycle_id": "cycle-closed",
                "table": {"table_id": 1},
                "next_signal": {"market_id": "1.2", "selection_id": 2, "price": 2.0},
            }
        )
    )

    assert rc._last_cycle_executor_result["money_management_status"] == "MM_STOP_CYCLE_CLOSED"
    assert rc._last_cycle_executor_result["cycle_executor_status"] == "CYCLE_STOPPED_CLOSED"


def test_cycle_executor_skips_invalid_next_stake(monkeypatch):
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE

    class _Decision:
        cycle_active = True
        progression_allowed = True
        next_stake = 0.0
        money_management_status = "MM_CONTINUE_ALLOWED"
        stop_reason = "approved"
        table_id = 1
        cycle_id = "cycle-1"

    monkeypatch.setattr(rc.mm, "evaluate_next_trade_after_settlement", lambda **_kwargs: _Decision())
    rc._on_close_position(_close_payload())

    assert rc._last_cycle_executor_result["cycle_executor_status"] == "CYCLE_SKIPPED_INVALID_STAKE"


def test_cycle_executor_stops_on_kill_switch():
    rc, _ = _make_controller(responses=[{"available": 150.0}], safe_mode=_SafeMode(enabled=True))
    rc.mode = RuntimeMode.ACTIVE

    rc._on_close_position(_close_payload())

    assert rc._last_cycle_executor_result["cycle_executor_status"] == "CYCLE_STOPPED_KILL_SWITCH"
    assert rc._last_cycle_executor_result["submitted"] is False


def test_cycle_executor_stops_on_max_steps_limit():
    rc, _ = _make_controller(responses=[{"available": 150.0}, {"available": 160.0}])
    rc.mode = RuntimeMode.ACTIVE

    rc._on_close_position(_close_payload())
    rc._on_close_position(
        _close_payload(
            event_key="evt-2",
            batch_id="batch-2",
            correlation_id="corr-2",
            mm_context={
                "cycle_active": True,
                "cycle_id": "cycle-1",
                "max_steps": 1,
                "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
                "next_signal": {"market_id": "1.234", "selection_id": 42, "price": 2.0, "bet_type": "BACK"},
            },
        )
    )

    assert rc._last_cycle_executor_result["cycle_executor_status"] == "CYCLE_STOPPED_MAX_STEPS"
    assert rc._last_cycle_executor_result["max_steps_reached"] is True


def test_cycle_executor_skips_risk_rejected_runtime_inactive():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.STOPPED

    rc._on_close_position(_close_payload())

    assert rc._last_cycle_executor_result["cycle_executor_status"] == "CYCLE_SKIPPED_RISK_REJECTED"


def test_cycle_executor_skips_inflight_conflict():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.table_manager.activate(table_id=2, event_key="evt-open", exposure=1.0, market_id="1.999", selection_id=1)

    rc._on_close_position(
        _close_payload(
            mm_context={
                "cycle_active": True,
                "cycle_id": "cycle-1",
                "table_id": 2,
                "table": {"table_id": 2, "loss_amount": 0.0, "in_recovery": False},
                "next_signal": {"market_id": "1.2", "selection_id": 2, "price": 2.0, "table_id": 2},
            }
        )
    )

    assert rc._last_cycle_executor_result["cycle_executor_status"] == "CYCLE_SKIPPED_EXISTING_INFLIGHT"


def test_cycle_executor_duplicate_settlement_idempotency():
    rc, bus = _make_controller(responses=[{"available": 150.0}, {"available": 180.0}])
    rc.mode = RuntimeMode.ACTIVE
    payload = _close_payload()

    rc._on_close_position(payload)
    rc._on_close_position(payload)

    assert len([e for e in bus.events if e[0] == "CMD_QUICK_BET"]) == 1
    assert rc._last_cycle_executor_result["cycle_executor_status"] == "CYCLE_SKIPPED_DUPLICATE"
