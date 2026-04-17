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
        return {
            "market_data_mode": "poll",
            "enabled": False,
            "market_ids": [],
            "snapshot_fallback_enabled": True,
            "snapshot_fallback_interval_sec": 1,
        }


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
            return {"available": 0.0}
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
    rc = RuntimeController(
        bus=bus,
        db=_DB(),
        settings_service=_Settings(),
        betfair_service=_Betfair(responses),
        telegram_service=_Telegram(),
    )
    return rc, bus


def _close_payload(**overrides):
    payload = {
        "event_key": "evt-1",
        "table_id": 1,
        "batch_id": "batch-1",
        "correlation_id": "corr-1",
        "gross_pnl": 5.235602094240838,
        "commission_amount": 0.23560209424083772,
        "net_pnl": 5.0,
        "commission_pct": 4.5,
        "settlement_source": "core_pnl_engine",
        "settlement_kind": "realized_settlement",
        "mm_context": {
            "cycle_active": True,
            "cycle_id": "cycle-1",
            "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
            "next_signal": {
                "market_id": "1.234",
                "selection_id": 42,
                "price": 2.0,
                "bet_type": "BACK",
                "event_name": "A vs B",
            },
        },
    }
    payload.update(overrides)
    return payload


def _legacy_non_canonical_close_payload(**overrides):
    payload = _close_payload(**overrides)
    payload.pop("gross_pnl", None)
    payload.pop("commission_amount", None)
    payload.pop("net_pnl", None)
    payload.pop("commission_pct", None)
    payload.pop("settlement_source", None)
    payload.pop("settlement_kind", None)
    if "pnl" not in payload:
        payload["pnl"] = 5.0
    return payload


def test_auto_trade_disabled_by_default():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(_close_payload())

    assert rc._last_auto_trade_result["auto_trade_status"] == "AUTO_TRADE_DISABLED"
    assert rc._last_auto_trade_result["submitted"] is False
    assert rc._last_bankroll_sync_result["bankroll_sync_status"] == "SYNC_SUCCESS"


def test_auto_trade_rejects_non_canonical_settlement_payload():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        _legacy_non_canonical_close_payload(
            # Force hard rejection path instead of accepted canonical path.
            pnl=None,
        )
    )

    assert rc._last_auto_trade_result["auto_trade_status"] == "AUTO_TRADE_REJECTED_SETTLEMENT"
    assert rc._last_auto_trade_result["submitted"] is False
    assert rc._last_bankroll_sync_result["bankroll_sync_status"] == "SYNC_FAILED_INVALID_SETTLEMENT_CONTRACT"


def test_auto_trade_happy_path_submits_exactly_one_trade():
    rc, bus = _make_controller(responses=[{"available": 150.0}, {"available": 999.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)
    payload = _close_payload(auto_trade_enabled=True, cycle_executor_enabled=True)

    rc._on_close_position(payload)
    rc._on_close_position(payload)

    cmd_events = [event for event in bus.events if event[0] == "CMD_QUICK_BET"]
    assert len(cmd_events) == 1
    assert rc._last_auto_trade_result["auto_trade_status"] == "AUTO_TRADE_SKIPPED_DUPLICATE"


def test_auto_trade_skips_on_bankroll_sync_failed():
    rc, _ = _make_controller(responses=[RuntimeError("boom")])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(_close_payload(auto_trade_enabled=True, cycle_executor_enabled=True))

    assert rc._last_auto_trade_result["auto_trade_status"] == "AUTO_TRADE_SKIPPED_SYNC_FAILED"
    assert rc._last_auto_trade_result["submitted"] is False


def test_auto_trade_skips_when_mm_blocks_progression():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)

    payload = _close_payload(
        auto_trade_enabled=True,
        cycle_executor_enabled=True,
        mm_context={
            "cycle_active": False,
            "cycle_id": "cycle-stop",
            "table": {"table_id": 1},
            "next_signal": {"market_id": "1.2", "selection_id": 2, "price": 2.0},
        },
    )
    rc._on_close_position(payload)

    assert rc._last_auto_trade_result["money_management_status"] == "MM_STOP_CYCLE_CLOSED"
    assert rc._last_auto_trade_result["auto_trade_status"] == "AUTO_TRADE_SKIPPED_MM_BLOCKED"


def test_auto_trade_skips_invalid_next_stake(monkeypatch):
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)

    class _Decision:
        cycle_active = True
        progression_allowed = True
        next_stake = 0.0
        money_management_status = "MM_CONTINUE_ALLOWED"
        stop_reason = "approved"
        table_id = 1

    monkeypatch.setattr(rc.mm, "evaluate_next_trade_after_settlement", lambda **_kwargs: _Decision())
    rc._on_close_position(_close_payload(auto_trade_enabled=True, cycle_executor_enabled=True))

    assert rc._last_auto_trade_result["auto_trade_status"] == "AUTO_TRADE_SKIPPED_INVALID_STAKE"
    assert rc._last_auto_trade_result["money_management_status"] == "MM_STOP_INVALID_STAKE"


def test_auto_trade_skips_when_risk_rejects_runtime_not_active():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.STOPPED
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(_close_payload(auto_trade_enabled=True, cycle_executor_enabled=True))

    assert rc._last_auto_trade_result["auto_trade_status"] == "AUTO_TRADE_SKIPPED_RISK_REJECTED"
    assert rc._last_auto_trade_result["risk_status"] == "RISK_REJECTED"


def test_auto_trade_skips_on_existing_inflight_conflict():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)
    rc.table_manager.activate(
        table_id=2,
        event_key="evt-open",
        exposure=1.0,
        market_id="1.999",
        selection_id=1,
    )

    rc._on_close_position(
        _close_payload(
            auto_trade_enabled=True,
            cycle_executor_enabled=True,
            mm_context={
                "cycle_active": True,
                "table_id": 2,
                "table": {"table_id": 2, "loss_amount": 0.0, "in_recovery": False},
                "next_signal": {"market_id": "1.2", "selection_id": 2, "price": 2.0},
            },
        )
    )

    assert rc._last_auto_trade_result["auto_trade_status"] == "AUTO_TRADE_SKIPPED_EXISTING_INFLIGHT"


def test_auto_trade_submit_failure_returns_structured_status():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)

    payload = _close_payload(
        auto_trade_enabled=True,
        cycle_executor_enabled=True,
        mm_context={
            "cycle_active": True,
            "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
            "next_signal": {"market_id": "1.2", "selection_id": "bad", "price": 2.0},
        },
    )
    rc._on_close_position(payload)

    assert rc._last_auto_trade_result["auto_trade_status"] == "AUTO_TRADE_SUBMIT_FAILED"
    assert rc._last_auto_trade_result["submitted"] is False
