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


def test_cycle_executor_result_event_contains_contract_fields():
    rc, bus = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE

    rc._on_close_position(
        {
            "event_key": "evt-integration-cycle-1",
            "table_id": 1,
            "batch_id": "batch-integration-cycle-1",
            "correlation_id": "corr-integration-cycle-1",
            "pnl": 5.0,
            "auto_trade_enabled": True,
            "cycle_executor_enabled": True,
            "mm_context": {
                "cycle_active": True,
                "cycle_id": "cycle-integration-1",
                "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
                "next_signal": {"market_id": "1.2", "selection_id": 2, "price": 2.0},
            },
        }
    )

    payload = [event for event in bus.events if event[0] == "AUTO_TRADE_MM_RESULT"][0][1]
    assert payload["source_settlement_correlation_id"] == "corr-integration-cycle-1"
    assert payload["cycle_executor_status"] == "CYCLE_STEP_SUBMITTED"
    assert payload["money_management_status"] == "MM_CONTINUE_ALLOWED"
    assert payload["bankroll_sync_status"] == "SYNC_SUCCESS"


def test_cycle_executor_disabled_keeps_submission_off_and_status_snapshot_available():
    rc, bus = _make_controller(responses=[{"available": 140.0}])
    rc.mode = RuntimeMode.ACTIVE

    rc._on_close_position(
        {
            "event_key": "evt-integration-cycle-disabled",
            "table_id": 1,
            "batch_id": "batch-integration-cycle-disabled",
            "correlation_id": "corr-integration-cycle-disabled",
            "pnl": 5.0,
            "auto_trade_enabled": True,
        }
    )

    assert rc._last_cycle_executor_result["cycle_executor_status"] == "CYCLE_EXECUTOR_DISABLED"
    assert [event for event in bus.events if event[0] == "CMD_QUICK_BET"] == []

    status = rc.get_status()
    assert status["cycle_executor"]["cycle_executor_status"] == "CYCLE_EXECUTOR_DISABLED"


def test_cycle_executor_max_steps_with_valid_cycle_id_allows_below_limit():
    rc, bus = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE

    rc._on_close_position(
        {
            "event_key": "evt-integration-cycle-max-ok",
            "table_id": 1,
            "batch_id": "batch-integration-cycle-max-ok",
            "correlation_id": "corr-integration-cycle-max-ok",
            "pnl": 5.0,
            "auto_trade_enabled": True,
            "cycle_executor_enabled": True,
            "mm_context": {
                "cycle_active": True,
                "cycle_id": "cycle-integration-max-ok",
                "max_steps": 2,
                "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
                "next_signal": {"market_id": "1.2", "selection_id": 2, "price": 2.0},
            },
        }
    )

    assert rc._last_cycle_executor_result["cycle_executor_status"] == "CYCLE_STEP_SUBMITTED"
    assert len([event for event in bus.events if event[0] == "CMD_QUICK_BET"]) == 1
