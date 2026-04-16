from __future__ import annotations

from core.runtime_controller import RuntimeController
from core.system_state import RoserpinaConfig, RuntimeMode


class _Bus:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []
        self.on_publish = None

    def subscribe(self, *_args) -> None:
        return None

    def publish(self, topic, payload=None) -> None:
        if callable(self.on_publish):
            self.on_publish(topic, dict(payload or {}))
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


def test_runtime_controller_emits_structured_auto_trade_result_payload():
    rc, bus = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-integration-1",
            "table_id": 1,
            "batch_id": "batch-integration-1",
            "correlation_id": "corr-integration-1",
            "pnl": 10.0,
            "auto_trade_enabled": True,
            "cycle_executor_enabled": True,
            "mm_context": {
                "cycle_active": True,
                "cycle_id": "cycle-1",
                "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
                "next_signal": {
                    "market_id": "1.234",
                    "selection_id": 8,
                    "price": 2.0,
                },
            },
        }
    )

    auto_events = [event for event in bus.events if event[0] == "AUTO_TRADE_MM_RESULT"]
    assert len(auto_events) == 1
    payload = auto_events[0][1]
    assert payload["source_settlement_correlation_id"] == "corr-integration-1"
    assert payload["bankroll_sync_status"] == "SYNC_SUCCESS"
    assert payload["money_management_status"] == "MM_CONTINUE_ALLOWED"
    assert payload["auto_trade_status"] == "AUTO_TRADE_SUBMITTED"


def test_runtime_controller_auto_trade_disabled_preserves_backward_compatibility():
    rc, bus = _make_controller(responses=[{"available": 140.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-integration-backward",
            "table_id": 1,
            "batch_id": "batch-integration-backward",
            "correlation_id": "corr-integration-backward",
            "pnl": 5.0,
        }
    )

    assert rc._last_bankroll_sync_result["bankroll_sync_status"] == "SYNC_SUCCESS"
    assert rc._last_auto_trade_result["auto_trade_status"] == "AUTO_TRADE_DISABLED"
    assert [event for event in bus.events if event[0] == "CMD_QUICK_BET"] == []


def test_runtime_controller_status_includes_auto_trade_snapshot():
    rc, _ = _make_controller(responses=[{"available": 150.0}, {"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-integration-status",
            "table_id": 1,
            "batch_id": "batch-integration-status",
            "correlation_id": "corr-integration-status",
            "pnl": 4.0,
        }
    )

    status = rc.get_status()
    assert "auto_trade_mm" in status
    assert status["auto_trade_mm"]["source_settlement_correlation_id"] == "corr-integration-status"


def test_runtime_controller_auto_trade_activates_table_before_publish():
    rc, bus = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)
    observed = {}

    def _inspect_publish(topic: str, payload: dict) -> None:
        if topic != "CMD_QUICK_BET":
            return
        table_id = int(payload["table_id"])
        table = rc.table_manager.get_table(table_id)
        observed["status"] = table.status if table else ""
        observed["event_key"] = table.current_event_key if table else ""
        observed["exposure"] = float(table.current_exposure if table else 0.0)
        observed["payload_event_key"] = str(payload.get("event_key") or "")
        observed["payload_stake"] = float(payload.get("stake") or 0.0)

    bus.on_publish = _inspect_publish

    rc._on_close_position(
        {
            "event_key": "evt-integration-activate",
            "table_id": 1,
            "batch_id": "batch-integration-activate",
            "correlation_id": "corr-integration-activate",
            "pnl": 10.0,
            "auto_trade_enabled": True,
            "cycle_executor_enabled": True,
            "mm_context": {
                "cycle_active": True,
                "cycle_id": "cycle-activate",
                "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
                "next_signal": {
                    "market_id": "1.888",
                    "selection_id": 11,
                    "table_id": 1,
                    "price": 2.0,
                },
            },
        }
    )

    assert observed["status"] == "ACTIVE"
    assert observed["event_key"] == observed["payload_event_key"]
    assert observed["exposure"] == observed["payload_stake"]
