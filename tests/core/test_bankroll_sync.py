from __future__ import annotations

from core.runtime_controller import RuntimeController
from core.system_state import RoserpinaConfig


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


def test_bankroll_sync_success_on_settlement_close_event():
    rc, bus = _make_controller(responses=[{"available": 150.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-1",
            "table_id": 1,
            "batch_id": "batch-1",
            "correlation_id": "corr-1",
            "pnl": 25.0,
        }
    )

    result = rc._last_bankroll_sync_result
    assert result["correlation_id"] == "corr-1"
    assert result["settlement_detected"] is True
    assert result["bankroll_before"] == 100.0
    assert result["bankroll_after"] == 150.0
    assert result["bankroll_sync_status"] == "SYNC_SUCCESS"
    assert result["balance_source"] == "exchange_available"
    assert result["reason"] == "BALANCE_SYNCED_FROM_EXCHANGE"
    assert float(rc.risk_desk.bankroll_current) == 150.0
    assert any(topic == "BANKROLL_SYNC_RESULT" for topic, _ in bus.events)


def test_bankroll_sync_duplicate_settlement_is_idempotent():
    rc, _ = _make_controller(responses=[{"available": 150.0}, {"available": 999.0}])
    rc.risk_desk.sync_bankroll(100.0)
    payload = {
        "event_key": "evt-dup",
        "table_id": 1,
        "batch_id": "batch-dup",
        "correlation_id": "corr-dup",
        "pnl": 10.0,
    }

    rc._on_close_position(payload)
    first_after = float(rc.risk_desk.bankroll_current)
    rc._on_close_position(payload)

    result = rc._last_bankroll_sync_result
    assert first_after == 150.0
    assert float(rc.risk_desk.bankroll_current) == 150.0
    assert result["bankroll_sync_status"] == "SYNC_SKIPPED_DUPLICATE"
    assert result["reason"] == "SETTLEMENT_ALREADY_SYNCED"


def test_bankroll_sync_fails_closed_when_balance_unavailable():
    rc, _ = _make_controller(responses=[RuntimeError("boom")])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-fail",
            "table_id": 1,
            "batch_id": "batch-fail",
            "correlation_id": "corr-fail",
            "pnl": -50.0,
        }
    )

    result = rc._last_bankroll_sync_result
    assert result["bankroll_sync_status"] == "SYNC_FAILED_BALANCE_UNAVAILABLE"
    assert float(rc.risk_desk.bankroll_current) == 100.0


def test_bankroll_sync_fails_closed_when_balance_invalid():
    rc, _ = _make_controller(responses=[{"available": "nan"}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-invalid",
            "table_id": 1,
            "batch_id": "batch-invalid",
            "correlation_id": "corr-invalid",
            "pnl": 10.0,
        }
    )

    result = rc._last_bankroll_sync_result
    assert result["bankroll_sync_status"] == "SYNC_FAILED_INVALID_BALANCE"
    assert float(rc.risk_desk.bankroll_current) == 100.0
