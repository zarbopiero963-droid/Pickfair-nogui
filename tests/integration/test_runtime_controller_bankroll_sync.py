from __future__ import annotations

import pytest

from core.runtime_controller import RuntimeController
from core.system_state import RoserpinaConfig, RuntimeMode


class _Bus:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def subscribe(self, *_args):
        return None

    def publish(self, topic, payload=None):
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


class _Betfair:
    def __init__(self, responses):
        self._responses = list(responses)

    def set_simulation_mode(self, enabled):
        _ = enabled
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

    def get_market_book_snapshot(self, market_id):
        _ = market_id
        return None

    def ensure_stream_session_ready(self):
        return True


class _Telegram:
    def start(self):
        return {"started": True}

    def stop(self):
        return None

    def status(self):
        return {"connected": True}


def _make_controller(*, responses):
    bus = _Bus()
    rc = RuntimeController(
        bus=bus,
        db=_DB(),
        settings_service=_Settings(),
        betfair_service=_Betfair(responses),
        telegram_service=_Telegram(),
    )
    rc.mode = RuntimeMode.ACTIVE
    return rc, bus


@pytest.mark.integration
def test_runtime_controller_bankroll_sync_prefers_exchange_over_local_pnl():
    rc, bus = _make_controller(responses=[{"available": 120.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-prec",
            "table_id": 1,
            "batch_id": "batch-prec",
            "correlation_id": "corr-prec",
            "pnl": 999.0,
        }
    )

    assert float(rc.risk_desk.bankroll_current) == 120.0
    assert rc._last_bankroll_sync_result["bankroll_sync_status"] == "SYNC_SUCCESS"
    assert rc._last_bankroll_sync_result["balance_source"] == "exchange_available"
    assert any(topic == "BANKROLL_SYNC_RESULT" for topic, _ in bus.events)


@pytest.mark.integration
def test_runtime_controller_close_payload_preserves_settlement_provenance_fields():
    rc, bus = _make_controller(responses=[{"available": 140.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-prov",
            "table_id": 1,
            "batch_id": "batch-prov",
            "correlation_id": "corr-prov",
            "gross_pnl": 20.0,
            "commission_amount": 0.9,
            "net_pnl": 19.1,
            "commission_pct": 4.5,
            "settlement_source": "test_settlement",
            # conflicting legacy alias should not override explicit net_pnl
            "pnl": 999.0,
        }
    )

    closed = [payload for topic, payload in bus.events if topic == "BATCH_POSITION_CLOSED"]
    assert len(closed) == 1
    payload = closed[0]
    assert payload["gross_pnl"] == 20.0
    assert payload["commission_amount"] == 0.9
    assert payload["net_pnl"] == 19.1
    assert payload["commission_pct"] == 4.5
    assert payload["settlement_source"] == "test_settlement"
    assert payload["settlement_authority"] == "explicit_contract"
    assert payload["pnl"] == 19.1
    assert float(rc.risk_desk.realized_pnl) == 19.1


@pytest.mark.integration
def test_runtime_controller_close_payload_falls_back_to_legacy_pnl_when_net_is_null():
    rc, bus = _make_controller(responses=[{"available": 140.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-null-net",
            "table_id": 1,
            "batch_id": "batch-null-net",
            "correlation_id": "corr-null-net",
            "gross_pnl": 13.0,
            "commission_amount": 0.5,
            "net_pnl": None,
            "commission_pct": 4.5,
            "settlement_source": "test_settlement",
            "pnl": 12.5,
        }
    )

    closed = [payload for topic, payload in bus.events if topic == "BATCH_POSITION_CLOSED"]
    assert len(closed) == 1
    payload = closed[0]
    assert payload["pnl"] == 12.5
    assert payload["net_pnl"] == 12.5
    assert payload["settlement_source"] == "test_settlement"
    assert payload["settlement_authority"] == "legacy_fallback"
    assert float(rc.risk_desk.realized_pnl) == 12.5


@pytest.mark.integration
def test_runtime_controller_non_settlement_paths_do_not_trigger_bankroll_sync():
    rc, _ = _make_controller(responses=[{"available": 777.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_quick_bet_filled({"event_key": "evt-filled", "table_id": 1})

    result = rc._last_bankroll_sync_result
    assert result["bankroll_sync_status"] == "NOT_SETTLED"
    assert float(rc.risk_desk.bankroll_current) == 100.0


@pytest.mark.integration
def test_runtime_controller_status_exposes_last_bankroll_sync_result():
    rc, _ = _make_controller(responses=[{"available": 130.0}, {"available": 130.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-status",
            "table_id": 1,
            "batch_id": "batch-status",
            "correlation_id": "corr-status",
            "pnl": 5.0,
        }
    )

    status = rc.get_status()
    assert "bankroll_sync" in status
    assert status["bankroll_sync"]["bankroll_sync_status"] == "SYNC_SUCCESS"
    assert status["bankroll_sync"]["correlation_id"] == "corr-status"


@pytest.mark.integration
def test_runtime_controller_rejects_ambiguous_zero_fallback_balance_payload():
    rc, _ = _make_controller(responses=[{"available": 0.0, "exposure": 0.0, "total": 0.0, "simulated": False}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-zero-fallback-int",
            "table_id": 1,
            "batch_id": "batch-zero-fallback-int",
            "correlation_id": "corr-zero-fallback-int",
            "pnl": 20.0,
        }
    )

    assert float(rc.risk_desk.bankroll_current) == 100.0
    assert rc._last_bankroll_sync_result["bankroll_sync_status"] == "SYNC_FAILED_BALANCE_UNAVAILABLE"


@pytest.mark.integration
def test_runtime_controller_close_updates_realized_pnl_even_with_exchange_first_bankroll_sync():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-realized-int",
            "table_id": 1,
            "batch_id": "batch-realized-int",
            "correlation_id": "corr-realized-int",
            "pnl": 7.0,
        }
    )

    assert float(rc.risk_desk.realized_pnl) == 7.0
    assert float(rc.risk_desk.bankroll_current) == 150.0
