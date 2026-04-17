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
            "gross_pnl": 20.0,
            "commission_amount": 0.9,
            "net_pnl": 19.1,
            "commission_pct": 4.5,
            "settlement_source": "test_settlement",
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
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
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
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
    assert payload["settlement_basis"] == "market_net_realized"
    assert payload["settlement_source"] == "test_settlement"
    assert payload["settlement_kind"] == "realized_settlement"
    assert payload["settlement_authority"] == "explicit_contract"
    assert payload["settlement_validation"] == "accepted"
    assert payload["settlement_acceptance"] == "ACCEPT_REALIZED_SETTLEMENT"
    assert payload["pnl"] == 19.1
    assert float(rc.risk_desk.realized_pnl) == 19.1


@pytest.mark.integration
def test_runtime_controller_close_payload_rejects_legacy_non_canonical_settlement_payload():
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
    assert rc._last_bankroll_sync_result["bankroll_sync_status"] == "SYNC_SUCCESS"
    assert float(rc.risk_desk.realized_pnl) == 0.0


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
            "gross_pnl": 10.0,
            "commission_amount": 0.45,
            "net_pnl": 9.55,
            "commission_pct": 4.5,
            "settlement_source": "test_settlement",
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
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
            "gross_pnl": 20.0,
            "commission_amount": 0.9,
            "net_pnl": 19.1,
            "commission_pct": 4.5,
            "settlement_source": "simulation_broker",
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
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
            "gross_pnl": 8.0,
            "commission_amount": 0.36,
            "net_pnl": 7.64,
            "commission_pct": 4.5,
            "settlement_source": "test_settlement",
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
        }
    )

    assert float(rc.risk_desk.realized_pnl) == 7.64


    assert float(rc.risk_desk.bankroll_current) == 150.0


@pytest.mark.integration
def test_runtime_controller_rejects_ambiguous_contract_without_explicit_or_legacy_net():
    rc, _ = _make_controller(responses=[{"available": 140.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-reject-ambiguous",
            "table_id": 1,
            "batch_id": "batch-reject-ambiguous",
            "correlation_id": "corr-reject-ambiguous",
            "gross_pnl": 10.0,
            "commission_amount": 0.45,
            "commission_pct": 4.5,
            "settlement_source": "simulation_broker",
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
        }
    )

    assert rc._last_bankroll_sync_result["bankroll_sync_status"] == "SYNC_FAILED_INVALID_SETTLEMENT_CONTRACT"
    assert rc._last_bankroll_sync_result["reason"] == "MISSING_CANONICAL_SETTLEMENT_FIELDS"
    assert rc._last_bankroll_sync_result["settlement_acceptance"] == "REJECT_AMBIGUOUS_SETTLEMENT"
    assert float(rc.risk_desk.realized_pnl) == 0.0


@pytest.mark.integration
def test_runtime_controller_rejects_mark_to_market_settlement_kind_for_close_processing():
    rc, _ = _make_controller(responses=[{"available": 140.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-reject-mtm",
            "table_id": 1,
            "batch_id": "batch-reject-mtm",
            "correlation_id": "corr-reject-mtm",
            "gross_pnl": 10.0,
            "commission_amount": 0.45,
            "net_pnl": 9.55,
            "commission_pct": 4.5,
            "settlement_source": "core_pnl_engine",
            "settlement_kind": "mark_to_market_estimate",
            "settlement_basis": "market_net_realized",
        }
    )

    assert rc._last_bankroll_sync_result["bankroll_sync_status"] == "SYNC_FAILED_INVALID_SETTLEMENT_CONTRACT"
    assert rc._last_bankroll_sync_result["reason"] == "SETTLEMENT_KIND_NOT_REALIZED"
    assert rc._last_bankroll_sync_result["settlement_acceptance"] == "REJECT_AMBIGUOUS_SETTLEMENT"
    assert float(rc.risk_desk.realized_pnl) == 0.0


@pytest.mark.integration
def test_runtime_controller_rejected_settlement_does_not_release_table_or_mutate_recovery_loss():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.risk_desk.sync_bankroll(100.0)
    rc.table_manager.activate(
        table_id=1,
        event_key="evt-reject-ordering",
        exposure=20.0,
        market_id="1.100",
        selection_id=7,
        meta={},
    )
    rc.table_manager.release(1, pnl=-10.0)
    rc.table_manager.activate(
        table_id=1,
        event_key="evt-reject-ordering",
        exposure=15.0,
        market_id="1.100",
        selection_id=7,
        meta={},
    )
    rc.duplication_guard.acquire("evt-reject-ordering")
    before = rc.table_manager.get_table(1)
    assert before is not None
    assert before.current_event_key == "evt-reject-ordering"
    assert float(before.loss_amount) == 10.0
    assert before.in_recovery is True

    rc._on_close_position(
        {
            "event_key": "evt-reject-ordering",
            "table_id": 1,
            "batch_id": "batch-reject-ordering",
            "correlation_id": "corr-reject-ordering",
            "gross_pnl": 10.0,
            "commission_amount": 0.45,
            "net_pnl": 9.55,
            "commission_pct": 4.5,
            "settlement_source": "core_pnl_engine",
            "settlement_kind": "mark_to_market_estimate",
            "settlement_basis": "market_net_realized",
        }
    )

    after = rc.table_manager.get_table(1)
    assert after is not None
    assert after.current_event_key == "evt-reject-ordering"
    assert float(after.loss_amount) == 10.0
    assert after.in_recovery is True
    assert any(k["event_key"] == "evt-reject-ordering" for k in rc.duplication_guard.snapshot()["active_keys"])


@pytest.mark.integration
def test_runtime_controller_legacy_non_canonical_settlement_releases_table_and_duplication_key():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.risk_desk.sync_bankroll(100.0)
    rc.table_manager.activate(
        table_id=1,
        event_key="evt-legacy-unlock",
        exposure=20.0,
        market_id="1.100",
        selection_id=7,
        meta={},
    )
    rc.table_manager.release(1, pnl=-10.0)
    rc.table_manager.activate(
        table_id=1,
        event_key="evt-legacy-unlock",
        exposure=15.0,
        market_id="1.100",
        selection_id=7,
        meta={},
    )
    rc.duplication_guard.acquire("evt-legacy-unlock")

    rc._on_close_position(
        {
            "event_key": "evt-legacy-unlock",
            "table_id": 1,
            "batch_id": "batch-legacy-unlock",
            "correlation_id": "corr-legacy-unlock",
            "gross_pnl": 13.0,
            "commission_amount": 0.5,
            "net_pnl": None,
            "commission_pct": 4.5,
            "settlement_source": "test_settlement",
            "pnl": 12.5,
            "mm_context": {"cycle_active": True},
        }
    )

    after = rc.table_manager.get_table(1)
    assert after is not None
    assert after.current_event_key in ("", None)
    assert float(after.loss_amount) == 0.0
    assert after.in_recovery is False
    assert not any(k["event_key"] == "evt-legacy-unlock" for k in rc.duplication_guard.snapshot()["active_keys"])
    assert rc._last_auto_trade_result["auto_trade_status"] == "AUTO_TRADE_DISABLED"


@pytest.mark.integration
def test_runtime_controller_accepted_settlement_still_releases_table_and_updates_recovery():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.risk_desk.sync_bankroll(100.0)
    rc.table_manager.activate(
        table_id=1,
        event_key="evt-accept-ordering",
        exposure=20.0,
        market_id="1.100",
        selection_id=7,
        meta={},
    )
    rc.table_manager.release(1, pnl=-10.0)
    rc.table_manager.activate(
        table_id=1,
        event_key="evt-accept-ordering",
        exposure=15.0,
        market_id="1.100",
        selection_id=7,
        meta={},
    )
    rc.duplication_guard.acquire("evt-accept-ordering")

    rc._on_close_position(
        {
            "event_key": "evt-accept-ordering",
            "table_id": 1,
            "batch_id": "batch-accept-ordering",
            "correlation_id": "corr-accept-ordering",
            "gross_pnl": 5.0,
            "commission_amount": 0.225,
            "net_pnl": 4.775,
            "commission_pct": 4.5,
            "settlement_source": "core_pnl_engine",
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
        }
    )

    after = rc.table_manager.get_table(1)
    assert after is not None
    assert after.current_event_key == ""
    assert float(after.loss_amount) == 5.225
    assert after.in_recovery is True
    assert all(k["event_key"] != "evt-accept-ordering" for k in rc.duplication_guard.snapshot()["active_keys"])


@pytest.mark.integration
def test_runtime_controller_helper_like_close_payload_does_not_become_authoritative_realized_settlement():
    rc, _ = _make_controller(responses=[{"available": 150.0}])
    rc.risk_desk.sync_bankroll(100.0)

    rc._on_close_position(
        {
            "event_key": "evt-helper-like-contract",
            "table_id": 1,
            "batch_id": "batch-helper-like-contract",
            "correlation_id": "corr-helper-like-contract",
            "gross_pnl": 30.0,
            "commission_amount": 1.35,
            "net_pnl": 28.65,
            "commission_pct": 4.5,
        }
    )

    assert rc._last_bankroll_sync_result["bankroll_sync_status"] == "SYNC_FAILED_INVALID_SETTLEMENT_CONTRACT"
    assert rc._last_bankroll_sync_result["reason"] == "MISSING_CANONICAL_SETTLEMENT_FIELDS"
    assert rc._last_bankroll_sync_result["settlement_acceptance"] == "REJECT_AMBIGUOUS_SETTLEMENT"
    assert float(rc.risk_desk.realized_pnl) == 0.0
