from __future__ import annotations

import math

from core.risk_desk import RiskDesk


def test_sync_bankroll_updates_authoritative_state_and_is_idempotent_for_same_value():
    desk = RiskDesk()

    desk.sync_bankroll(100.0)
    first_updated_at = desk.updated_at
    desk.sync_bankroll(100.0)

    assert desk.bankroll_start == 100.0
    assert desk.bankroll_current == 100.0
    assert desk.equity_peak == 100.0
    assert desk.updated_at >= first_updated_at


def test_sync_bankroll_updates_changing_values_and_preserves_monotonic_equity_peak():
    desk = RiskDesk()

    desk.sync_bankroll(100.0)
    first_updated_at = desk.updated_at
    assert desk.bankroll_start == 100.0
    assert desk.bankroll_current == 100.0
    assert desk.equity_peak == 100.0

    desk.sync_bankroll(80.0)
    second_updated_at = desk.updated_at
    assert desk.bankroll_start == 100.0
    assert desk.bankroll_current == 80.0
    assert desk.equity_peak == 100.0
    assert second_updated_at >= first_updated_at

    desk.sync_bankroll(120.0)
    third_updated_at = desk.updated_at
    assert desk.bankroll_start == 100.0
    assert desk.bankroll_current == 120.0
    assert desk.equity_peak == 120.0
    assert third_updated_at >= second_updated_at


def test_sync_bankroll_coerces_none_and_non_finite_values_per_current_contract():
    desk = RiskDesk()

    desk.sync_bankroll(None)
    assert desk.bankroll_current == 0.0

    desk.sync_bankroll(float("nan"))
    assert math.isnan(desk.bankroll_current)

    desk.sync_bankroll(float("inf"))
    assert math.isinf(desk.bankroll_current)


def test_apply_closed_pnl_updates_realized_and_bankroll_for_positive_negative_zero_paths():
    desk = RiskDesk()
    desk.sync_bankroll(100.0)

    desk.apply_closed_pnl(25.5)
    assert desk.realized_pnl == 25.5
    assert desk.bankroll_current == 125.5

    desk.apply_closed_pnl(-10.5)
    assert desk.realized_pnl == 15.0
    assert desk.bankroll_current == 115.0

    before = (desk.realized_pnl, desk.bankroll_current)
    desk.apply_closed_pnl(0.0)
    assert (desk.realized_pnl, desk.bankroll_current) == before


def test_unrealized_and_exposure_snapshot_do_not_mutate_realized_pnl_and_build_coherent_snapshot():
    desk = RiskDesk()
    desk.sync_bankroll(200.0)
    desk.apply_closed_pnl(20.0)
    realized_before = desk.realized_pnl

    desk.set_unrealized_pnl(-5.0)
    desk.apply_open_exposure_snapshot(bankroll_current=210.0, unrealized_pnl=7.0)

    snap = desk.snapshot_dict(
        runtime_mode="ACTIVE",
        desk_mode="NORMAL",
        total_exposure=33.0,
        telegram_connected=True,
        betfair_connected=False,
        active_tables=2,
        recovery_tables=1,
        last_error="",
        last_signal_at="sig",
    )

    assert desk.realized_pnl == realized_before
    assert snap["bankroll_current"] == 210.0
    assert snap["unrealized_pnl"] == 7.0
    assert snap["total_exposure"] == 33.0
    assert snap["realized_pnl"] == realized_before


def test_snapshot_dataclass_returns_copy_not_live_internal_state():
    desk = RiskDesk()
    desk.sync_bankroll(300.0)

    snap = desk.snapshot_dataclass(
        runtime_mode="ACTIVE",
        desk_mode="NORMAL",
        total_exposure=1.0,
        telegram_connected=False,
        betfair_connected=True,
        active_tables=0,
        recovery_tables=0,
        last_error="",
        last_signal_at="",
    )
    snap["bankroll_current"] = -999.0

    assert desk.bankroll_current == 300.0


def test_apply_closed_pnl_and_unrealized_with_invalid_inputs_follow_current_fail_open_coercion():
    desk = RiskDesk()
    desk.sync_bankroll(50.0)

    desk.apply_closed_pnl(None)
    assert desk.realized_pnl == 0.0
    assert desk.bankroll_current == 50.0

    desk.apply_closed_pnl(float("nan"))
    assert math.isnan(desk.realized_pnl)
    assert math.isnan(desk.bankroll_current)

    desk = RiskDesk()
    desk.sync_bankroll(50.0)
    desk.apply_closed_pnl(float("inf"))
    assert math.isinf(desk.realized_pnl) and desk.realized_pnl > 0
    assert math.isinf(desk.bankroll_current) and desk.bankroll_current > 0

    desk = RiskDesk()
    desk.sync_bankroll(50.0)
    desk.apply_closed_pnl(-float("inf"))
    assert math.isinf(desk.realized_pnl) and desk.realized_pnl < 0
    assert math.isinf(desk.bankroll_current) and desk.bankroll_current < 0

    desk.set_unrealized_pnl("3.5")
    assert desk.unrealized_pnl == 3.5

    desk.set_unrealized_pnl(float("nan"))
    assert math.isnan(desk.unrealized_pnl)

    desk.set_unrealized_pnl(float("inf"))
    assert math.isinf(desk.unrealized_pnl) and desk.unrealized_pnl > 0

    desk.set_unrealized_pnl(-float("inf"))
    assert math.isinf(desk.unrealized_pnl) and desk.unrealized_pnl < 0
