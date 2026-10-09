"""PR28-d: cap assoluti Roserpina e input ordine fail-closed."""
from __future__ import annotations

import pytest
import threading

from core import validators
from services.telegram_signal_processor import TelegramSignalProcessor
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal

pytestmark = [pytest.mark.integration, pytest.mark.safety]


@pytest.mark.parametrize("side", [None, "", "BOTH", True])
def test_missing_or_invalid_side_rejected_before_any_order(tmp_path, side):
    c = _catena(tmp_path)
    sig = _signal(stake=0.5)
    sig["bet_type"] = side
    c.rc._on_signal_received(sig)
    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert c.broker.state.orders == {}
    assert _rifiuti(c) == ["lato_non_valido"]


@pytest.mark.parametrize("value", [True, False, float("nan"), float("inf"), "x"])
def test_bool_and_non_finite_money_never_become_numbers(value):
    assert validators.finite_number(value) is None
    with pytest.raises(ValueError):
        validators.order_stake(value)


def test_order_absolute_cap_exact_and_over(tmp_path):
    exact = _catena(tmp_path / "exact")
    exact.rc.config.max_order_exposure = 1.0
    exact.rc._on_signal_received(_signal(stake=1.0, bet_type="BACK"))
    assert len(exact.broker.state.orders) == 1

    over = _catena(tmp_path / "over")
    over.rc.config.max_order_exposure = 1.0
    over.rc._on_signal_received(_signal(stake=1.01, bet_type="BACK"))
    assert over.broker.state.orders == {}
    assert _rifiuti(over)[0].startswith("max_order_exposure_exceeded")


def test_lay_order_cap_uses_liability(tmp_path):
    c = _catena(tmp_path)
    c.rc.config.max_order_exposure = 1.0
    c.rc._on_signal_received(_signal(stake=0.51, price=3.0, bet_type="LAY"))
    assert c.broker.state.orders == {}
    assert _rifiuti(c)[0].startswith("max_order_exposure_exceeded")


def test_market_and_event_caps_use_existing_exposure(tmp_path):
    c = _catena(tmp_path)
    c.rc.config.max_market_exposure = 2.0
    c.rc.config.max_event_exposure_abs = 2.0
    sig = _signal(stake=0.6, bet_type="BACK")
    key = c.rc.duplication_guard.build_event_key(sig)
    table = c.rc.table_manager._tables[2]
    table.status = "ACTIVE"
    table.current_event_key = key
    table.market_id = str(sig["market_id"])
    table.current_exposure = 1.5
    c.rc._on_signal_received(sig)
    assert c.broker.state.orders == {}
    assert _rifiuti(c)[0].startswith(("max_market_exposure_exceeded", "max_event_exposure_abs_exceeded"))


@pytest.mark.parametrize("field", [
    "max_order_exposure", "max_market_exposure", "max_event_exposure_abs",
    "max_open_exposure", "max_drawdown_abs",
])
@pytest.mark.parametrize("bad", [True, False, float("nan"), float("inf"), 0.0, -1.0])
def test_invalid_configured_absolute_cap_fails_closed(tmp_path, field, bad):
    c = _catena(tmp_path)
    setattr(c.rc.config, field, bad)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert c.broker.state.orders == {}
    assert field in _rifiuti(c)[0]


def test_absolute_drawdown_reaches_threshold_and_blocks(tmp_path):
    c = _catena(tmp_path)
    c.rc.config.max_drawdown_abs = 10.0
    c.rc.risk_desk.equity_peak = 100.0
    c.rc.risk_desk.bankroll_current = 90.0
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert c.broker.state.orders == {}
    assert _rifiuti(c)[-1] == "max_drawdown_abs_active"


def test_drawdown_rejection_releases_dedupe_for_retry(tmp_path):
    c = _catena(tmp_path)
    c.rc.config.max_drawdown_abs = True
    sig = _signal(stake=0.5, bet_type="BACK")
    c.rc._on_signal_received(sig)
    c.rc.config.max_drawdown_abs = 10.0
    c.rc.risk_desk.equity_peak = c.rc.risk_desk.bankroll_current
    c.rc._on_signal_received(sig)
    assert len(c.broker.state.orders) == 1


def test_event_cap_aggregates_distinct_markets_same_event(tmp_path):
    c = _catena(tmp_path)
    c.rc.config.max_market_exposure = 100.0
    c.rc.config.max_event_exposure_abs = 2.0
    table = c.rc.table_manager._tables[2]
    table.status = "ACTIVE"
    table.current_event_key = "other-market:key"
    table.market_id = "1.other"
    table.current_exposure = 1.5
    table.meta = {"event_name": "Roma v Milan", "event_identity": "name:roma v milan"}
    sig = _signal(stake=0.6, bet_type="BACK", market_id="1.new")
    sig["event_name"] = " ROMA   v MILAN "
    sig["event_id"] = "betfair-event-42"
    c.rc._on_signal_received(sig)
    assert c.broker.state.orders == {}
    assert _rifiuti(c)[0].startswith("max_event_exposure_abs_exceeded")


@pytest.mark.parametrize("raw", [{}, {"action": "BOTH"}, {"action": "BACK", "side": "LAY"}])
def test_telegram_ingestion_rejects_missing_invalid_or_conflicting_side(raw):
    payload = {"market_id": "1.2", "selection_id": 3, "price": 2.0, **raw}
    result = TelegramSignalProcessor().normalize_ingestion_signal(payload)
    assert result["ok"] is False
    assert result["error_code"] == "INVALID_OR_MISSING_SIDE"


def test_most_restrictive_cap_wins(tmp_path):
    c = _catena(tmp_path)
    c.rc.config.max_order_exposure = 1.0
    c.rc.config.max_open_exposure = 100.0
    c.rc._on_signal_received(_signal(stake=1.01, bet_type="BACK"))
    assert _rifiuti(c)[0].startswith("max_order_exposure_exceeded")


def test_concurrent_orders_cannot_bypass_total_cap(tmp_path):
    c = _catena(tmp_path, max_open_exposure=1.0)
    barrier = threading.Barrier(3)

    def submit(selection):
        sig = _signal(stake=0.6, bet_type="BACK", selection_id=selection)
        barrier.wait()
        c.rc._on_signal_received(sig)

    threads = [threading.Thread(target=submit, args=(n,)) for n in (101, 102)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=5)
    assert sum(1 for thread in threads if thread.is_alive()) == 0
    assert len(c.broker.state.orders) == 1
    assert any(reason.startswith("max_open_exposure_exceeded") for reason in _rifiuti(c))
