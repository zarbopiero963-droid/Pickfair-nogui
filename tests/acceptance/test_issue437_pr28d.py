"""PR28-d: cap assoluti Roserpina e input ordine fail-closed."""
from __future__ import annotations

import pytest
import threading
import time

from core import validators
from core.runtime_controller import RuntimeController
from core.system_state import RuntimeMode
from services.setting_service import SettingsService
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


def test_filled_position_stays_in_market_and_event_cap_until_settlement(tmp_path):
    c = _catena(tmp_path)
    c.rc.config.max_market_exposure = 2.0
    c.rc.config.max_event_exposure_abs = 2.0
    table = c.rc.table_manager._tables[2]
    table.status = "ACTIVE"
    table.current_event_key = "filled:event:key"
    table.market_id = "1.234"
    table.current_exposure = 1.5
    table.meta = {"event_id": "E42", "event_name": "Roma v Milan"}

    c.rc._on_quick_bet_filled({"table_id": 2, "event_key": "filled:event:key"})
    sig = _signal(stake=0.6, bet_type="BACK", market_id="1.234")
    sig.update({"event_id": "E42", "event_name": "Roma v Milan"})
    c.rc._on_signal_received(sig)

    assert c.rc.table_manager.get_table(2).current_exposure == pytest.approx(1.5)
    assert c.broker.state.orders == {}
    assert _rifiuti(c)[0].startswith("max_market_exposure_exceeded")


def test_market_cap_canonicalizes_whitespace_before_aggregation(tmp_path):
    c = _catena(tmp_path)
    c.rc.config.max_market_exposure = 2.0
    c.rc.config.max_event_exposure_abs = 100.0
    table = c.rc.table_manager._tables[2]
    table.status = "ACTIVE"
    table.current_event_key = "existing:key"
    table.market_id = "1.234"
    table.current_exposure = 1.5

    c.rc._on_signal_received(
        _signal(stake=0.6, bet_type="BACK", market_id=" 1.234 ")
    )

    assert c.broker.state.orders == {}
    assert _rifiuti(c)[0].startswith("max_market_exposure_exceeded")


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


def test_event_cap_same_id_aggregates_even_when_names_differ(tmp_path):
    c = _catena(tmp_path)
    c.rc.config.max_market_exposure = 100.0
    c.rc.config.max_event_exposure_abs = 2.0
    table = c.rc.table_manager._tables[2]
    table.status = "ACTIVE"
    table.current_event_key = "old:key"
    table.market_id = "1.old"
    table.current_exposure = 1.5
    table.meta = {"event_id": "E42", "event_name": "Roma v Milan", "event_identity": "id:E42"}
    sig = _signal(stake=0.6, bet_type="BACK", market_id="1.new")
    sig.update({"event_id": "E42", "event_name": "Roma - Milan"})
    c.rc._on_signal_received(sig)
    assert c.broker.state.orders == {}
    assert _rifiuti(c)[0].startswith("max_event_exposure_abs_exceeded")


def test_event_cap_honours_legacy_identity_alias_over_stale_display_name(tmp_path):
    c = _catena(tmp_path)
    c.rc.config.max_market_exposure = 100.0
    c.rc.config.max_event_exposure_abs = 2.0
    table = c.rc.table_manager._tables[2]
    table.status = "ACTIVE"
    table.current_event_key = "legacy:event:key"
    table.market_id = "1.old"
    table.current_exposure = 1.5
    table.meta = {
        "event_name": "Roma - Milan",
        "event_identity": "name:roma v milan",
    }
    sig = _signal(stake=0.6, bet_type="BACK", market_id="1.new")
    sig.update({"event_name": "Roma v Milan", "event_id": None})

    c.rc._on_signal_received(sig)

    assert c.broker.state.orders == {}
    assert _rifiuti(c)[0].startswith("max_event_exposure_abs_exceeded")


def test_auto_trade_payload_falls_back_to_event_id_alias_when_primary_is_blank(tmp_path):
    c = _catena(tmp_path)
    sig = _signal(stake=0.5, bet_type="BACK", market_id=" 1.234 ")
    sig.update({"event_id": None, "eventId": "E42"})

    payload = c.rc._build_auto_trade_payload(signal=sig, decision_stake=0.5)

    assert payload["event_id"] == "E42"
    assert payload["market_id"] == "1.234"
    assert payload["event_key"].startswith("1.234:")


def test_telegram_event_id_alias_reaches_runtime_event_cap(tmp_path):
    c = _catena(tmp_path)
    c.rc.config.max_market_exposure = 100.0
    c.rc.config.max_event_exposure_abs = 2.0
    table = c.rc.table_manager._tables[2]
    table.status = "ACTIVE"
    table.current_event_key = "legacy:event:key"
    table.market_id = "1.old"
    table.current_exposure = 1.5
    table.meta = {"event_id": "E42", "event_name": "Roma v Milan"}
    raw = _signal(stake=0.6, bet_type="BACK", market_id="1.new")
    raw.update({"event_id": None, "eventId": "E42", "event_name": "Roma - Milan"})

    ingested = TelegramSignalProcessor().normalize_ingestion_signal(raw)

    assert ingested["ok"] is True
    assert ingested["normalized_signal"]["event_id"] == "E42"
    c.rc._on_signal_received(ingested["normalized_signal"])
    assert c.broker.state.orders == {}
    assert _rifiuti(c)[0].startswith("max_event_exposure_abs_exceeded")


@pytest.mark.parametrize("field", [
    "max_order_exposure", "max_market_exposure", "max_event_exposure_abs",
    "max_drawdown_abs",
])
def test_blank_persisted_absolute_cap_stays_configured_and_fails_closed(field):
    key = f"roserpina.{field}"

    class _Db:
        def get_settings(self):
            return {key: ""}

        def get_all_settings(self):
            return self.get_settings()

    value = getattr(SettingsService(_Db()).load_roserpina_config(), field)
    assert validators.finite_number(value) is None
    assert value is not None


def test_auto_trade_admission_uses_same_serial_lock_as_signal_orders():
    rc = object.__new__(RuntimeController)
    rc._risk_admission_lock = threading.RLock()
    active = 0
    max_active = 0
    state_lock = threading.Lock()

    def serial(*, payload, sync_result):
        nonlocal active, max_active
        with state_lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.03)
        with state_lock:
            active -= 1
        return {"payload": payload, "sync_result": sync_result}

    rc._evaluate_and_maybe_submit_auto_next_trade_serial = serial
    threads = [threading.Thread(
        target=rc._evaluate_and_maybe_submit_auto_next_trade,
        kwargs={"payload": {"n": n}, "sync_result": {"n": n}},
    ) for n in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert max_active == 1


def test_reload_config_waits_for_risk_admission_lock(tmp_path):
    c = _catena(tmp_path)
    started = threading.Event()
    completed = threading.Event()

    def reload():
        started.set()
        c.rc.reload_config()
        completed.set()

    with c.rc._risk_admission_lock:
        thread = threading.Thread(target=reload)
        thread.start()
        assert started.wait(timeout=1)
        assert completed.wait(timeout=0.1) is False
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert completed.is_set()


def test_inflight_best_price_is_fenced_by_cashout_and_leaves_no_reservation(tmp_path):
    c = _catena(tmp_path)
    c.rc.mode = RuntimeMode.ACTIVE
    lookup_started = threading.Event()
    release_lookup = threading.Event()
    routed = threading.Event()
    original_best_price = c.rc._apply_direct_best_price

    def blocked_lookup(payload):
        lookup_started.set()
        assert release_lookup.wait(timeout=2)
        return original_best_price(payload)

    c.rc._apply_direct_best_price = blocked_lookup
    c.rc._route_cashout_signal = lambda _signal: routed.set()
    sig = _signal(stake=0.5)
    order_thread = threading.Thread(target=c.rc._on_signal_received, args=(sig,))
    order_thread.start()
    assert lookup_started.wait(timeout=2)

    cashout_thread = threading.Thread(
        target=c.rc._on_signal_received,
        args=({"signal_type": "CASHOUT", "market_id": sig["market_id"],
               "selection_id": sig["selection_id"]},),
    )
    cashout_thread.start()
    assert routed.wait(timeout=1), "cashout deve iniziare mentre il best-price è sospeso"
    cashout_thread.join(timeout=2)
    assert not cashout_thread.is_alive()
    release_lookup.set()
    order_thread.join(timeout=2)

    assert not order_thread.is_alive()
    assert c.bus.payloads("CMD_QUICK_BET") == []
    event_key = c.rc.duplication_guard.build_event_key(sig)
    assert not c.rc.duplication_guard.is_duplicate(event_key)
    assert all(t.status == "FREE" for t in c.rc.table_manager._tables.values())


def test_entry_started_during_scoped_cashout_is_rejected_but_later_entry_works(tmp_path):
    c = _catena(tmp_path)
    c.rc.mode = RuntimeMode.ACTIVE
    cashout_started = threading.Event()
    finish_cashout = threading.Event()

    def blocked_route(_signal):
        cashout_started.set()
        assert finish_cashout.wait(timeout=2)

    c.rc._route_cashout_signal = blocked_route
    target = _signal(stake=0.5)
    cashout = threading.Thread(target=c.rc._on_signal_received, args=(
        {"signal_type": "CASHOUT", "market_id": target["market_id"],
         "selection_id": target["selection_id"]},))
    cashout.start()
    assert cashout_started.wait(timeout=2)

    c.rc._on_signal_received(target)
    assert c.bus.payloads("CMD_QUICK_BET") == []

    finish_cashout.set()
    cashout.join(timeout=2)
    assert not cashout.is_alive()
    c.rc._on_signal_received(target)
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1


@pytest.mark.parametrize("other_market,other_selection", [("1.999", 999), ("1.234", 999)])
def test_nonpertinent_market_or_selection_cashout_does_not_invalidate_entry(
    tmp_path, other_market, other_selection
):
    c = _catena(tmp_path)
    c.rc.mode = RuntimeMode.ACTIVE
    lookup_started = threading.Event()
    release_lookup = threading.Event()
    routed = threading.Event()
    original_best_price = c.rc._apply_direct_best_price

    def blocked_lookup(payload):
        lookup_started.set()
        assert release_lookup.wait(timeout=2)
        return original_best_price(payload)

    c.rc._apply_direct_best_price = blocked_lookup
    c.rc._route_cashout_signal = lambda _signal: routed.set()
    sig = _signal(stake=0.5)
    order = threading.Thread(target=c.rc._on_signal_received, args=(sig,))
    order.start()
    assert lookup_started.wait(timeout=2)
    other = _signal(market_id=other_market, selection_id=other_selection)
    cashout = threading.Thread(target=c.rc._on_signal_received, args=(
        {"signal_type": "CASHOUT", "market_id": other["market_id"],
         "selection_id": other["selection_id"]},))
    cashout.start()
    assert routed.wait(timeout=1)
    cashout.join(timeout=2)
    release_lookup.set()
    order.join(timeout=2)

    assert not order.is_alive() and not cashout.is_alive()
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1


def test_risk_stop_during_unrelated_cashout_keeps_entry_blocked(tmp_path):
    c = _catena(tmp_path)
    c.rc.mode = RuntimeMode.ACTIVE
    lookup_started = threading.Event()
    release_lookup = threading.Event()
    routed = threading.Event()
    finish_cashout = threading.Event()
    original_best_price = c.rc._apply_direct_best_price

    def blocked_lookup(payload):
        lookup_started.set()
        assert release_lookup.wait(timeout=2)
        return original_best_price(payload)

    c.rc._apply_direct_best_price = blocked_lookup
    def blocked_cashout(_signal):
        routed.set()
        assert finish_cashout.wait(timeout=2)

    c.rc._route_cashout_signal = blocked_cashout
    sig = _signal(stake=0.5)
    order = threading.Thread(target=c.rc._on_signal_received, args=(sig,))
    order.start()
    assert lookup_started.wait(timeout=2)
    cashout = threading.Thread(target=c.rc._on_signal_received, args=(
        {"signal_type": "CASHOUT", "market_id": "1.999", "selection_id": 999},))
    cashout.start()
    assert routed.wait(timeout=1)
    c.rc._risk_stop_reason = "test-risk-stop"
    finish_cashout.set()
    cashout.join(timeout=2)
    release_lookup.set()
    order.join(timeout=2)
    assert not order.is_alive() and not cashout.is_alive()
    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert _rifiuti(c)[-1].startswith("risk_stop_active:")


def test_cashout_barrier_and_price_lookup_do_not_hold_admission_lock(tmp_path):
    c = _catena(tmp_path)
    c.rc.mode = RuntimeMode.ACTIVE
    lookup_started = threading.Event()
    release_lookup = threading.Event()
    routed = threading.Event()
    original_best_price = c.rc._apply_direct_best_price

    def blocked_lookup(payload):
        lookup_started.set()
        acquired = threading.Event()

        def probe_lock():
            with c.rc._risk_admission_lock:
                acquired.set()

        probe = threading.Thread(target=probe_lock)
        probe.start()
        probe.join(timeout=1)
        assert acquired.is_set(), "il lock deve essere libero durante il lookup"
        assert release_lookup.wait(timeout=2)
        return original_best_price(payload)

    c.rc._apply_direct_best_price = blocked_lookup
    c.rc._route_cashout_signal = lambda _signal: routed.set()
    order = threading.Thread(target=c.rc._on_signal_received, args=(_signal(stake=0.5),))
    order.start()
    assert lookup_started.wait(timeout=2)
    cashout = threading.Thread(target=c.rc._on_signal_received, args=(
        {"signal_type": "CASHOUT_ALL"},))
    cashout.start()
    assert routed.wait(timeout=1)
    release_lookup.set()
    cashout.join(timeout=2)
    order.join(timeout=2)
    assert not cashout.is_alive() and not order.is_alive()


def test_cashout_ingestion_does_not_require_order_side():
    result = TelegramSignalProcessor().normalize_ingestion_signal({
        "signal_type": "CASHOUT_ALL",
        "event_name": "Roma v Milan",
        "raw_text": "CASHOUT ALL",
    })
    assert result["ok"] is True
    assert result["normalized_signal"]["signal_type"] == "CASHOUT_ALL"
    assert "bet_type" not in result["normalized_signal"]


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
