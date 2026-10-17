from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor

import pytest

from core.microstake_safe import (
    Fault,
    JsonSagaStore,
    MicrostakeCoordinator,
    MicrostakePolicy,
    MicrostakeRequest,
    Phase,
    Side,
    StatefulExchangeSimulator,
    market_commission,
)


D = Decimal


def policy(**changes):
    values = dict(
        simulation_only=True,
        minimum_economic_stake=D("0.10"),
        exchange_stub_size=D("2.00"),
        max_transient_exposure=D("10.00"),
        commission_rate=D("0.045"),
    )
    values.update(changes)
    return MicrostakePolicy(**values)


def request(*, side=Side.BACK, stake="0.10", price="2.00", ref="ms-1"):
    return MicrostakeRequest(
        market_id="1.100", selection_id=7, side=side,
        economic_stake=D(stake), target_price=D(price), customer_ref=ref,
    )


def coordinator(tmp_path, exchange=None, **policy_changes):
    exchange = exchange or StatefulExchangeSimulator(balance=D("100"))
    store = JsonSagaStore(tmp_path / "microstake-sagas.json")
    return MicrostakeCoordinator(exchange, store, policy(**policy_changes)), exchange, store


def test_red_lay_extreme_stub_is_rejected_before_place(tmp_path):
    c, ex, _ = coordinator(tmp_path, max_transient_exposure=D("5"))
    result = c.execute(request(side=Side.LAY), stub_price=D("1000"))
    assert result.phase is Phase.BLOCKED
    assert result.reason == "transient_exposure_cap"
    assert ex.operation_log == []


@pytest.mark.parametrize("matched", [D("2.00"), D("0.75")])
def test_red_matched_stub_never_becomes_microstake_success(tmp_path, matched):
    ex = StatefulExchangeSimulator(balance=D("100"))
    ex.queue_fault(Fault.MATCH_ON_PLACE, matched)
    c, ex, _ = coordinator(tmp_path, exchange=ex)
    result = c.execute(request(ref=f"matched-{matched}"), stub_price=D("3.00"))
    assert result.phase is Phase.AMBIGUOUS
    assert result.matched_size == matched
    assert result.residual_exposure > 0
    assert not any(op[0] == "REPLACE" for op in ex.operation_log)
    assert result.reservation_held


def test_red_match_between_place_and_cancel_is_detected(tmp_path):
    ex = StatefulExchangeSimulator(balance=D("100"))
    ex.queue_fault(Fault.MATCH_BEFORE_CANCEL, D("0.40"))
    c, _, _ = coordinator(tmp_path, exchange=ex)
    result = c.execute(request(ref="race-cancel"), stub_price=D("3.00"))
    assert result.phase is Phase.AMBIGUOUS
    assert result.matched_size == D("0.40")
    assert result.reservation_held


def test_red_lost_place_response_is_reconciled_without_duplicate(tmp_path):
    ex = StatefulExchangeSimulator(balance=D("100"))
    ex.queue_fault(Fault.LOST_PLACE_RESPONSE)
    c, ex, _ = coordinator(tmp_path, exchange=ex)
    result = c.execute(request(ref="lost-place"), stub_price=D("3.00"))
    assert result.phase is Phase.ACTIVE
    assert len([op for op in ex.operation_log if op[0] == "PLACE"]) == 1


def test_red_unresolved_lost_response_stays_fail_closed_across_restart(tmp_path):
    ex = StatefulExchangeSimulator(balance=D("100"), hide_reconciliation=True)
    ex.queue_fault(Fault.LOST_PLACE_RESPONSE)
    c, ex, store = coordinator(tmp_path, exchange=ex)
    first = c.execute(request(ref="lost-hidden"), stub_price=D("3.00"))
    assert first.phase is Phase.AMBIGUOUS and first.reservation_held

    restarted = MicrostakeCoordinator(ex.restart(), JsonSagaStore(store.path), policy())
    second = restarted.recover("lost-hidden")
    assert second.phase is Phase.AMBIGUOUS and second.reservation_held
    assert len([op for op in ex.operation_log if op[0] == "PLACE"]) == 1


def test_red_cleanup_uses_persisted_identity_not_price(tmp_path):
    ex = StatefulExchangeSimulator(balance=D("100"))
    legitimate = ex.seed_order(
        market_id="1.100", selection_id=99, side=Side.BACK,
        size=D("4"), price=D("3"), customer_ref="legitimate",
    )
    ex.queue_fault(Fault.FAIL_REPLACE)
    c, ex, _ = coordinator(tmp_path, exchange=ex)
    result = c.execute(request(ref="cleanup-id"), stub_price=D("3"))
    assert result.phase is Phase.ROLLED_BACK
    assert ex.order(legitimate).remaining == D("4")
    assert all(op[1] != legitimate for op in ex.operation_log if op[0] == "CANCEL")


@pytest.mark.parametrize("stake", ["0.10", "0.20", "0.30", "0.40", "0.50", "1.00", "1.50", "1.99"])
@pytest.mark.parametrize("side", [Side.BACK, Side.LAY])
def test_amount_matrix_unmatched_finishes_exact_economic_size(tmp_path, stake, side):
    c, _, _ = coordinator(tmp_path)
    result = c.execute(request(side=side, stake=stake, ref=f"{side}-{stake}"), stub_price=D("3"))
    assert result.phase is Phase.ACTIVE
    assert result.final_unmatched_size == D(stake)
    assert result.matched_size == 0
    assert result.max_transient_exposure <= D("10")
    assert result.commission_rate == D("0.045")


def test_component_is_automatic_in_simulation_by_default(tmp_path):
    ex = StatefulExchangeSimulator(balance=D("100"))
    c = MicrostakeCoordinator(
        ex,
        JsonSagaStore(tmp_path / "s.json"),
        MicrostakePolicy(max_transient_exposure=D("10")),
    )
    result = c.execute(request(), stub_price=D("3"))
    assert result.phase is Phase.ACTIVE
    assert [op[0] for op in ex.operation_log] == ["PLACE", "CANCEL", "REPLACE"]


def test_non_sim_adapter_is_blocked_without_exchange_call(tmp_path):
    ex = StatefulExchangeSimulator(balance=D("100"))
    c = MicrostakeCoordinator(
        ex,
        JsonSagaStore(tmp_path / "s.json"),
        MicrostakePolicy(simulation_only=False, max_transient_exposure=D("10")),
    )
    result = c.execute(request(), stub_price=D("3"))
    assert result.phase is Phase.BLOCKED
    assert result.reason == "adapter_mode_not_allowed"
    assert ex.operation_log == []


@pytest.mark.parametrize(
    "side,stub_price,book_price",
    [(Side.BACK, "1.01", "2.00"), (Side.LAY, "1000", "2.00")],
)
def test_historical_extreme_stub_crosses_book_and_is_never_success(tmp_path, side, stub_price, book_price):
    ex = StatefulExchangeSimulator(balance=D("5000"))
    ex.set_liquidity("1.100", 7, side, D(book_price), D("2"))
    c, _, _ = coordinator(tmp_path, exchange=ex, max_transient_exposure=D("5000"))
    result = c.execute(request(side=side, ref=f"cross-{side}"), stub_price=D(stub_price))
    assert result.phase is Phase.AMBIGUOUS
    assert result.matched_size == D("2")
    assert result.residual_exposure > 0


def test_aggregate_transient_cap_blocks_second_table_before_place(tmp_path):
    ex = StatefulExchangeSimulator(balance=D("100"))
    c1, _, _ = coordinator(tmp_path, exchange=ex, max_transient_exposure=D("3"))
    first = c1.execute(request(stake="1.50", ref="table-1"), stub_price=D("3"))
    assert first.phase is Phase.ACTIVE
    c2 = MicrostakeCoordinator(ex, JsonSagaStore(tmp_path / "second.json"),
                               policy(max_transient_exposure=D("3")))
    second = c2.execute(request(stake="0.10", ref="table-2"), stub_price=D("3"))
    assert second.phase is Phase.BLOCKED
    assert len([op for op in ex.operation_log if op[0] == "PLACE"]) == 1


def test_timeout_before_acceptance_releases_reservation_and_creates_no_order(tmp_path):
    ex = StatefulExchangeSimulator(balance=D("100"))
    ex.queue_fault(Fault.TIMEOUT_PLACE)
    c, ex, _ = coordinator(tmp_path, exchange=ex)
    result = c.execute(request(ref="timeout-before"), stub_price=D("3"))
    assert result.phase is Phase.FAILED
    assert not result.reservation_held
    assert ex.find_by_ref("timeout-before") is None


def test_insufficient_funds_is_fail_closed(tmp_path):
    ex = StatefulExchangeSimulator(balance=D("1"))
    c, ex, _ = coordinator(tmp_path, exchange=ex)
    result = c.execute(request(ref="no-funds"), stub_price=D("3"))
    assert result.phase is Phase.FAILED
    assert result.reason == "INSUFFICIENT_FUNDS"
    assert ex.find_by_ref("no-funds") is None


def test_duplicate_execute_reconciles_without_second_place(tmp_path):
    c, ex, _ = coordinator(tmp_path)
    req = request(ref="dedupe")
    assert c.execute(req, stub_price=D("3")).phase is Phase.ACTIVE
    assert c.execute(req, stub_price=D("3")).phase is Phase.ACTIVE
    assert len([op for op in ex.operation_log if op[0] == "PLACE"]) == 1


@pytest.mark.parametrize(
    "net,expected", [("10", "0.450"), ("0", "0"), ("-10", "0")]
)
def test_commission_is_45_percent_of_positive_net_market_profit_only(net, expected):
    assert market_commission(D(net)) == D(expected)


def test_concurrent_tables_reserve_atomically_and_never_exceed_cap(tmp_path):
    ex = StatefulExchangeSimulator(balance=D("100"))

    def run(number):
        c = MicrostakeCoordinator(
            ex, JsonSagaStore(tmp_path / f"table-{number}.json"),
            policy(max_transient_exposure=D("2")),
        )
        return c.execute(request(stake="0.10", ref=f"table-{number}"), stub_price=D("3"))

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(run, (1, 2)))
    assert sorted(r.phase.value for r in results) == ["ACTIVE", "BLOCKED"]
    assert len([op for op in ex.operation_log if op[0] == "PLACE"]) == 1


def test_lost_cancel_response_reconciles_after_restart_without_second_place(tmp_path):
    ex = StatefulExchangeSimulator(balance=D("100"))
    ex.queue_fault(Fault.LOST_CANCEL_RESPONSE)
    c, ex, store = coordinator(tmp_path, exchange=ex)
    first = c.execute(request(stake="0.30", ref="lost-cancel"), stub_price=D("3"))
    assert first.phase is Phase.AMBIGUOUS and first.reservation_held
    recovered = MicrostakeCoordinator(ex.restart(), JsonSagaStore(store.path), policy()).recover("lost-cancel")
    assert recovered.phase is Phase.ACTIVE
    assert recovered.final_unmatched_size == D("0.30")
    assert len([op for op in ex.operation_log if op[0] == "PLACE"]) == 1


def test_lost_replace_response_is_reconciled_without_replacing_twice(tmp_path):
    ex = StatefulExchangeSimulator(balance=D("100"))
    ex.queue_fault(Fault.LOST_REPLACE_RESPONSE)
    c, ex, store = coordinator(tmp_path, exchange=ex)
    first = c.execute(request(stake="0.40", ref="lost-replace"), stub_price=D("3"))
    assert first.phase is Phase.AMBIGUOUS and first.reservation_held
    recovered = MicrostakeCoordinator(ex.restart(), JsonSagaStore(store.path), policy()).recover("lost-replace")
    assert recovered.phase is Phase.ACTIVE
    assert len([op for op in ex.operation_log if op[0] == "REPLACE"]) == 1


def test_market_suspension_creates_no_order(tmp_path):
    ex = StatefulExchangeSimulator(balance=D("100"))
    ex.queue_fault(Fault.SUSPEND)
    c, ex, _ = coordinator(tmp_path, exchange=ex)
    result = c.execute(request(ref="suspended"), stub_price=D("3"))
    assert result.phase is Phase.FAILED
    assert result.reason == "MARKET_SUSPENDED"
    assert ex.find_by_ref("suspended") is None

