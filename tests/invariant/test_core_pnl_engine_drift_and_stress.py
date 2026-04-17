import math
import random
import pytest

from pnl_engine import PnLEngine
from core.pnl_engine import PnLEngine as EventDrivenPnLEngine

_REQUIRED_CALC_API = (
    "calculate_position_pnl",
    "calculate_settlement_pnl",
    "calculate_green_up_size",
    "calculate_cashout_pnl",
    "mark_to_market_pnl",
)

# Fail-closed: if the required calculation API is absent, fail at collection
# time instead of silently skipping.  A supported implementation must pass its
# invariant suite; missing API → broken contract → CI failure.
_missing_api = [name for name in _REQUIRED_CALC_API if not hasattr(PnLEngine, name)]
if _missing_api:
    pytest.fail(
        f"pnl_engine.PnLEngine is missing required methods: {_missing_api}. "
        "This invariant suite is fail-closed and cannot silently skip for a "
        "supported PnL implementation."
    )

@pytest.mark.invariant
def test_core_pnl_position_repeat_same_input_is_stable():
    from pnl_engine import PnLEngine

    engine = PnLEngine(commission_pct=4.5)

    results = [
        engine.calculate_position_pnl(
            market_id="1.300",
            selection_id=10,
            side="BACK",
            entry_price=2.5,
            exit_price=2.0,
            size=100.0,
        )
        for _ in range(5000)
    ]

    first = results[0]
    for r in results:
        assert math.isfinite(r.gross_pnl)
        assert math.isfinite(r.commission_amount)
        assert math.isfinite(r.net_pnl)
        assert r.to_dict() == first.to_dict()


@pytest.mark.invariant
def test_core_pnl_settlement_repeat_same_input_is_stable():
    from pnl_engine import PnLEngine

    engine = PnLEngine(commission_pct=4.5)

    values = [
        engine.calculate_settlement_pnl(
            side="BACK",
            price=3.0,
            size=100.0,
            won=True,
        )
        for _ in range(5000)
    ]

    first = values[0]
    assert all(v == first for v in values)


@pytest.mark.invariant
def test_core_pnl_commission_sign_semantics_are_explicit():
    from pnl_engine import PnLEngine

    engine = PnLEngine(commission_pct=4.5)

    # Positive gross -> commission applied
    positive = engine.calculate_position_pnl(
        market_id="1.305",
        selection_id=10,
        side="BACK",
        entry_price=2.0,
        exit_price=3.0,
        size=100.0,
    )
    assert positive.gross_pnl > 0.0
    assert positive.commission_amount > 0.0

    # Zero gross -> no commission
    zero = engine.calculate_position_pnl(
        market_id="1.305",
        selection_id=10,
        side="BACK",
        entry_price=2.0,
        exit_price=2.0,
        size=100.0,
    )
    assert zero.gross_pnl == 0.0
    assert zero.commission_amount == 0.0
    assert zero.net_pnl == 0.0

    # Negative gross -> no commission
    negative = engine.calculate_position_pnl(
        market_id="1.305",
        selection_id=10,
        side="BACK",
        entry_price=2.0,
        exit_price=1.5,
        size=100.0,
    )
    assert negative.gross_pnl < 0.0
    assert negative.commission_amount == 0.0
    assert negative.net_pnl == negative.gross_pnl


@pytest.mark.invariant
def test_core_pnl_commission_fixed_4p5_reference_and_no_double_commission():
    from pnl_engine import PnLEngine

    engine = PnLEngine(commission_pct=4.5)
    win = engine.calculate_settlement_pnl(side="BACK", price=3.0, size=100.0, won=True)
    zero = engine.calculate_settlement_pnl(side="LAY", price=2.0, size=0.000001, won=False)

    assert math.isfinite(win["gross_pnl"])
    assert math.isfinite(win["commission_amount"])
    assert math.isfinite(win["net_pnl"])
    assert abs(win["commission_amount"] - (win["gross_pnl"] * 0.045)) < 1e-12
    assert abs(win["net_pnl"] - (win["gross_pnl"] * 0.955)) < 1e-12

    # No double-commission: net + commission must reconstruct gross.
    assert abs((win["net_pnl"] + win["commission_amount"]) - win["gross_pnl"]) < 1e-12
    assert zero["commission_amount"] == 0.0


@pytest.mark.invariant
def test_event_driven_pnl_commission_sign_semantics_match_contract():
    engine = EventDrivenPnLEngine(bus=None, commission_pct=4.5)

    pos = {
        "event_key": "E-SIGN",
        "market_id": "1.450",
        "selection_id": 77,
        "side": "BACK",
        "price": 2.0,
        "stake": 100.0,
        "table_id": 1,
        "batch_id": "B",
    }

    def _book(lay: float) -> dict:
        return {
            "marketId": "1.450",
            "runners": [
                {
                    "selectionId": 77,
                    "ex": {
                        "availableToBack": [{"price": 2.0}],
                        "availableToLay": [{"price": lay}],
                    },
                }
            ],
        }

    pos_pnl = engine._calc(dict(pos), _book(1.5))
    zero_pnl = engine._calc(dict(pos), _book(2.0))
    neg_pnl = engine._calc(dict(pos), _book(2.5))

    assert math.isfinite(pos_pnl)
    assert math.isfinite(zero_pnl)
    assert math.isfinite(neg_pnl)
    assert abs(pos_pnl - 47.75) < 1e-12
    assert zero_pnl == 0.0
    assert abs(neg_pnl - (-50.0)) < 1e-12


@pytest.mark.chaos
def test_core_pnl_numeric_stress_extreme_ranges_are_finite():
    from pnl_engine import PnLEngine

    engine = PnLEngine(commission_pct=4.5)

    cases = [
        ("BACK", 1.01, 1000.0, 1e-6),
        ("BACK", 1000.0, 1.01, 1e6),
        ("LAY", 1.01, 1000.0, 1e6),
        ("LAY", 1000.0, 1.01, 1e-6),
    ]

    for side, entry, exit_price, size in cases:
        result = engine.calculate_position_pnl(
            market_id="1.301",
            selection_id=11,
            side=side,
            entry_price=entry,
            exit_price=exit_price,
            size=size,
        )

        assert math.isfinite(result.gross_pnl)
        assert math.isfinite(result.commission_amount)
        assert math.isfinite(result.net_pnl)


@pytest.mark.chaos
def test_core_pnl_randomized_stress_position_model_stays_finite():
    from pnl_engine import PnLEngine

    engine = PnLEngine(commission_pct=4.5)
    rng = random.Random(9001)

    for _ in range(20000):
        side = "BACK" if rng.random() < 0.5 else "LAY"
        entry_price = rng.uniform(1.01, 1000.0)
        exit_price = rng.uniform(1.01, 1000.0)
        size = rng.uniform(1e-6, 1e6)

        result = engine.calculate_position_pnl(
            market_id="1.302",
            selection_id=12,
            side=side,
            entry_price=entry_price,
            exit_price=exit_price,
            size=size,
        )

        assert math.isfinite(result.gross_pnl)
        assert math.isfinite(result.commission_amount)
        assert math.isfinite(result.net_pnl)


@pytest.mark.invariant
def test_core_pnl_green_up_and_cashout_are_finite_under_extremes():
    from pnl_engine import PnLEngine

    engine = PnLEngine(commission_pct=4.5)

    cases = [
        ("BACK", 2.5, 100.0, 2.0),
        ("LAY", 3.0, 250.0, 2.2),
        ("BACK", 1000.0, 1e6, 1.01),
        ("LAY", 1.02, 1e-3, 1000.0),
    ]

    for side, entry_price, entry_size, hedge_price in cases:
        hedge_size = engine.calculate_green_up_size(
            entry_side=side,
            entry_price=entry_price,
            entry_size=entry_size,
            hedge_price=hedge_price,
        )
        cashout = engine.calculate_cashout_pnl(
            entry_side=side,
            entry_price=entry_price,
            entry_size=entry_size,
            hedge_price=hedge_price,
        )

        assert math.isfinite(hedge_size)
        assert hedge_size > 0.0
        assert math.isfinite(cashout["hedge_size"])
        assert math.isfinite(cashout["gross_pnl"])
        assert math.isfinite(cashout["commission_amount"])
        assert math.isfinite(cashout["net_pnl"])


@pytest.mark.invariant
def test_core_pnl_mark_to_market_repeat_read_is_stable():
    from pnl_engine import PnLEngine

    engine = PnLEngine(commission_pct=4.5)

    values = [
        engine.mark_to_market_pnl(
            side="BACK",
            entry_price=2.5,
            current_price=2.2,
            size=100.0,
        )
        for _ in range(5000)
    ]

    first = values[0]
    assert all(math.isfinite(v) for v in values)
    assert all(abs(v - first) < 1e-12 for v in values)


@pytest.mark.invariant
def test_two_pnl_engines_have_separate_contract_surfaces():
    # Calculation PnL engine contract (top-level pnl_engine.py)
    assert hasattr(PnLEngine, "calculate_position_pnl")
    assert hasattr(PnLEngine, "calculate_settlement_pnl")
    assert not hasattr(PnLEngine, "_on_market")

    # Event-driven PnL engine contract (core/pnl_engine.py)
    assert hasattr(EventDrivenPnLEngine, "_on_market")
    assert hasattr(EventDrivenPnLEngine, "_on_filled")
    assert not hasattr(EventDrivenPnLEngine, "calculate_position_pnl")
