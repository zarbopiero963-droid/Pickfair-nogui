"""Regression tests for PR5 numeric guardrails."""

from typing import cast

import pytest  # type: ignore[reportMissingImports]

from core.money_management import RoserpinaMoneyManagement
from core.system_state import RoserpinaConfig
from dutching import calculate_dutching_stakes
from pnl_engine import PnLEngine


@pytest.mark.unit
def test_mm_inputs_fail_closed() -> None:
    """Money management should reject non-finite numeric inputs."""
    manager = RoserpinaMoneyManagement(RoserpinaConfig())
    decision = manager.calculate(
        signal={"price": 2.0},
        bankroll_current=float("nan"),
        equity_peak=float("inf"),
        current_total_exposure=float("nan"),
        event_current_exposure=float("inf"),
        table={"id": 1, "loss": float("nan")},
    )
    if decision.approved:
        pytest.fail("decision must be rejected for non-finite inputs")
    assert decision.recommended_stake == pytest.approx(0.0), "recommended stake must be exactly zero"


@pytest.mark.unit
def test_dutching_rejects_nonfinite() -> None:
    """Dutching should fail closed on non-finite odds and stake."""
    bad_odds = calculate_dutching_stakes([2.0, float("inf")], 100.0)
    bad_stake = calculate_dutching_stakes([2.0, 3.0], float("nan"))

    if bad_odds["stakes"] != []:
        pytest.fail("expected no stakes for invalid odds")
    if bad_odds.get("error") != "Invalid odds <= 1.0":
        pytest.fail("expected invalid-odds error")
    if bad_stake["stakes"] != []:
        pytest.fail("expected no stakes for invalid stake")


@pytest.mark.unit
@pytest.mark.parametrize(
    "odds",
    [
        ["NaN", "2.0"],
        ["nan", "2.0"],
        [" INF ", "2.0"],
        ["+infinity", "2.0"],
        ["-INF", "2.0"],
    ],
)
def test_dutching_rejects_bad_str(odds: list[str]) -> None:
    """Dutching should reject textual NaN/Inf odds variants."""
    result = calculate_dutching_stakes(cast(list[float], odds), cast(float, "100.0"))
    if result["stakes"] != []:
        pytest.fail("expected no stakes for invalid textual odds")
    if "Invalid odds" not in (result.get("error") or ""):
        pytest.fail("expected invalid-odds error for textual non-finite values")


@pytest.mark.unit
@pytest.mark.parametrize("stake", ["NaN", "nan", " INF ", "+infinity", "-INF"])
def test_dutching_rejects_bad_stake(stake: str) -> None:
    """Dutching should reject textual NaN/Inf stake variants."""
    result = calculate_dutching_stakes(cast(list[float], ["2.0", "3.0"]), cast(float, stake))
    if result["stakes"] != []:
        pytest.fail("expected no stakes for invalid textual stake")


@pytest.mark.unit
def test_dutching_accepts_norm_text() -> None:
    """Dutching should normalize comma/space formatted inputs."""
    result = calculate_dutching_stakes(
        cast(list[float], ["2,0", " 3,0 "]),
        cast(float, " 100,0 "),
    )
    if not result["stakes"]:
        pytest.fail("expected stake allocation for normalized textual values")
    if result.get("error"):
        pytest.fail("did not expect error for normalized textual values")


@pytest.mark.unit
def test_dutching_allocation_valid() -> None:
    """Valid dutching allocation should still sum to total stake."""
    result = calculate_dutching_stakes([2.0, 4.0], 100.0, commission=0.0)
    if len(result["stakes"]) != 2:
        pytest.fail("expected two stake entries")
    if abs(sum(result["stakes"]) - 100.0) >= 0.01:
        pytest.fail("stakes should sum to total stake within rounding tolerance")


@pytest.mark.unit
def test_pnl_rejects_nonfinite_prev() -> None:
    """Position preview should reject non-finite entry values."""
    engine = PnLEngine(commission_pct=4.5)
    with pytest.raises(ValueError):
        engine.calculate_position_pnl(
            market_id="1.1",
            selection_id=1,
            side="BACK",
            entry_price=float("nan"),
            exit_price=2.0,
            size=10.0,
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    "price,size",
    [
        (float("nan"), 1.0),
        (2.0, float("inf")),
        (2.0, float("-inf")),
    ],
)
def test_settlement_rejects_bad_num(price: float, size: float) -> None:
    """Settlement calculation should reject non-finite numbers."""
    engine = PnLEngine(commission_pct=4.5)
    with pytest.raises(ValueError):
        engine.calculate_settlement_pnl(side="BACK", price=price, size=size, won=True)


@pytest.mark.unit
@pytest.mark.parametrize(
    "price,size",
    [
        (float("nan"), 1.0),
        (2.0, float("inf")),
        (2.0, float("-inf")),
    ],
)
def test_settlement_bad_num_lay(price: float, size: float) -> None:
    """Settlement calculation should reject non-finite numbers for LAY side too."""
    engine = PnLEngine(commission_pct=4.5)
    with pytest.raises(ValueError):
        engine.calculate_settlement_pnl(side="LAY", price=price, size=size, won=False)


@pytest.mark.unit
@pytest.mark.parametrize(
    "entry_price,entry_size,hedge_price",
    [
        (float("nan"), 1.0, 2.0),
        (1.5, float("inf"), 2.0),
        (1.5, 1.0, float("-inf")),
    ],
)
def test_green_up_rejects_nonfinite(
    entry_price: float,
    entry_size: float,
    hedge_price: float,
) -> None:
    """Green-up helper should reject non-finite numbers."""
    engine = PnLEngine(commission_pct=4.5)
    with pytest.raises(ValueError):
        engine.calculate_green_up_size(
            entry_side="BACK",
            entry_price=entry_price,
            entry_size=entry_size,
            hedge_price=hedge_price,
        )


@pytest.mark.unit
def test_engine_rejects_bad_comm() -> None:
    """Engine should reject non-finite commission during computation."""
    with pytest.raises(ValueError):
        PnLEngine(commission_pct=float("nan")).calculate_position_pnl(
            market_id="1.1",
            selection_id=1,
            side="BACK",
            entry_price=2.0,
            exit_price=2.2,
            size=10.0,
        )


@pytest.mark.unit
@pytest.mark.parametrize("commission_pct", [float("nan"), float("inf"), float("-inf")])
def test_engine_rejects_comm_pct(commission_pct: float) -> None:
    """Explicit commission override should reject non-finite values."""
    engine = PnLEngine(commission_pct=4.5)
    with pytest.raises(ValueError):
        engine.calculate_position_pnl(
            market_id="1.1",
            selection_id=1,
            side="BACK",
            entry_price=2.0,
            exit_price=2.2,
            size=10.0,
            commission_pct=commission_pct,
        )


@pytest.mark.unit
def test_pnl_valid_output_unchanged() -> None:
    """Finite valid input path should preserve expected PnL numbers."""
    engine = PnLEngine(commission_pct=4.5)
    result = engine.calculate_position_pnl(
        market_id="1.1",
        selection_id=1,
        side="BACK",
        entry_price=2.0,
        exit_price=3.0,
        size=10.0,
    )
    if result.gross_pnl != pytest.approx(5.0):
        pytest.fail("unexpected gross pnl")
    if result.commission_amount != pytest.approx(0.225):
        pytest.fail("unexpected commission amount")
    if result.net_pnl != pytest.approx(4.775):
        pytest.fail("unexpected net pnl")


@pytest.mark.unit
def test_pnl_engine_rejects_non_finite_commission_pct():
    with pytest.raises(ValueError):
        PnLEngine(commission_pct=float("nan"))

@pytest.mark.unit
@pytest.mark.parametrize(
    "odds",
    [
        ["NaN", "2.0"],
        ["nan", "2.0"],
        [" INF ", "2.0"],
        ["+infinity", "2.0"],
        ["-INF", "2.0"],
    ],
)
def test_dutching_rejects_non_finite_string_odds(odds):
    result = calculate_dutching_stakes(odds, "100.0")

    assert result["stakes"] == []
    assert "Invalid odds" in (result.get("error") or "")


@pytest.mark.unit
@pytest.mark.parametrize(
    "stake",
    [
        "NaN",
        "nan",
        " INF ",
        "+infinity",
        "-INF",
    ],
)
def test_dutching_rejects_non_finite_string_stakes(stake):
    result = calculate_dutching_stakes(["2.0", "3.0"], stake)

    assert result["stakes"] == []
    assert "Invalid stake" in (result.get("error") or "")


@pytest.mark.unit
def test_dutching_accepts_normalized_string_inputs():
    result = calculate_dutching_stakes(["2,0", " 3,0 "], " 100,0 ")

    assert result["stakes"]
    assert not (result.get("error") or "")
