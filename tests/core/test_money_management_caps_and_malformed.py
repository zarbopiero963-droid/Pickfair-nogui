"""PR-E programma test hedge-fund grade: caps MM e input malformati.

Matrice diretta su RoserpinaMoneyManagement (finora testato solo
indirettamente via parity/auto-trade): cap singola bet, quota invalida,
desk mode LOCKDOWN/DEFENSE, esposizioni piene, recovery sotto cap, e
input avversari (NaN/inf/stringhe) che non devono MAI esplodere ne'
produrre stake non finiti o negativi (TOP30 #22-24).

Dedup verificato: parity live/sim, auto-trade gate e bankroll sync sono
GIA' coperti (test_money_management_live_sim_parity, test_auto_trade_mm_
gate, test_bankroll_sync); qui solo la matrice unit mancante.
"""
from __future__ import annotations

import math

import pytest

from core.money_management import RoserpinaMoneyManagement
from core.system_state import RoserpinaConfig


def _mm(**overrides):
    cfg = RoserpinaConfig(
        target_profit_cycle_pct=3.0,
        max_single_bet_pct=25.0,
        max_total_exposure_pct=50.0,
        max_event_exposure_pct=25.0,
        **overrides,
    )
    return RoserpinaMoneyManagement(cfg)


def _calc(mm, price, br=100.0, peak=100.0, tot=0.0, ev=0.0, table=None):
    return mm.calculate(
        signal={"price": price},
        bankroll_current=br,
        equity_peak=peak,
        current_total_exposure=tot,
        event_current_exposure=ev,
        table=table or {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
    )


# ---------------------------------------------------------------------------
# CAPS
# ---------------------------------------------------------------------------

@pytest.mark.core
def test_recovery_stake_never_exceeds_single_bet_cap():
    """Tavolo in recovery con perdita grossa: lo stake calcolato sarebbe
    ~103 ma il cap del 25% su bankroll 100 lo blocca a 25."""
    mm = _mm()
    decision = _calc(
        mm, price=2.0,
        table={"table_id": 3, "loss_amount": 100.0, "in_recovery": True},
    )
    assert decision.recommended_stake <= 25.0
    assert decision.recommended_stake > 0


@pytest.mark.core
def test_normal_stake_is_target_based_and_under_cap():
    mm = _mm()
    decision = _calc(mm, price=2.0)
    # target 3% di 100 a quota 2.0 -> 3 EUR, ben sotto il cap del 25%.
    assert decision.approved is True
    assert decision.recommended_stake == pytest.approx(3.0, abs=0.01)
    assert decision.recommended_stake <= 25.0


# ---------------------------------------------------------------------------
# QUOTA INVALIDA
# ---------------------------------------------------------------------------

@pytest.mark.core
@pytest.mark.parametrize("price", [1.0, 0.5, 0.0, -2.0])
def test_price_at_or_below_one_blocks_with_zero_stake(price):
    mm = _mm()
    decision = _calc(mm, price=price)
    assert decision.approved is False
    assert decision.recommended_stake == 0.0
    assert decision.reason == "quota_non_valida"


# ---------------------------------------------------------------------------
# DESK MODE
# ---------------------------------------------------------------------------

@pytest.mark.core
def test_lockdown_drawdown_blocks_with_zero_stake():
    """Drawdown 21% >= lockdown (20%): stake 0, reason esplicita."""
    mm = _mm()
    decision = _calc(mm, price=2.0, br=79.0, peak=100.0)
    assert decision.approved is False
    assert decision.recommended_stake == 0.0
    assert decision.reason == "desk_lockdown"


@pytest.mark.core
def test_defense_drawdown_reduces_stake_below_normal():
    """Drawdown 10% (tra defense 7.5% e lockdown 20%): stake ridotto
    rispetto al NORMAL, mai azzerato."""
    mm = _mm()
    normal = _calc(mm, price=2.0, br=100.0, peak=100.0)
    defense = _calc(mm, price=2.0, br=90.0, peak=100.0)
    assert defense.approved is True
    assert 0 < defense.recommended_stake < normal.recommended_stake


# ---------------------------------------------------------------------------
# ESPOSIZIONI PIENE
# ---------------------------------------------------------------------------

@pytest.mark.core
def test_full_total_exposure_blocks():
    mm = _mm()
    decision = _calc(mm, price=2.0, tot=50.0)  # 50% di 100 gia' impegnato
    assert decision.approved is False
    assert decision.recommended_stake == 0.0
    assert decision.reason == "supera_max_total_exposure"


@pytest.mark.core
def test_full_event_exposure_blocks():
    mm = _mm()
    decision = _calc(mm, price=2.0, ev=25.0)  # 25% evento gia' impegnato
    assert decision.approved is False
    assert decision.recommended_stake == 0.0
    assert decision.reason == "supera_max_event_exposure"


# ---------------------------------------------------------------------------
# INPUT MALFORMATI (mai esplodere, mai stake non finiti o negativi)
# ---------------------------------------------------------------------------

@pytest.mark.core
@pytest.mark.parametrize("bad_price,expected_reason", [
    (float("nan"), "quota_non_valida"),
    (float("inf"), None),               # inf -> safe default, reason variabile
    ("abc", "quota_non_valida"),
    (None, "quota_non_valida"),
])
def test_malformed_price_is_fail_safe(bad_price, expected_reason):
    mm = _mm()
    decision = _calc(mm, price=bad_price)
    assert decision.recommended_stake == 0.0 or math.isfinite(decision.recommended_stake)
    if expected_reason is not None:
        assert decision.approved is False
        assert decision.reason == expected_reason


@pytest.mark.core
@pytest.mark.parametrize("bad_bankroll", [float("nan"), float("inf"), float("-inf"), "garbage", None, -100.0, 0.0])
def test_malformed_bankroll_blocks_with_zero_stake(bad_bankroll):
    mm = _mm()
    decision = _calc(mm, price=2.0, br=bad_bankroll)
    assert decision.approved is False
    assert decision.recommended_stake == 0.0
    assert decision.reason == "bankroll_non_valido"


@pytest.mark.core
@pytest.mark.invariant
def test_adversarial_grid_never_produces_invalid_stake():
    """Proprieta' globale (TOP30 #24): su tutta la griglia avversaria lo
    stake raccomandato e' SEMPRE un float finito >= 0, e quando la
    decisione non e' approvata lo stake e' SEMPRE 0."""
    mm = _mm()
    adversarial = [
        float("nan"), float("inf"), float("-inf"),
        -1.0, 0.0, 1.0, 2.5, "abc", None, 1e308,
    ]
    for price in adversarial:
        for br in adversarial:
            for exposure in (0.0, float("nan"), -5.0, 1e308):
                decision = _calc(mm, price=price, br=br, tot=exposure, ev=exposure)
                stake = float(decision.recommended_stake)
                assert math.isfinite(stake), f"stake non finito per price={price!r} br={br!r}"
                assert stake >= 0.0, f"stake negativo per price={price!r} br={br!r}"
                if not decision.approved:
                    assert stake == 0.0
                assert decision.reason, "reason sempre presente"
