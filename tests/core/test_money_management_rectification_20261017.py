"""PF-MICROSTAKE-OWNER-RECTIFICATION-20261017 (#426 c.6235508131) — il Money Management richiede lo stake calcolato.

RED sul codice precedente: il `_clamp` alzava in silenzio lo stake calcolato fino a `min_stake` (con min_stake 2.0 uno
stake da 0.90 diventava 2.0). Ora: lo stake calcolato e' richiesto cosi' com'e'; sotto il minimo configurato => rifiuto
ESPLICITO con lo stake richiesto in chiaro, mai una puntata diversa. Nessun cap fisso 1/2 EUR nel money management.
"""
import pytest

from core.money_management import RoserpinaMoneyManagement
from core.system_state import DeskMode, RoserpinaConfig
from tests.risk_config_fixtures import legacy_test_config


def _mm(**over):
    cfg = legacy_test_config()
    cfg.max_single_bet_pct = cfg.max_total_exposure_pct = cfg.max_event_exposure_pct = 100.0
    cfg.target_profit_cycle_pct = 3.0
    for k, v in over.items():
        setattr(cfg, k, v)
    return RoserpinaMoneyManagement(cfg)


def _calc(mm, bankroll=30.0, price=2.0):
    return mm.calculate(signal={"price": price, "side": "BACK", "bet_type": "BACK"}, bankroll_current=bankroll, cap_base_cash=bankroll,
                        equity_peak=bankroll, current_total_exposure=0.0, event_current_exposure=0.0, table=None)


@pytest.mark.parametrize("bankroll,expected", [(30.0, 0.90), (10.0, 0.30), (3.34, 0.1002), (50.0, 1.50), (200.0, 6.0)])
def test_calculated_stake_is_requested_exactly_without_rounding_to_any_minimum(bankroll, expected):
    d = _calc(_mm(min_stake=0.10), bankroll=bankroll)                    # stake = bankroll*3%/(2-1)
    assert d.approved is True and d.desk_mode is DeskMode.NORMAL
    assert d.recommended_stake == pytest.approx(expected, abs=1e-9), "stake sostituito con un importo diverso"


def test_configured_minimum_is_an_explicit_rejection_not_a_silent_round_up():
    d = _calc(_mm(min_stake=2.0), bankroll=30.0)                        # calcolato 0.90 < 2.0
    assert d.approved is False and d.reason == "stake_sotto_min_stake"
    assert d.recommended_stake == pytest.approx(0.90), "lo stake richiesto deve restare visibile"
    assert d.metadata["requested_stake"] == pytest.approx(0.90) and d.metadata["min_stake"] == 2.0


def test_stake_equal_to_configured_minimum_is_approved():
    d = _calc(_mm(min_stake=0.90), bankroll=30.0)
    assert d.approved is True and d.recommended_stake == pytest.approx(0.90)


def test_no_fixed_two_euro_floor_anywhere_in_the_decision():
    d = _calc(_mm(min_stake=0.10), bankroll=30.0)
    assert d.recommended_stake < 2.0 and d.approved is True
