"""Decisione owner 2 (PF-PR-RECOVERY-DECISIONS-02, 17/10/2026): stake sopra max_single_bet => rifiuto esplicito,
mai ridimensionamento silenzioso. RED prima della patch (il BACK veniva ridotto al cap e approvato).

Comportamento obbligatorio: calcolare lo stake richiesto, verificare i limiti applicabili, eseguire ESATTAMENTE lo
stake richiesto se consentito, altrimenti rifiutare con motivo esplicito. Nessun limite viene ignorato: il cap resta
un limite di rischio valido, cambia solo l'esito (rifiuto invece di sostituzione).
"""
import pytest

from core.money_management import RoserpinaMoneyManagement
from core.system_state import RoserpinaConfig
from tests.risk_config_fixtures import legacy_test_config


def _mm(max_single_pct=1.0):
    cfg = legacy_test_config()
    cfg.base_stake_pct = 3.0
    cfg.min_stake = 0.10
    cfg.max_single_bet_pct = max_single_pct
    cfg.max_total_exposure_pct = 100.0
    cfg.max_event_exposure_pct = 100.0
    return RoserpinaMoneyManagement(cfg)


def _calc(mm, *, side="BACK", price=2.0, br=100.0):
    return mm.calculate(signal={"price": price, "bet_type": side, "stake": 3.0}, bankroll_current=br, cap_base_cash=br,
                        equity_peak=br, current_total_exposure=0.0, event_current_exposure=0.0,
                        table={"table_id": 1, "loss_amount": 0.0, "in_recovery": False})


def test_back_stake_above_max_single_is_rejected_with_the_requested_stake_not_resized():
    decision = _calc(_mm(1.0), side="BACK")                               # cap 1 EUR, stake calcolato 3 EUR
    assert decision.approved is False and decision.reason == "supera_max_single_bet"
    assert decision.recommended_stake == pytest.approx(3.0, abs=0.01), "lo stake richiesto e' stato sostituito"
    assert decision.metadata["requested_stake"] == pytest.approx(3.0, abs=0.01)
    assert decision.metadata["max_single"] == pytest.approx(1.0, abs=0.01)


def test_lay_liability_above_max_single_is_rejected_the_same_way():
    decision = _calc(_mm(2.0), side="LAY", price=3.0)                     # liability 3*(3-1)=6 > cap 2
    assert decision.approved is False and decision.reason == "supera_max_single_bet"
    assert decision.recommended_stake == pytest.approx(3.0, abs=0.01)
    assert decision.metadata["exposure"] == pytest.approx(6.0, abs=0.01)


def test_stake_within_the_cap_is_executed_exactly():
    decision = _calc(_mm(25.0), side="BACK")
    assert decision.approved is True and decision.recommended_stake == pytest.approx(3.0, abs=0.01)


def test_stake_exactly_at_the_cap_is_allowed_above_is_refused():
    assert _calc(_mm(3.0), side="BACK").approved is True                   # 3% di 100 = 3 == cap 3
    refused = _calc(_mm(2.99), side="BACK")
    assert refused.approved is False and refused.reason == "supera_max_single_bet"


def test_no_approved_decision_ever_carries_a_stake_different_from_the_calculated_one():
    for pct in (0.5, 1.0, 2.0, 2.99, 3.0, 5.0, 25.0):
        d = _calc(_mm(pct), side="BACK")
        if d.approved:
            assert d.recommended_stake == pytest.approx(3.0, abs=0.01), pct
        else:
            assert d.reason == "supera_max_single_bet" and d.recommended_stake == pytest.approx(3.0, abs=0.01), pct
