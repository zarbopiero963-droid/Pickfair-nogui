"""Priorita' 2 — oracoli Decimal indipendenti: soglie esatte, BACK/LAY, liability, commissione Italia, microstake,
stake proporzionale (capitalizzazione). Ogni confronto con il codice di prodotto e' esatto.

RO-DEC-01 (Grok D-3): `validators.exceeds_cap` ammette un importo OLTRE il limite entro una tolleranza relativa 1e-9
           (60.0000000001 su 60): viola «nessuna tolleranza nascosta che consenta di superare un limite».
RO-DEC-02: il rischio LAY e la commissione sono float binari (0.9988 -> 0.9987999999999999): rappresentazione, non
           importo; nessun effetto al centesimo dimostrato, registrato come P3.
"""
from __future__ import annotations

from decimal import Decimal
from fractions import Fraction

import pytest

from core import stake_precision, validators
from core.money_management import RoserpinaMoneyManagement
from core.pnl_engine import PnLEngine
from tests.risk_config_fixtures import legacy_test_config
from tests.roserpina_oracle import oracle as O


# ------------------------------------------------------------------ soglie esatte
@pytest.mark.parametrize("amount,cap", [("60", "60"), ("59.99", "60"), ("0.10", "0.10"), ("0", "60")])
def test_cap_ammette_fino_al_limite_esatto(amount, cap) -> None:
    assert O.admitted_by_cap(amount, cap)
    assert validators.exceeds_cap(float(amount), float(cap)) is False


@pytest.mark.parametrize("amount,cap", [("60.01", "60"), ("60.000001", "60"), ("0.11", "0.10")])
def test_cap_rifiuta_oltre_il_limite(amount, cap) -> None:
    assert not O.admitted_by_cap(amount, cap)
    assert validators.exceeds_cap(float(amount), float(cap)) is True


@pytest.mark.xfail(strict=True, reason="RO-DEC-01: tolleranza relativa 1e-9 in exceeds_cap")
@pytest.mark.parametrize("amount,cap", [("60.0000000001", "60"), ("60.00000005", "60"), ("100000.00009", "100000")])
def test_ro_dec_01_nessuna_tolleranza_oltre_il_limite(amount, cap) -> None:
    assert not O.admitted_by_cap(amount, cap)
    assert validators.exceeds_cap(float(amount), float(cap)) is True, f"{amount} ammesso con limite {cap}"


@pytest.mark.xfail(strict=True, reason="RO-DEC-01: il cap assoluto Roserpina eredita la tolleranza")
def test_ro_dec_01b_cap_assoluto_e_cap_percentuale_del_mm() -> None:
    assert validators.absolute_cap_reason("max_order_exposure", 60, 0, 60.0000000001) != ""
    mm = RoserpinaMoneyManagement(legacy_test_config(max_single_bet_pct=5.0, max_total_exposure_pct=60.0,
                                                     max_event_exposure_pct=5.0))
    assert mm.fresh_caps_reason(cash=100, order_exposure=5.0000000001, total_exposure=5, event_exposure=5) != ""


@pytest.mark.parametrize("amount,limit,atteso", [("10", "10", True), ("10.01", "10", True), ("9.99", "10", False)])
def test_soglia_di_arresto_scatta_al_limite(amount, limit, atteso) -> None:
    assert O.stop_reached(amount, limit) is atteso
    assert validators.reaches_limit(float(amount), float(limit)) is atteso


def test_soglia_di_arresto_mai_permissiva() -> None:
    """La tolleranza di `reaches_limit` anticipa l'arresto (9.9999999999 su 10 => arresto): piu' severa
    dell'esatto, mai permissiva. Registrata, non e' un difetto di sicurezza."""
    for amount in ("10", "10.0000000001", "1000"):
        assert validators.reaches_limit(float(amount), 10.0) is True
    assert validators.reaches_limit(9.9999999999, 10.0) is True and not O.stop_reached("9.9999999999", "10")


# ------------------------------------------------------------------ BACK / LAY / liability
@pytest.mark.parametrize("side,stake,price,rischio", [
    ("BACK", "2.77", "1.36", "2.77"), ("LAY", "2.27", "1.44", "0.9988"), ("LAY", "0.10", "1.01", "0.001"),
    ("LAY", "0.10", "1000", "99.90"), ("BACK", "0.10", "1000", "0.10"), ("LAY", "1.00", "2.0", "1.00"),
])
def test_rischio_back_lay_al_centesimo(side, stake, price, rischio) -> None:
    assert O.risk(side, stake, price) == Decimal(rischio)
    code = validators.order_exposure_or_inf(side, float(stake), float(price))
    assert O.cents(code) == O.cents(rischio)


@pytest.mark.xfail(strict=True, reason="RO-DEC-02 (P3): rischio LAY in float binario, non Decimal esatto")
def test_ro_dec_02_rischio_lay_esatto() -> None:
    assert Decimal(str(validators.order_exposure_or_inf("LAY", 2.27, 1.44))) == Decimal("0.9988")


# ------------------------------------------------------------------ commissione Betfair Italia
@pytest.mark.parametrize("net,comm", [("10", "0.45"), ("0.67", "0.03015"), ("-6", "0"), ("0", "0"), ("2.865", "0.128925")])
def test_commissione_solo_sul_netto_positivo(net, comm) -> None:
    assert O.commission(net) == Decimal(comm)
    engine = PnLEngine(commission_pct=4.5)
    assert O.cents(engine._commission_amount(float(net))) == O.cents(comm)


# ------------------------------------------------------------------ stake, microstake, capitalizzazione
@pytest.mark.parametrize("bankroll,pct,price,atteso", [
    ("100", "1", "1.36", "2.77"), ("100", "1", "1.44", "2.27"), ("99.43", "1", "1.5", "1.98"),
    ("10", "1", "2.0", "0.10"), ("10", "1", "2.01", "0.09"), ("3.3333333333333335", "3", "2", "0.10"),
])
def test_stake_proporzionale_per_difetto_al_centesimo(bankroll, pct, price, atteso) -> None:
    assert O.stake(bankroll, pct, price) == Decimal(atteso)
    mm = RoserpinaMoneyManagement(legacy_test_config(target_profit_cycle_pct=float(pct)))
    exact = mm._base_stake_decimal(price=float(price), bankroll_current=float(bankroll), table_loss=0.0,
                                   risk_mult=1.0, desk_mult=1.0)
    assert isinstance(exact, Fraction)
    assert stake_precision.normalize_stake(exact) == Decimal(atteso)


def test_capitalizzazione_proporzionale_lo_stake_segue_il_bankroll() -> None:
    a, b = O.stake("100", "1", "1.5"), O.stake("200", "1", "1.5")
    assert (a, b) == (Decimal("2.00"), Decimal("4.00"))


def test_microstake_minimo_0_10_mai_arrotondato_verso_l_alto() -> None:
    """Il minimo configurato (0,10) e' un limite: 0,09 calcolato => rifiuto esplicito, mai 0,10 (decisione owner)."""
    assert O.stake("10", "1", "2.01") == Decimal("0.09") < Decimal("0.10")
    assert stake_precision.normalize_stake(Fraction(9, 100) + Fraction(9, 10000)) == Decimal("0.09")
