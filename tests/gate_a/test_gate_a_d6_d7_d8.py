"""Decisione owner PF-OWNER-OD12-P38-CASHOUT-FINAL-20261111 (D6/D7/D8), Claude D. Test del comportamento RATIFICATO,
xfail(strict) finche' la correzione non arriva nella PR di destinazione (D6 -> PR-09/PR-05 con l'Emergency Stop #629;
D7 -> PR-07 dopo PR-06; D8 -> PR-14). D6 e' gia' coperto da
tests/gate_a/test_gate_a_t2b_b4_runtime.py::test_trg_628_b_0740_p38_stop_risk_reduction (xfail strict)."""
import pytest

from cashout_router import CashoutRouter


# ------------------------------------------------------------------ D7: identita' del CASHOUT singolo per ID Betfair
_POS = [{"market_id": "1.100", "selection_id": 1}, {"market_id": "1.200", "selection_id": 2}]
_MARKET_EVENT = {"1.100": "inter - milan", "1.200": "inter - milan"}   # due eventi omonimi su mercati diversi


@pytest.mark.xfail(strict=True, reason="TRG-628-B-0770 / D7: ambito per nome evento, non per marketId")
def test_trg_628_b_0770_d7_single_cashout_only_the_requested_market():
    """ID: TRG-628-B-0770 / PF-B1-CASH-SCOPE-EVENT — due eventi con lo stesso nome non si confondono: un CASHOUT
    singolo con marketId 1.100 tocca solo 1.100."""
    out = CashoutRouter._restrict_to_event({"event_name": "Inter - Milan", "market_id": "1.100"}, _POS, _MARKET_EVENT)
    assert [p["market_id"] for p in (out or [])] == ["1.100"]


@pytest.mark.xfail(strict=True, reason="TRG-628-B-0770 / D7: senza marketId il nome evento basta ancora")
def test_trg_628_b_0770_d7_name_alone_is_never_identity():
    """ID: TRG-628-B-0770 — il solo nome della partita non identifica: nessun cashout automatico (fail-closed)."""
    out = CashoutRouter._restrict_to_event({"event_name": "Inter - Milan"}, _POS, _MARKET_EVENT)
    assert not out
    assert not CashoutRouter._in_scope_markets("CASHOUT", {"event_name": "Inter - Milan"}, _MARKET_EVENT)


def test_trg_628_b_0770_d7_missing_identity_closes_nothing():
    """ID: TRG-628-B-0770 — gia' vero su main: senza alcuna identita' non si chiude nulla."""
    assert CashoutRouter._restrict_to_event({}, _POS, _MARKET_EVENT) is None
    assert CashoutRouter._in_scope_markets("CASHOUT", {}, _MARKET_EVENT) == set()


# ------------------------------------------------------------------ D8: contabilita' prudenziale del ledger SIM
def _hedged_broker():
    from simulation_broker import SimulationBroker
    b = SimulationBroker(starting_balance=1000.0)
    m, s = "1.900", 5
    b.update_market_book({"marketId": m, "status": "OPEN", "runners": [{"selectionId": s, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": 3.0, "size": 100.0}], "availableToLay": [{"price": 2.0, "size": 100.0}]}}]})
    assert float(b.place_bet(market_id=m, selection_id=s, side="BACK", price=3.0, size=10.0)
                 ["instructionReports"][0]["sizeMatched"]) == 10.0
    assert float(b.place_bet(market_id=m, selection_id=s, side="LAY", price=2.0, size=10.0)
                 ["instructionReports"][0]["sizeMatched"]) == 10.0
    return b


def test_trg_628_b_1002_d8_available_funds_use_the_guaranteed_minimum():
    """ID: TRG-628-B-1002 / PF-B1-R2-LEDGER-CLOSE — gia' vero su main: i fondi disponibili riflettono il minimo
    garantito (0 EUR): BACK 10@3 + LAY 10@2 => esito +10 / 0, disponibile 1000."""
    f = _hedged_broker().get_account_funds()
    assert f["available"] == pytest.approx(1000.0) and f["total"] == pytest.approx(1000.0)


@pytest.mark.xfail(strict=True, reason="TRG-628-B-1002 / D8: il ledger accredita +10 non regolati al saldo")
def test_trg_628_b_1002_d8_unsettled_hedge_profit_is_never_credited():
    """ID: TRG-628-B-1002 — D8: il PnL potenziale (+10 se vince, 0 se perde) non diventa PnL regolato prima del
    settlement: il saldo non include i +10."""
    assert _hedged_broker().state.balance == pytest.approx(1000.0)
