"""G7 — parita' finanziaria SIM/LIVE sui fondi riservati (CORRETTO in PFC-004, G7-a).

Decisione owner su #531 (20/10/2026): G7 era un difetto finanziario reale da correggere obbligatoriamente
(fase M6, roadmap #461/#506). Questi erano i test RED in `xfail(strict=True)` sul codice reale; con
PFC-004 il SimulationBroker riserva stake BACK e liability LAY anche sul non abbinato e rifiuta oltre il
disponibile (INSUFFICIENT_FUNDS), quindi passano e il marcatore e' stato tolto come previsto. Dettaglio dei
casi (parziali, copertura, cancellazione, settlement, riavvio, batch, sostituzione) in
`tests/unit/test_pfc004_sim_funds_reservation.py`.

Comportamento LIVE di riferimento: Betfair riserva i fondi al piazzamento (BACK = stake, LAY = liability)
anche per la parte non abbinata, e rifiuta un ordine oltre il disponibile (INSUFFICIENT_FUNDS).
"""
from __future__ import annotations

import pytest

from simulation_broker import SimulationBroker
from tests.acceptance.test_issue437_pr27 import _rifiuti
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _chain, _signal

def _broker(balance=100.0):
    return SimulationBroker(starting_balance=balance)


def test_g7_back_non_abbinato_riserva_lo_stake():
    b = _broker()
    assert b.place_bet(market_id="1.1", selection_id=1, side="BACK", price=2.0, size=60.0,
                       customer_ref="g7-back")["status"] == "SUCCESS"
    funds = b.get_account_funds()
    assert funds["available"] == pytest.approx(40.0) and funds["exposure"] == pytest.approx(60.0), funds


def test_g7_lay_non_abbinato_riserva_la_liability():
    b = _broker()
    assert b.place_bet(market_id="1.1", selection_id=1, side="LAY", price=4.0, size=10.0,
                       customer_ref="g7-lay")["status"] == "SUCCESS"
    funds = b.get_account_funds()
    assert funds["available"] == pytest.approx(70.0) and funds["exposure"] == pytest.approx(30.0), funds


def test_g7_ordine_oltre_il_disponibile_e_rifiutato_dal_broker():
    b = _broker()
    b.place_bet(market_id="1.1", selection_id=1, side="BACK", price=2.0, size=60.0, customer_ref="g7-a")
    secondo = b.place_bet(market_id="1.2", selection_id=2, side="BACK", price=2.0, size=60.0,
                          customer_ref="g7-b")
    assert secondo["status"] != "SUCCESS", secondo


def test_g7_runtime_il_gate_in_sim_vede_gli_ordini_aperti(tmp_path):
    # Riproduzione runtime: in SIM il gate PR28-e legge il saldo del SimulationBroker. Un ordine da 60 gia'
    # piazzato (SUCCESS rilascia il tavolo) deve consumare il disponibile, come in LIVE: una seconda entrata
    # da 60 su 100 di saldo va rifiutata. Oggi il saldo simulato resta 100 e l'entrata passa.
    broker = _broker()
    broker.place_bet(market_id="1.5", selection_id=5, side="BACK", price=2.0, size=60.0, customer_ref="g7-open")
    c = _chain(tmp_path, stake=60.0, anti_duplication=True)
    c.rc.betfair_service.get_account_funds = broker.get_account_funds
    c.rc._on_signal_received(_signal())
    assert [r for r in _rifiuti(c) if r.startswith("fondi_insufficienti")], _rifiuti(c)
    assert c.bus.payloads("CMD_QUICK_BET") == []
