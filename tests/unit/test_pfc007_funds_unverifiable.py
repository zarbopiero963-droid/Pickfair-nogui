"""PFC-007 (roadmap #461, M013 PF-FIN-02 residuo; #544): fondi non verificabili non valgono come saldo.

Lungo la catena exchange/broker -> BetfairService -> RuntimeController/TradingEngine un saldo assente,
non numerico, non finito o accompagnato da un errore (es. registro dei fill SIM incoerente) e' un dato
NON verificabile: niente sincronizzazione del bankroll, niente baseline del ciclo, niente cap Microstake,
e la causa resta visibile ("fondi non verificabili", non "saldo zero"). Lo stream Betfair passa
`numberOfWinners`, che il SimulationBroker usa per l'esposizione del mercato.
"""
from __future__ import annotations

import json
import math
from types import SimpleNamespace

import pytest

from betfair_client import BetfairClient
from core.runtime_controller import RuntimeController
from core.trading_engine import TradingEngine
from services.betfair_service import BetfairService
from services.streaming_feed import StreamingFeed
from simulation_broker import SimulationBroker
from tests.integration.test_runtime_controller_cycle_executor import _make_controller


class _Settings:
    def load_betfair_config(self):
        return SimpleNamespace(username="u", app_key="k", certificate="c", private_key="p")

    def load_password(self):
        return "pw"


def _servizio(broker):
    svc = BetfairService(_Settings())
    svc.get_client = lambda *a, **k: broker
    return svc


def _client_con(risposta):
    finto = SimpleNamespace(ACCOUNT_URL="https://x", _post_jsonrpc=lambda *a, **k: risposta,
                            _safe_float=BetfairClient._safe_float)
    return BetfairClient.get_account_funds(finto)


def _sim_con_registro_incoerente():
    b = SimulationBroker(starting_balance=1000.0)
    b.update_market_book("1.1", {"status": "OPEN", "runners": [{"selectionId": 1, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": 3.0, "size": 100.0}]}}]})
    b.place_bet(market_id="1.1", selection_id=1, side="BACK", price=3.0, size=10.0)
    stato = json.loads(json.dumps(b.state.to_dict()))
    stato["fills"] = []  # registro perso
    nuovo = SimulationBroker(starting_balance=1000.0)
    nuovo.state.load_from_dict(stato)
    return nuovo


# --- client LIVE ----------------------------------------------------------------------------------

@pytest.mark.parametrize("valore", [None, "abc", "NaN", float("inf")])
def test_client_live_saldo_assente_o_non_numerico_non_vale_zero(valore):
    risposta = {"exposure": -5.0}
    if valore is not None:
        risposta["availableToBetBalance"] = valore
    fondi = _client_con(risposta)
    assert fondi["available"] is None and fondi["exposure"] == -5.0


def test_client_live_saldo_valido_invariato():
    assert _client_con({"availableToBetBalance": 120.5, "exposure": -3.0})["available"] == 120.5


# --- servizio --------------------------------------------------------------------------------------

def test_servizio_conserva_l_errore_del_broker_sim():
    fondi = _servizio(_sim_con_registro_incoerente()).get_account_funds()
    assert fondi["error"] == "FILL_LEDGER_INCONSISTENT"


def test_servizio_saldo_live_mancante_e_un_errore():
    finto = SimpleNamespace(get_account_funds=lambda: _client_con({"exposure": 0.0}))
    assert _servizio(finto).get_account_funds()["error"] == "BALANCE_FIELD_MISSING:available"


@pytest.mark.parametrize("campo", ["available", "exposure"])
def test_servizio_valore_non_finito_e_un_errore(campo):
    grezzo = {"available": 10.0, "exposure": 0.0, "total": 10.0}
    grezzo[campo] = float("nan")
    fondi = _servizio(SimpleNamespace(get_account_funds=lambda: dict(grezzo))).get_account_funds()
    assert fondi["error"] == "BALANCE_NOT_FINITE:" + campo
    assert all(math.isfinite(fondi[k]) for k in ("available", "exposure", "total"))


def test_servizio_valido_senza_errore_e_con_riserva():
    b = SimulationBroker(starting_balance=100.0)
    b.place_bet(market_id="1.1", selection_id=1, side="BACK", price=2.0, size=30.0)
    fondi = _servizio(b).get_account_funds()
    assert "error" not in fondi and fondi["available"] == 70.0 and fondi["reserved_unmatched"] == 30.0


# --- runtime ---------------------------------------------------------------------------------------

def test_conto_con_errore_non_e_valido_anche_se_i_campi_ci_sono():
    conto = RuntimeController._account_from_funds({"available": 50.0, "exposure": 0.0, "error": "X"})
    assert conto["ok"] is False and conto["reason"] == "BALANCE_ERROR:X"


def test_avvio_con_fondi_non_verificabili_lascia_bankroll_e_baseline_invariati():
    rc, _bus = _make_controller(responses=[])
    prima = float(rc.risk_desk.bankroll_current)
    salvati = []
    rc.db.save_settings = lambda d: salvati.append(dict(d))
    conto = rc._applica_fondi_avvio({"available": 0.0, "exposure": 0.0, "error": "FILL_LEDGER_INCONSISTENT"})
    assert conto["ok"] is False
    assert float(rc.risk_desk.bankroll_current) == prima
    assert not any(RuntimeController._EXPANSION_KEY in d for d in salvati)  # nessuna baseline su dati non validi


def test_avvio_con_fondi_validi_sincronizza_e_apre_il_ciclo():
    rc, _bus = _make_controller(responses=[])
    rc._applica_fondi_avvio({"available": 250.0, "exposure": 0.0})
    assert float(rc.risk_desk.bankroll_current) == 250.0
    assert rc._expansion_baseline == 250.0


@pytest.mark.parametrize("fondi", [
    {"available": float("nan"), "exposure": 0.0},
    {"available": 0.0, "exposure": 0.0, "error": "FILL_LEDGER_INCONSISTENT"},
    {"available": 40.0, "exposure": 0.0, "error": "BALANCE_FIELD_MISSING:exposure"},
])
def test_get_status_non_sincronizza_il_bankroll_su_fondi_non_verificabili(fondi):
    rc, _bus = _make_controller(responses=[fondi])
    prima = float(rc.risk_desk.bankroll_current)
    rc.get_status()
    assert float(rc.risk_desk.bankroll_current) == prima and math.isfinite(prima)


# --- cap Microstake --------------------------------------------------------------------------------

def test_cap_microstake_non_disponibile_con_fondi_in_errore():
    rc = SimpleNamespace(betfair_service=SimpleNamespace(
        get_account_funds=lambda: {"available": 100.0, "exposure": 0.0, "error": "FILL_LEDGER_INCONSISTENT"}))
    motore = SimpleNamespace(runtime_controller=rc, microstake_executor=None)
    assert TradingEngine._native_transient_cap(motore, {}) is None


# --- stream ----------------------------------------------------------------------------------------

def _feed():
    return StreamingFeed(client_getter=lambda: None, config={"enabled": False}, on_market_book=lambda _b: None)


@pytest.mark.parametrize("vincitori,atteso", [(1, 1), (3, 3), (0, None), (True, None), (None, None)])
def test_stream_passa_il_numero_di_vincitori(vincitori, atteso):
    definizione = SimpleNamespace(status="OPEN", in_play=False, runners=[], number_of_winners=vincitori)
    out = _feed()._resource_market_definition_to_dict(definizione)
    assert out.get("numberOfWinners") == atteso


def test_dutching_lay_dal_book_dello_stream_usa_il_vincitore_unico():
    # Con numberOfWinners preso dalla marketDefinition dello stream il SIM riserva la liability peggiore.
    b = SimulationBroker(starting_balance=30.0)
    definizione = _feed()._resource_market_definition_to_dict(
        SimpleNamespace(status="OPEN", in_play=False, runners=[], number_of_winners=1))
    b.update_market_book("1.1", {"status": "OPEN", "marketDefinition": definizione, "runners": [
        {"selectionId": s, "status": "ACTIVE", "ex": {"availableToLay": [{"price": 5.2, "size": 100.0}]}}
        for s in (1, 2)]})
    assert b.place_bet(market_id="1.1", selection_id=1, side="LAY", price=3.0, size=10.0)["status"] == "SUCCESS"
    assert b.place_bet(market_id="1.1", selection_id=2, side="LAY", price=4.0, size=10.0)["status"] == "SUCCESS"
