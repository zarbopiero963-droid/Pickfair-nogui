"""Simulazione Roserpina 100 EUR — campagna 2: SL, TP e trailing end-to-end (F-02 + F-04 + F-03), dati SINTETICI.

Runtime reale (HeadlessApp in sottoprocesso, SQLite, Money Management reale, poller SL/TP di F-02, chiusura F-04,
trailing F-03), broker SIM come confine dell'exchange. Le protezioni non sono su `main`: senza `core.trailing_stop`
(branch `claude/d-f03-trailing-stop`, impilato su F-02 + F-04) il test e' SALTATO con il motivo, mai simulato.

PROVENIENZA: SINTETICO (quote scritte a mano qui sotto). PDF MoneyFlow non disponibile: nessun dato inventato.
Oracolo indipendente identico alla campagna 1 (ordini abbinati sull'exchange prima della chiusura, hedge compresi,
commissione 4,5 % sul netto positivo del mercato).
"""
from __future__ import annotations

import importlib.util
import json
from decimal import Decimal as D

import pytest

from tests.f026.test_f026_financial import run
from tests.f026.test_roserpina_100_report import COMM, _oracle_net, _q
from tools.sim_report.roserpina_report import build_report

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    importlib.util.find_spec("core.trailing_stop") is None,
    reason="richiede F-02 + F-04 + F-03 (claude/d-f03-trailing-stop): SL/TP/trailing non sono su main")]

OUTCOMES = {"1.901": False, "1.902": True, "1.903": True, "1.904": True}
POLL = {"op": "wait", "s": 3}                     # il poller di F-02 gira ogni 2 s (sl_tp_poll_sec)

PHASE_1 = [
    {"op": "signal", "m": "1.901", "price": 2.0, "stop_loss_pct": 10},
    {"op": "signal", "m": "1.902", "price": 2.0, "take_profit_pct": 10},
    {"op": "signal", "m": "1.903", "price": 2.0, "trailing_stop_pct": 20},
    {"op": "signal", "m": "1.904", "price": 2.0},                                  # nessuna protezione
    {"op": "price", "m": "1.901", "price": 3.0}, POLL,                             # perdita: SL
    {"op": "price", "m": "1.902", "price": 1.6}, POLL,                             # profitto: TP
    {"op": "price", "m": "1.903", "price": 1.4}, POLL,                             # picco del trailing
    {"op": "price", "m": "1.903", "price": 1.3}, POLL,                             # picco piu' alto
    {"op": "price", "m": "1.903", "price": 1.6}, POLL,                             # ritracciata: trailing
    {"op": "price", "m": "1.904", "price": 3.0}, POLL,                             # D in perdita: nessuna azione
    {"op": "crash_now"},
]
PHASE_2 = [
    POLL,                                                                          # gli scatti si ripetono: SKIPPED
    {"op": "settle", "m": "1.901", "win": False},
    {"op": "settle", "m": "1.902", "win": True},
    {"op": "settle", "m": "1.903", "win": True},
    {"op": "settle", "m": "1.904", "win": True},
]


@pytest.fixture(scope="module")
def campagna():
    r1 = run(PHASE_1)
    r2 = run(PHASE_2, home=r1["_home"])
    report = build_report([r1, r2], provenance="SINTETICO", scenario="roserpina-100-protezioni-v1")
    return r1, r2, report


def test_protezioni_scattano_e_chiudono_solo_il_proprio_tavolo(campagna) -> None:
    _r1, _r2, rep = campagna
    print(json.dumps(rep, indent=1))
    trig = {(t["event"], t["market_id"]) for t in rep["protections"]["triggers"] if t["run"] == 0}
    assert trig == {("SL_TRIGGERED", "1.901"), ("TP_TRIGGERED", "1.902"), ("TRAILING_TRIGGERED", "1.903")}, trig
    routed = sorted(c["market_id"] for c in rep["protections"]["closes"] if c["event"] == "SL_TP_CLOSE_ROUTED")
    assert routed == ["1.901", "1.902", "1.903"], rep["protections"]["closes"]
    by_market = {t["market_id"]: t for t in rep["tables"]}
    assert by_market["1.904"]["exits"] == 0, "tavolo senza protezioni chiuso"
    for m in ("1.901", "1.902", "1.903"):
        assert by_market[m]["exits"] == 1, (m, by_market[m])


def test_riavvio_nessuna_seconda_chiusura(campagna) -> None:
    _r1, _r2, rep = campagna
    second = [c for c in rep["protections"]["closes"] if c["run"] == 1]
    assert all(c["event"] == "SL_TP_CLOSE_SKIPPED" for c in second), second


def test_report_coerente_con_l_oracolo(campagna) -> None:
    r1, r2, rep = campagna
    snaps = r1["snaps"] + r2["snaps"]
    total = D(0)
    by_market = {t["market_id"]: t for t in rep["tables"]}
    for market, win in OUTCOMES.items():
        gross = _oracle_net(snaps, market, win)
        commission = gross * COMM if gross > 0 else D(0)
        total += gross - commission
        assert _q(by_market[market]["net_pnl"]) == _q(gross - commission), (market, by_market[market], gross)
    assert _q(rep["final_balance_total"]) == _q(D(100) + total), rep
    assert rep["unreconciled_orders"] == []
