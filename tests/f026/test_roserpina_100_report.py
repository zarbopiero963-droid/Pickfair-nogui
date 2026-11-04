"""Simulazione Roserpina con 100 EUR FITTIZI: runtime reale (HeadlessApp in sottoprocesso, SQLite, broker SIM come
confine dell'exchange), report finale verificato contro un oracolo indipendente.

PROVENIENZA: SINTETICO. Scenario e quote sono scritti a mano qui sotto e dichiarati tali; non sono dati registrati
ne' dati del PDF MoneyFlow (non disponibile al 04/11/2026). Lo stake lo decide il Money Management reale.

Oracolo (indipendente dal report e dal codice di settlement): per ogni mercato regolato, dagli ordini ABBINATI
sull'exchange nell'ultimo snapshot prima della chiusura e dall'esito dichiarato:
  netto_lordo = somma(BACK vinto: m*(p-1); BACK perso: -m; LAY vinto dal banco: m; LAY perso: -m*(p-1))
  commissione = 4,5% del netto_lordo del mercato se positivo, altrimenti 0 (commissione sulle vincite nette).
"""
from __future__ import annotations

import json
from decimal import Decimal as D

import pytest

from tests.f026.test_f026_financial import run
from tools.sim_report.roserpina_report import build_report

pytestmark = pytest.mark.integration

COMM = D("0.045")
CENT = D("0.01")
OUTCOMES = {"1.801": True, "1.802": False, "1.803": True, "1.804": True}   # True = vince la selezione puntata (11)
NOT_COVERED = ["SL/TP: F-02 non e' su main (F-04 pronto su branch)", "trailing stop: MISSING (K-7)"]

PHASE_1 = [
    {"op": "signal", "m": "1.801", "price": 2.0},                    # tavolo A
    {"op": "signal", "m": "1.802", "price": 1.5},                    # tavolo B
    {"op": "mode", "m": "1.803", "mode": "nomatch"},
    {"op": "signal", "m": "1.803", "price": 3.0},                    # tavolo C: non abbinato
    {"op": "price", "m": "1.801", "price": 1.6},                     # gol a favore di A: quota scesa
    {"op": "cashout", "m": "1.801"},                                 # cashout PARZIALE: solo A
    {"op": "suspend", "m": "1.802"},
    {"op": "reopen", "m": "1.802"},
    {"op": "settle", "m": "1.801", "win": True},
    {"op": "crash_now"},
]
PHASE_2 = [
    {"op": "settle", "m": "1.802", "win": False},
    {"op": "settle", "m": "1.803", "win": True},                     # C non abbinato: decade, nessun P&L
    {"op": "signal", "m": "1.804", "price": 2.2},                    # nuovo ingresso dopo la perdita (MM reale)
    {"op": "settle", "m": "1.804", "win": True},
]


def _q(x) -> D:
    return D(str(x)).quantize(CENT)


def _oracle_net(snaps, market: str, win: bool) -> D:
    before = None
    for s in snaps:
        if s["tag"].endswith(f":settle:{market}"):
            break
        before = s
    gross = D(0)
    for o in (before or {}).get("exch_orders") or []:
        if str(o["market_id"]) != market or not o.get("matched"):
            continue
        m = D(str(o["matched"]))
        # prezzo medio non esposto nello snapshot: la quota del prezzo richiesto e' quella abbinata nel broker SIM
        # (il book offre esattamente quel prezzo; nessun miglioramento modellato).
        p = D(str(o["price"]))
        mine = str(o["selection_id"]) == "11"
        sel_wins = win if mine else not win
        if o["side"] == "BACK":
            gross += m * (p - 1) if sel_wins else -m
        else:
            gross += -m * (p - 1) if sel_wins else m
    return gross


@pytest.fixture(scope="module")
def simulation():
    r1 = run(PHASE_1)
    r2 = run(PHASE_2, home=r1["_home"])
    report = build_report([r1, r2], provenance="SINTETICO", scenario="roserpina-100-multitavolo-v1",
                          not_covered=NOT_COVERED)
    return r1, r2, report


def test_report_della_simulazione_100_coerente_con_l_oracolo(simulation) -> None:
    r1, r2, rep = simulation
    snaps = r1["snaps"] + r2["snaps"]
    print(json.dumps(rep, indent=1))

    assert rep["provenance"] == "SINTETICO" and rep["restarts"] == 1
    assert rep["initial_capital"] == "100.00"
    expected_total = D(0)
    by_market = {t["market_id"]: t for t in rep["tables"]}
    for market, win in OUTCOMES.items():
        gross = _oracle_net(snaps, market, win)
        commission = (gross * COMM if gross > 0 else D(0))
        net = gross - commission
        expected_total += net
        t = by_market.get(market)
        assert t is not None, (market, rep["tables"])
        if gross == 0 and not t["settled"]:
            continue                                                  # niente abbinato: nessun settlement
        assert _q(t["net_pnl"]) == _q(net), (market, t, gross)
        assert _q(t["commission"]) == _q(commission), (market, t, gross)
    assert _q(rep["realized_net_pnl"]) == _q(expected_total)
    assert _q(rep["final_balance_total"]) == _q(D(100) + expected_total), rep
    assert rep["unreconciled_orders"] == [], rep["unreconciled_orders"]


def test_cashout_parziale_e_unmatched_nel_report(simulation) -> None:
    _r1, _r2, rep = simulation
    by_market = {t["market_id"]: t for t in rep["tables"]}
    assert by_market["1.801"]["exits"] >= 1, by_market["1.801"]       # hedge del cashout di A
    assert by_market["1.802"]["exits"] == 0                            # B non toccato dal cashout di A
    assert by_market["1.803"]["net_pnl"] in (None, "0.00")              # C mai abbinato
    assert D(rep["exposure_max"]) > 0 and D(rep["cash_drawdown_max_pct"]) >= 0
    assert rep["not_covered"] == NOT_COVERED


@pytest.mark.parametrize("bad", ["STIMATO", "", None])
def test_block_provenienza_non_dichiarata_rifiutata(bad) -> None:
    with pytest.raises(ValueError):
        build_report([{"snaps": [{"funds": {"total": 100, "available": 100}}]}], provenance=bad, scenario="x")


@pytest.mark.parametrize("value", [None, True, "nan", "inf"])
def test_block_valori_non_numerici_rifiutati(value) -> None:
    snap = {"funds": {"total": value, "available": 100}}
    with pytest.raises(ValueError):
        build_report([{"snaps": [snap]}], provenance="SINTETICO", scenario="x")


def test_block_dump_senza_snapshot_rifiutato() -> None:
    with pytest.raises(ValueError):
        build_report([{"snaps": []}], provenance="SINTETICO", scenario="x")


def test_commissione_e_drawdown_su_dati_minimi() -> None:
    snaps = [
        {"funds": {"total": 100, "available": 100, "exposure": 0}, "drawdown_pct": 0, "orders": [], "exch_orders": []},
        {"funds": {"total": 90, "available": 80, "exposure": 10}, "drawdown_pct": 10, "orders": [], "exch_orders": []},
        {"funds": {"total": 109.55, "available": 109.55, "exposure": 0}, "drawdown_pct": 0,
         "orders": [{"market_id": "1.1", "status": "SUBMITTED", "hedge": False}],
         "exch_orders": [], "exch_settlements": [{"market_id": "1.1", "gross_pnl": 10.0, "net_pnl": 9.55}]},
    ]
    rep = build_report([{"snaps": snaps}], provenance="SINTETICO", scenario="minimo")
    assert rep["commission_total"] == "0.45" and rep["realized_net_pnl"] == "9.55"
    assert rep["cash_drawdown_max_pct"] == "10.00" and rep["exposure_max"] == "10.00"
    assert rep["final_balance_total"] == "109.55" and rep["tables"][0]["entries"] == 1
    assert rep["unreconciled_orders"] == []


@pytest.mark.parametrize("status,atteso", [("SETTLED", 0), ("LAPSED", 0), ("EXECUTABLE", 1), ("EXECUTION_COMPLETE", 1)])
def test_ordini_vivi_su_mercato_regolato_sono_non_riconciliati(status, atteso) -> None:
    snap = {"funds": {"total": 100, "available": 100, "exposure": 0}, "orders": [],
            "exch_orders": [{"market_id": "1.1", "status": status}],
            "exch_settlements": [{"market_id": "1.1", "gross_pnl": 0.0, "net_pnl": 0.0}]}
    rep = build_report([{"snaps": [snap]}], provenance="SINTETICO", scenario="x")
    assert len(rep["unreconciled_orders"]) == atteso


@pytest.mark.parametrize("status,atteso", [("AMBIGUOUS", 1), ("INFLIGHT", 1), ("SUBMITTED", 0), ("FAILED", 0)])
def test_ordini_incerti_nel_db_sono_non_riconciliati(status, atteso) -> None:
    snap = {"funds": {"total": 100, "available": 100, "exposure": 0}, "exch_orders": [],
            "orders": [{"market_id": "1.1", "status": status, "hedge": False}]}
    rep = build_report([{"snaps": [snap]}], provenance="SINTETICO", scenario="x")
    assert len(rep["unreconciled_orders"]) == atteso
