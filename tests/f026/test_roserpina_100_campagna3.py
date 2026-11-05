"""Simulazione Roserpina 100 EUR — campagna 3: guasti e campagne con seed (F-02 + F-04 + F-03), dati SINTETICI.

Runtime reale (HeadlessApp in sottoprocesso, SQLite, MM reale, poller SL/TP di F-02, chiusura F-04, trailing F-03),
broker SIM come confine dell'exchange. Senza `core.trailing_stop` (branch `claude/d-f03-trailing-stop`) il modulo e'
SALTATO con il motivo, mai simulato.

Parte A, casi fissi:
- A1 ingresso abbinato in parte (1 EUR di contropartita) + SL: un solo hedge, dimensionato sull'abbinato confermato;
- A2 hedge con esito ignoto (risposta persa dopo l'invio): nessun secondo hedge, ne' allo scatto ripetuto ne' dopo il
  riavvio; la chiusura resta ROUTED;
- A3 settlement di tutto: realizzato e bankroll coerenti con l'oracolo e con l'exchange.
Parte B, campagne con seed: 3 tavoli, protezioni e percorsi di quota estratti da un `random.Random(seed)` (seed fissi,
riproducibili), crash in un punto estratto dal seed (checkpoint: lo stato SIM e il DB sono durevoli a ogni passo),
riavvio, settlement. Invarianti: al piu' una chiusura instradata e un hedge per mercato, report = oracolo, nessun
ordine non riconciliato.
"""
from __future__ import annotations

import importlib.util
import random
from decimal import Decimal as D

import pytest

from tests.f026.test_f026_financial import run
from tests.f026.test_roserpina_100_report import COMM, _oracle_net, _q
from tools.sim_report.roserpina_report import build_report

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    importlib.util.find_spec("core.trailing_stop") is None,
    reason="richiede F-02 + F-04 + F-03 (claude/d-f03-trailing-stop): SL/TP/trailing non sono su main")]

POLL = {"op": "wait", "s": 3}


def _lays(snaps, market):
    last = snaps[-1]
    return [o for o in last["exch_orders"] if str(o["market_id"]) == market and o["side"] == "LAY"]


def _closes(rep, market, event=None):
    return [c for c in rep["protections"]["closes"]
            if c["market_id"] == market and (event is None or c["event"] == event)]


def _oracle_ok(rep, snaps, outcomes):
    total = D(0)
    by_market = {t["market_id"]: t for t in rep["tables"]}
    for market, win in outcomes.items():
        gross = _oracle_net(snaps, market, win)
        net = gross - (gross * COMM if gross > 0 else D(0))
        total += net
        t = by_market.get(market)
        if t is None or not t["settled"]:
            assert gross == 0, (market, t, gross)
            continue
        assert _q(t["net_pnl"]) == _q(net), (market, t, gross)
    assert _q(rep["final_balance_total"]) == _q(D(100) + total), rep
    return total


# ------------------------------------------------------------------ parte A
@pytest.fixture(scope="module")
def guasti():
    r1 = run([
        {"op": "mode", "m": "1.951", "mode": "partial"},
        {"op": "signal", "m": "1.951", "price": 2.0, "stop_loss_pct": 10},      # A1: abbinato 1 EUR
        {"op": "signal", "m": "1.952", "price": 2.0, "stop_loss_pct": 10},      # A2
        {"op": "price", "m": "1.951", "price": 3.0}, POLL,
        {"op": "mode", "m": "1.952", "mode": "lost"},                           # l'hedge di 1.952 parte, risposta persa
        {"op": "price", "m": "1.952", "price": 3.0}, POLL, POLL,
        {"op": "crash_now"},
    ])
    r2 = run([POLL, {"op": "settle", "m": "1.951", "win": False}, {"op": "settle", "m": "1.952", "win": False}],
             home=r1["_home"])
    rep = build_report([r1, r2], provenance="SINTETICO", scenario="roserpina-100-guasti-v1")
    return r1, r2, rep


def test_a1_ingresso_parziale_un_solo_hedge_sull_abbinato_confermato(guasti) -> None:
    r1, _r2, rep = guasti
    lays = _lays(r1["snaps"], "1.951")
    assert len(lays) == 1, lays
    backs = [o for o in r1["snaps"][-1]["exch_orders"] if str(o["market_id"]) == "1.951" and o["side"] == "BACK"]
    matched_back = sum(D(str(o["matched"] or 0)) for o in backs)
    assert matched_back == D("1"), backs
    # green-up di 1 EUR @2.0 chiuso @3.02: hedge ~0.66, mai sullo stake intero richiesto
    assert D(str(lays[0]["size"])) <= matched_back, lays
    # il residuo non abbinato del BACK e' cancellato PRIMA dell'hedge: non puo' abbinarsi dopo e riaprire la posizione
    assert all(not (o.get("status") == "EXECUTABLE" and D(str(o["size"])) > D(str(o["matched"] or 0)))
               for o in backs), backs
    assert len(_closes(rep, "1.951", "SL_TP_CLOSE_ROUTED")) == 1


def test_a2_hedge_con_esito_ignoto_mai_ripetuto(guasti) -> None:
    r1, r2, rep = guasti
    assert len(_lays(r1["snaps"], "1.952")) == 1, "hedge non partito o duplicato"
    assert len(_lays(r2["snaps"], "1.952")) == 1, "secondo hedge dopo il riavvio"
    assert len(_closes(rep, "1.952", "SL_TP_CLOSE_ROUTED")) == 1
    assert all(c["event"] == "SL_TP_CLOSE_SKIPPED" for c in _closes(rep, "1.952") if c["run"] == 1)


def test_a3_settlement_e_bankroll_coerenti(guasti) -> None:
    r1, r2, rep = guasti
    _oracle_ok(rep, r1["snaps"] + r2["snaps"], {"1.951": False, "1.952": False})
    last = r2["snaps"][-1]
    assert _q(last["bankroll"]) <= _q(last["funds"]["available"]), "bankroll sopra i fondi reali"


# ------------------------------------------------------------------ parte B: campagne con seed
SEEDS = (20261105, 7, 42)
PROTEZIONI = ({"stop_loss_pct": 10}, {"take_profit_pct": 10}, {"trailing_stop_pct": 20}, {})
QUOTE = (1.3, 1.4, 1.5, 1.6, 1.8, 2.2, 2.6, 3.0)


def _campagna(seed: int):
    rng = random.Random(seed)
    markets = [f"1.{960 + 10 * SEEDS.index(seed) + i}" for i in range(3)]
    steps, outcomes = [], {}
    for m in markets:
        steps.append({"op": "signal", "m": m, "price": 2.0, **rng.choice(PROTEZIONI)})
        outcomes[m] = rng.random() < 0.5
    ticks = [{"op": "price", "m": rng.choice(markets), "price": rng.choice(QUOTE)} for _ in range(6)]
    crash_at = rng.randrange(1, len(ticks))                     # checkpoint scelto dal seed
    phase1 = steps + [x for t in ticks[:crash_at] for x in (t, POLL)] + [{"op": "crash_now"}]
    phase2 = [x for t in ticks[crash_at:] for x in (t, POLL)] + [
        {"op": "settle", "m": m, "win": outcomes[m]} for m in markets]
    return markets, outcomes, phase1, phase2


@pytest.mark.parametrize("seed", SEEDS)
def test_campagna_con_seed_invarianti_e_oracolo(seed: int) -> None:
    markets, outcomes, phase1, phase2 = _campagna(seed)
    assert _campagna(seed)[2] == phase1, "campagna non riproducibile dal seed"
    r1 = run(phase1)
    r2 = run(phase2, home=r1["_home"])
    rep = build_report([r1, r2], provenance="SINTETICO", scenario=f"roserpina-100-seed-{seed}")
    snaps = r1["snaps"] + r2["snaps"]
    for m in markets:
        assert len(_closes(rep, m, "SL_TP_CLOSE_ROUTED")) <= 1, (seed, m, rep["protections"])
        before_settle = [s for s in snaps if s["tag"].endswith(f":settle:{m}")]
        idx = snaps.index(before_settle[0]) if before_settle else len(snaps)
        lays = [o for o in snaps[idx - 1]["exch_orders"] if str(o["market_id"]) == m and o["side"] == "LAY"]
        assert len(lays) <= 1, (seed, m, lays)
    _oracle_ok(rep, snaps, outcomes)
    assert rep["unreconciled_orders"] == [], (seed, rep["unreconciled_orders"])
    assert rep["restarts"] == 1
