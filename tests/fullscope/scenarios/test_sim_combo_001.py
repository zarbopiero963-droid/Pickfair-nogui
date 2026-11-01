"""SIM-COMBO-001 — Roserpina + ingressi BACK/LAY + abbinamento parziale + cashout parziale + crash esterno + riavvio
+ settlement + riconciliazione finale (PICKFAIR_FULL_E2E_SCENARIO_SUITE, piano #586 §4).

Componenti REALI: HeadlessApp, RuntimeController, TableManager, money management, cashout chain, SimulationBroker,
SQLite su disco. Oracolo indipendente in Decimal (tests/fullscope/harness/oracle.py). Seed: PF_SEED.

Perimetro dichiarato di questa versione (v1): saldo 100, 3 tavoli, 2 segnali Roserpina (BACK), 1 manuale LAY con
stake fisso, un mercato con abbinamento parziale, cashout parziale di un mercato, crash ESTERNO (SIGKILL) con
posizioni vive, riavvio, settlement dei tre mercati, riconciliazione. La gamba STOP-LOSS del mandato non e'
eseguibile su questo main: il monitor SL/TP e' inerte (CSH-01/CSH-02, schede F-01/F-02) — test separato in xfail
strict con riferimento, cosi' il giorno in cui F-02 cabla il percorso il marker va rimosso.
"""
import tempfile
from decimal import Decimal as D

import pytest

from tests.fullscope.harness import oracle as O
from tests.fullscope.harness import runner as H

pytestmark = [pytest.mark.fullscope, pytest.mark.e2e]

START = D("100")
COMM = D("0.045")
SETTINGS = {"roserpina.table_count": 3}

M_BACK, M_PARTIAL, M_LAY = "1.801", "1.802", "1.803"


def _phase1(home):
    return H.run([
        {"op": "signal", "m": M_BACK, "price": 2.0, "side": "BACK"},                       # segnale 1 (Roserpina)
        {"op": "mode", "m": M_PARTIAL, "mode": "partial"},
        {"op": "signal", "m": M_PARTIAL, "price": 2.0, "side": "BACK"},                    # segnale 2: abbinamento parziale
        {"op": "signal", "m": M_LAY, "price": 2.0, "side": "LAY", "stake": 2.0},           # manuale LAY stake fisso
        {"op": "cashout", "m": M_BACK},                                                     # cashout PARZIALE (solo 1.801)
        {"op": "checkpoint", "name": "live_positions", "hold_s": 12},                      # SIGKILL esterno qui
        {"op": "wait", "s": 1},
    ], home=home, settings=SETTINGS, kill_at_checkpoint="live_positions", scenario="sim_combo_001",
       requirements=["SIM-COMBO-001"])


def _phase2(home):
    return H.run([
        {"op": "wait", "s": 1},
        {"op": "settle", "m": M_BACK, "win": True},
        {"op": "settle", "m": M_PARTIAL, "win": False},
        {"op": "settle", "m": M_LAY, "win": False},      # la selezione layata PERDE => il LAY vince
        {"op": "wait", "s": 1},
    ], home=home, settings=SETTINGS, scenario="sim_combo_001")


@pytest.fixture(scope="module")
def combo():
    home = tempfile.mkdtemp(prefix="pf_combo001_")
    first = _phase1(home)
    second = _phase2(home)
    return first, second


def test_phase1_positions_and_partial_cashout(combo):
    first, _ = combo
    assert first["_killed_at"] == "live_positions", "il crash esterno non e' avvenuto al checkpoint"
    s = first["snaps"][-1]
    o = O.Oracle(s, {}, start=START, commission=COMM)
    # tre mercati con posizione; il parziale ha matched < stake; il LAY e' presente col suo stake fisso
    assert O.table_markets(s) == {M_BACK, M_PARTIAL, M_LAY}, O.describe(s, o)
    part = [x for x in s["orders"] if x["market_id"] == M_PARTIAL and (x["side"] or "BACK").upper() == "BACK"][0]
    assert part["matched"] is not None and D(str(part["matched"])) < D(str(part["stake"])), "parziale non parziale: " + O.describe(s)
    lay = [x for x in s["orders"] if x["market_id"] == M_LAY][0]
    assert (lay["side"] or "").upper() == "LAY" and D(str(lay["stake"])) == D("2"), O.describe(s)
    # un solo cashout; l'hedge (LAY) e' sull'exchange, abbinato, sul mercato 1.801
    assert len(H.events(first, "CASHOUT_SUCCESS")) == 1, O.describe(s)
    exch_hedges = [leg for leg in O.exchange_legs(s) if leg["market_id"] == M_BACK and leg["side"] == "LAY"]
    assert len(exch_hedges) == 1, "hedge del cashout non abbinato sull'exchange: " + O.describe(s)
    o = O.Oracle(s, {}, start=START, commission=COMM, extra_legs=O.exchange_legs(s))
    O.check_no_duplicate_intents(s, [M_BACK, M_PARTIAL, M_LAY])
    O.check_conservation(s, o)


def test_cashout_hedge_is_written_to_ledger(combo):
    # F-05 (CSH-06): era xfail strict su main 6fa29b2; con il ledger write-ahead dell'hedge ogni gamba
    # dell'exchange ha la sua riga in `orders` (chiusura verificata dall'XPASS strict).
    first, _ = combo
    s = first["snaps"][-1]
    assert O.ledger_missing_exchange_legs(s) == [], "gambe exchange assenti dal ledger: " + O.describe(s)


def test_phase2_restart_keeps_state_then_settles_and_reconciles(combo):
    first, second = combo
    s_before = first["snaps"][-1]
    s0 = second["snaps"][0]
    # nessuna perdita di stato dopo il riavvio: stessi ordini nel ledger, stessi tavoli vivi, stesso exchange
    assert {(x["market_id"], (x["side"] or "").upper()) for x in s0["orders"]} == \
           {(x["market_id"], (x["side"] or "").upper()) for x in s_before["orders"]}, O.describe(s0)
    assert O.table_markets(s0) == {M_BACK, M_PARTIAL, M_LAY}, "tavoli non ricostruiti al riavvio (F018): " + O.describe(s0)
    assert s0["exch_orders"] and len(s0["exch_settlements"]) == 0
    s = second["snaps"][-1]
    # gambe dall'exchange PRIMA del settlement (dopo il settlement currentOrders si svuota): include l'hedge
    o = O.Oracle(s, {M_BACK: True, M_PARTIAL: False, M_LAY: False}, start=START, commission=COMM,
                 extra_legs=O.exchange_legs(s0))
    O.check_single_settlement_per_market(s)
    assert len(s["exch_settlements"]) == 3, O.describe(s, o)
    O.check_realized(s, o)                       # saldo/P&L corretto al millesimo, hedge incluso, nessun profitto inventato
    O.check_conservation(s, o)                   # disponibile + impegnato = 100 + realizzato
    assert O.table_markets(s) == set(), "tavoli non liberati dopo il settlement: " + O.describe(s, o)
    assert s["auto_resets"] == 0, O.describe(s, o)
    O.check_no_duplicate_intents(s, [M_BACK, M_PARTIAL, M_LAY])
    # debito Roserpina: la perdita del parziale e' nel realizzato (negativo per quel mercato)
    assert o.pnl_market(M_PARTIAL) < 0


@pytest.mark.xfail(strict=True, reason="F-01/F-02: il monitor SL/TP e' inerte su main (CSH-01/CSH-02, #586 parte 4); "
                                       "nessun evento SL_TRIGGERED puo' essere osservato finche' F-02 non cabla il percorso")
def test_stop_loss_leg_triggers_on_adverse_price():
    home = tempfile.mkdtemp(prefix="pf_combo001_sl_")
    r = H.run([{"op": "signal", "m": "1.811", "price": 2.0, "side": "BACK", "stake": 3.0},
               {"op": "book", "m": "1.811", "price": 4.0},          # prezzo BACK che sale = perdita liquidabile
               {"op": "wait", "s": 3}],
              home=home, settings=dict(SETTINGS, **{"roserpina.stop_loss_pct": 20.0}), scenario="sim_combo_001_sl")
    assert any(e["e"].startswith("SL_") or "STOP_LOSS" in e["e"] for e in r["events"]), \
        "nessun trigger SL osservato"
