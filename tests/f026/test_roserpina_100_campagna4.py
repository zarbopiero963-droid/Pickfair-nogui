"""Simulazione Roserpina 100 EUR — campagna 4: lacune della matrice finanziaria (LAY, sospensione allo scatto). SINTETICO.

HeadlessApp reale, exchange SIM. Senza F-02/F-04/F-03 (`core.trailing_stop`) SALTATA.
- B1 posizione LAY con SL: la quota scende (contro il bancatore), lo SL chiude con un hedge BACK, un solo hedge;
- B2 posizione LAY con trailing: la quota sale (profitto del bancatore), poi ritraccia: trailing chiude con un BACK;
- B3 SL raggiunto mentre il mercato e' SOSPESO: nessun hedge durante la sospensione (PF-EXEC-05); alla riapertura, con
  la perdita ancora oltre lo SL, la protezione deve chiudere la posizione (una volta sola). DIFETTO D-C4-01 (xfail strict):
  il monitor valuta lo SL anche sul book SOSPESO, imposta il latch `sl_tp_triggered`, F-04 esita NOT_EXECUTED (ritentabile)
  ma il latch impedisce ogni nuovo scatto: alla riapertura la posizione resta aperta senza protezione;
- B4 trailing su un LAY mai abbinato: nessun profitto da proteggere, nessuno scatto, l'ingresso resta in attesa.
  DIFETTO D-C4-02 (xfail strict): `sl_tp_pnl.position_stake` usa lo stake RICHIESTO (meta['stake']) e non quello
  abbinato: il trailing scatta su un "profitto" fittizio e la chiusura F-04 cancella l'ingresso non abbinato.

Nel SIM un LAY alla quota del segnale non e' abbinato (book: lay = quota + 0.02): l'abbinamento arriva quando la quota
scende sotto quella del segnale (op `price`).
"""
from __future__ import annotations

import importlib.util
from decimal import Decimal as D

import pytest

from tests.f026.test_f026_financial import describe, run
from tests.f026.test_roserpina_100_campagna3 import POLL, _closes, _oracle_ok
from tools.sim_report.roserpina_report import build_report

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    importlib.util.find_spec("core.trailing_stop") is None,
    reason="richiede F-02 + F-04 + F-03 (claude/d-f03-trailing-stop): SL/TP/trailing non sono su main")]


def _hedges(snaps, market, side):
    return {o["ref"] for o in snaps[-1]["exch_orders"] if str(o["market_id"]) == market and o["side"] == side
            and str(o["ref"]).startswith("hedge:")}


def test_b1_lay_con_sl_chiude_con_un_back() -> None:
    r1 = run([{"op": "signal", "m": "1.961", "price": 3.0, "side": "LAY", "stop_loss_pct": 10},
              {"op": "price", "m": "1.961", "price": 2.0}, POLL])          # quota giu': perdita del bancatore
    r2 = run([{"op": "settle", "m": "1.961", "win": True}], home=r1["_home"])
    rep = build_report([r1, r2], provenance="SINTETICO", scenario="roserpina-100-lay-sl-v1")
    assert len(_hedges(r1["snaps"], "1.961", "BACK")) == 1, describe(r1["snaps"][-1])
    assert len(_closes(rep, "1.961", "SL_TP_CLOSE_ROUTED")) == 1
    _oracle_ok(rep, r1["snaps"] + r2["snaps"], {"1.961": True})


def test_b2_lay_con_trailing_chiude_con_un_back() -> None:
    r1 = run([{"op": "signal", "m": "1.962", "price": 2.0, "side": "LAY", "trailing_stop_pct": 20},
              {"op": "price", "m": "1.962", "price": 1.9}, POLL,             # abbinato a 1.92
              {"op": "price", "m": "1.962", "price": 3.0}, POLL,             # quota su: profitto del bancatore
              {"op": "price", "m": "1.962", "price": 3.4}, POLL,             # picco piu' alto
              {"op": "price", "m": "1.962", "price": 2.4}, POLL])            # ritracciata oltre la soglia
    rep = build_report([r1], provenance="SINTETICO", scenario="roserpina-100-lay-trailing-v1")
    trig = [t for t in rep["protections"]["triggers"] if t["market_id"] == "1.962"]
    assert [t["event"] for t in trig] == ["TRAILING_TRIGGERED"], trig
    assert len(_hedges(r1["snaps"], "1.962", "BACK")) == 1, describe(r1["snaps"][-1])


@pytest.mark.xfail(strict=True, reason="D-C4-01: SL valutato su mercato sospeso, latch F-02 mai rilasciato dopo NOT_EXECUTED")
def test_b3_sl_raggiunto_a_mercato_sospeso_chiude_alla_riapertura() -> None:
    r1 = run([{"op": "signal", "m": "1.963", "price": 2.0, "stop_loss_pct": 10},
              {"op": "price", "m": "1.963", "price": 3.0, "status": "SUSPENDED"}, POLL,   # oltre lo SL, mercato sospeso
              {"op": "reopen", "m": "1.963"}, POLL, POLL])                   # riapre con la perdita ancora oltre lo SL
    rep = build_report([r1], provenance="SINTETICO", scenario="roserpina-100-sl-sospeso-v1")
    sospeso = [s for s in r1["snaps"] if s["tag"].startswith(("1:price", "2:wait"))]
    assert len(sospeso) == 2, [s["tag"] for s in r1["snaps"]]
    assert all(not _hedges([s], "1.963", "LAY") for s in sospeso), "hedge su mercato sospeso"
    assert len(_hedges(r1["snaps"], "1.963", "LAY")) == 1, (
        "SL oltre soglia alla riapertura ma posizione non chiusa: " + str(rep["protections"]))


def _matched(snaps, market):
    return sum(float(o.get("matched") or 0) for o in snaps[-1]["exch_orders"] if str(o["market_id"]) == market)


@pytest.mark.xfail(strict=True, reason="D-C4-02: protezioni valutate sullo stake richiesto, non su quello abbinato")
def test_b4_trailing_su_lay_non_abbinato_non_scatta() -> None:
    r1 = run([{"op": "signal", "m": "1.964", "price": 2.0, "side": "LAY", "trailing_stop_pct": 20},
              {"op": "price", "m": "1.964", "price": 3.0}, POLL,             # mai abbinato: quota sale
              {"op": "price", "m": "1.964", "price": 3.4}, POLL,
              {"op": "price", "m": "1.964", "price": 2.4}, POLL])
    rep = build_report([r1], provenance="SINTETICO", scenario="roserpina-100-lay-unmatched-v1")
    assert _matched(r1["snaps"], "1.964") == 0, describe(r1["snaps"][-1])        # precondizione: nulla abbinato
    assert [t for t in rep["protections"]["triggers"] if t["market_id"] == "1.964"] == [], rep["protections"]
    assert [o["status"] for o in r1["snaps"][-1]["exch_orders"] if str(o["market_id"]) == "1.964"] == ["EXECUTABLE"]
