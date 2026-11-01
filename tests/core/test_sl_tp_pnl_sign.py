"""F-01 (#586, CSH-01/CSH-10/NF-06, P0 latente) — P&L liquidabile del monitor SL/TP (RED prima della patch).

Il monitor (`RuntimeController._monitor_sl_tp_trailing`, inerte fino a F-02) stimava il P&L con
`stake*(prezzo_corrente - ingresso)/ingresso` per il BACK: segno INVERTITO (un BACK a 2.0 che passa a 2.5 e' in
perdita), `lastPriceTraded` al posto del prezzo di chiusura dal book, esposizione al posto dello stake per il LAY,
nessuna commissione. Atteso (decisione owner #426 c.6112703848, Decimal c.6141450045 Q7): P&L liquidabile = green-up
del cashout a quel prezzo (stessa funzione del cashout), commissione sul netto positivo, confronto SL/TP in Decimal.

Oracolo indipendente (stake 10, ingresso 2.0, commissione 4.5 %), chiusura al lato opposto del book:
- BACK, chiusura LAY a 2.5: hedge 8.00, netto -2.00 (perdita)   | a 1.6: hedge 12.50, lordo +2.50, netto +2.39
- LAY,  chiusura BACK a 2.5: hedge 8.00, lordo +2.00, netto +1.91 | a 1.6: hedge 12.50, netto -2.50
- LAY con liability 10 a 3.0 = stake 5 (non 10): chiusura BACK a 4.0 => netto +1.19 (con stake 10: +2.39).
"""
from __future__ import annotations

from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from core.runtime_controller import RuntimeController
from core.system_state import RuntimeMode

pytestmark = [pytest.mark.unit, pytest.mark.safety]


class _Bus:
    def __init__(self):
        self.published = []

    def publish(self, topic, payload):
        self.published.append((topic, payload))


def _controller(table):
    rc = RuntimeController.__new__(RuntimeController)
    rc.mode = RuntimeMode.ACTIVE
    rc.bus = _Bus()
    rc.table_manager = SimpleNamespace(_tables={table.table_id: table})
    return rc


def _table(side, entry, exposure, sl=None, tp=None, stake=None):
    meta = {"price": entry, "bet_type": side, "stop_loss": sl, "take_profit": tp}
    if stake is not None:
        meta["stake"] = stake
    return SimpleNamespace(table_id=1, status="ACTIVE", market_id="1.5", selection_id=11, meta=meta,
                           current_exposure=exposure)


def _book(back, lay, last=None):
    """back = miglior prezzo availableToBack, lay = miglior availableToLay; `last` = lastPriceTraded (ingannevole)."""
    return {"marketId": "1.5", "runners": [{"selectionId": 11, "lastPriceTraded": last if last is not None else back,
                                            "ex": {"availableToBack": [{"price": back, "size": 100}],
                                                   "availableToLay": [{"price": lay, "size": 100}]}}]}


def _closes(rc):
    return [p for t, p in rc.bus.published if t == "REQ_EXECUTE_CASHOUT"]


def test_back_price_up_is_loss():
    # BACK 10 a 2.0, il mercato sale: chiusura LAY a 2.5 => -2.00. SL 1.5 deve scattare, TP 1.0 no.
    rc = _controller(_table("BACK", 2.0, 10.0, sl=1.5, tp=1.0))
    rc._monitor_sl_tp_trailing(_book(back=2.48, lay=2.5))
    closes = _closes(rc)
    assert len(closes) == 1 and closes[0]["reason"] == "STOP_LOSS_REACHED", closes


def test_back_price_down_is_profit_net_of_commission():
    # BACK 10 a 2.0, chiusura LAY a 1.6 => lordo +2.50, netto +2.39: TP 2.39 scatta, TP 2.40 no.
    rc = _controller(_table("BACK", 2.0, 10.0, tp=2.39))
    rc._monitor_sl_tp_trailing(_book(back=1.58, lay=1.6))
    assert [c["reason"] for c in _closes(rc)] == ["TAKE_PROFIT_REACHED"]
    rc = _controller(_table("BACK", 2.0, 10.0, tp=2.40))
    rc._monitor_sl_tp_trailing(_book(back=1.58, lay=1.6))
    assert _closes(rc) == [], "TP valutato sul lordo: la commissione va applicata al netto"


def test_lay_price_up_is_profit():
    # LAY 10 a 2.0 (liability 10), chiusura BACK a 2.5 => netto +1.91: TP 1.9 scatta, nessuno SL.
    rc = _controller(_table("LAY", 2.0, 10.0, sl=1.0, tp=1.9, stake=10.0))
    rc._monitor_sl_tp_trailing(_book(back=2.5, lay=2.52))
    assert [c["reason"] for c in _closes(rc)] == ["TAKE_PROFIT_REACHED"]


def test_lay_uses_stake_not_liability():
    # LAY a 3.0 con liability 10 => stake 5 (nessuno stake in meta: ricostruito da exposure/(price-1)).
    # Chiusura BACK a 4.0: hedge 3.75, lordo +1.25, netto +1.19. Con stake = liability (10, errato): netto +2.39.
    rc = _controller(_table("LAY", 3.0, 10.0, tp=1.19))
    rc._monitor_sl_tp_trailing(_book(back=4.0, lay=4.1))
    assert [c["reason"] for c in _closes(rc)] == ["TAKE_PROFIT_REACHED"]
    rc = _controller(_table("LAY", 3.0, 10.0, tp=1.20))
    rc._monitor_sl_tp_trailing(_book(back=4.0, lay=4.1))
    assert _closes(rc) == [], "LAY valutato con la liability come stake"


def test_commission_applied_on_net_and_not_on_losses():
    from core.sl_tp_pnl import liquidable_pnl
    assert liquidable_pnl("BACK", D("10"), D("2.0"), D("1.6"), D("4.5")) == D("2.39")
    assert liquidable_pnl("BACK", D("10"), D("2.0"), D("2.5"), D("4.5")) == D("-2.00")
    assert liquidable_pnl("LAY", D("10"), D("2.0"), D("2.5"), D("4.5")) == D("1.91")
    assert liquidable_pnl("LAY", D("10"), D("2.0"), D("1.6"), D("4.5")) == D("-2.50")


def test_last_price_traded_is_not_used():
    # lastPriceTraded lontano dal book (2.9) ma la chiusura reale e' il LAY a 2.5 (-2.00): decide il book, SL.
    rc = _controller(_table("BACK", 2.0, 10.0, sl=1.5, tp=1.0))
    rc._monitor_sl_tp_trailing(_book(back=2.48, lay=2.5, last=2.9))
    assert [c["reason"] for c in _closes(rc)] == ["STOP_LOSS_REACHED"]


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -1.0, "abc"])
def test_invalid_book_price_never_triggers(bad):
    rc = _controller(_table("BACK", 2.0, 10.0, sl=0.01, tp=0.01))
    rc._monitor_sl_tp_trailing(_book(back=bad, lay=bad, last=bad))
    assert _closes(rc) == []


def test_config_commission_other_than_policy_neither_raises_nor_changes_the_decision():
    """La funzione del cashout applica la policy commissione Italia (4.5) e solleva per altri valori: il monitor usa
    la stessa commissione dell'hedge reale e non solleva nell'handler con una configurazione SIM diversa."""
    rc = _controller(_table("BACK", 2.0, 10.0, sl=1.5))
    rc.config = SimpleNamespace(commission_pct=1.0)
    rc._monitor_sl_tp_trailing(_book(back=2.48, lay=2.5))
    assert [c["reason"] for c in _closes(rc)] == ["STOP_LOSS_REACHED"]


# --- Sol su 6d232f4 -------------------------------------------------------------------------------------------------

def _ladder_book(back_levels, lay_levels):
    return {"marketId": "1.5", "runners": [{"selectionId": 11, "ex": {
        "availableToBack": [{"price": p, "size": s} for p, s in back_levels],
        "availableToLay": [{"price": p, "size": s} for p, s in lay_levels]}}]}


def test_min_betfair_price_1_01_still_triggers_the_stop_loss():
    # LAY 10 a 3.0 (liability 20), la selezione sta vincendo: chiusura BACK a 1.01 => hedge 29.70, netto -19.70.
    # Prima 1.01 era scartata come prezzo non valido e lo SL spariva dal monitor.
    from core.sl_tp_pnl import liquidable_pnl
    assert liquidable_pnl("LAY", D("10"), D("3.0"), D("1.01"), D("4.5")) == D("-19.70")
    rc = _controller(_table("LAY", 3.0, 20.0, sl=5.0, stake=10.0))
    rc._monitor_sl_tp_trailing(_ladder_book([(1.01, 1000)], [(1.02, 1000)]))
    assert [c["reason"] for c in _closes(rc)] == ["STOP_LOSS_REACHED"]


def test_take_profit_requires_enough_depth_to_close():
    # BACK 10 a 2.0, chiusura LAY a 1.60 serve hedge 12.50: con 0.01 disponibili il profitto non e' liquidabile.
    rc = _controller(_table("BACK", 2.0, 10.0, tp=1.0))
    rc._monitor_sl_tp_trailing(_ladder_book([(1.58, 100)], [(1.60, 0.01)]))
    assert _closes(rc) == [], "TP su profitto non liquidabile (profondita' ignorata)"


def test_take_profit_uses_the_deepest_level_needed():
    # 1.60 x 5 + 1.70 x 100: hedge a 1.70 = 11.77 coperto solo arrivando al secondo livello => prezzo 1.70,
    # netto +1.69 (non +2.39 del miglior prezzo). TP 2.0 non scatta, TP 1.6 si'.
    from core.sl_tp_pnl import closing_quote, liquidable_pnl
    runner = _ladder_book([(1.58, 100)], [(1.60, 5), (1.70, 100)])["runners"][0]
    assert closing_quote(runner, "BACK", D("10"), D("2.0")) == (D("1.7"), True)
    assert liquidable_pnl("BACK", D("10"), D("2.0"), D("1.7"), D("4.5")) == D("1.69")
    rc = _controller(_table("BACK", 2.0, 10.0, tp=2.0))
    rc._monitor_sl_tp_trailing(_ladder_book([(1.58, 100)], [(1.60, 5), (1.70, 100)]))
    assert _closes(rc) == []
    rc = _controller(_table("BACK", 2.0, 10.0, tp=1.6))
    rc._monitor_sl_tp_trailing(_ladder_book([(1.58, 100)], [(1.60, 5), (1.70, 100)]))
    assert [c["reason"] for c in _closes(rc)] == ["TAKE_PROFIT_REACHED"]


def test_stop_loss_still_triggers_with_insufficient_depth():
    # Chiusura protettiva (PF-EXEC-03): BACK 10 a 2.0, il book LAY offre solo 1.00 a 2.5 (servono 8.00).
    # La perdita sull'ultimo livello (-2.00) e' un limite inferiore di quella reale: lo SL scatta comunque.
    from core.sl_tp_pnl import closing_quote
    runner = _ladder_book([(2.48, 100)], [(2.5, 1)])["runners"][0]
    assert closing_quote(runner, "BACK", D("10"), D("2.0")) == (D("2.5"), False)
    rc = _controller(_table("BACK", 2.0, 10.0, sl=1.5))
    rc._monitor_sl_tp_trailing(_ladder_book([(2.48, 100)], [(2.5, 1)]))
    assert [c["reason"] for c in _closes(rc)] == ["STOP_LOSS_REACHED"]


@pytest.mark.parametrize("size", [float("nan"), -1, "x", None])
def test_unreadable_depth_never_counts_as_covered(size):
    from core.sl_tp_pnl import closing_quote
    runner = {"ex": {"availableToLay": [{"price": 1.6, "size": size}]}}
    assert closing_quote(runner, "BACK", D("10"), D("2.0")) is None
    rc = _controller(_table("BACK", 2.0, 10.0, tp=0.01))
    rc._monitor_sl_tp_trailing({"runners": [{"selectionId": 11, "ex": {"availableToLay": [{"price": 1.6, "size": size}]}}]})
    assert _closes(rc) == []


@pytest.mark.parametrize("side", ["BACK", "LAY"])
@pytest.mark.parametrize("entry", ["1.5", "2.0", "3.4", "7.0"])
@pytest.mark.parametrize("close", ["1.02", "1.6", "2.5", "4.1", "12.0"])
def test_same_result_as_the_cashout_function_where_it_is_defined(side, entry, close):
    # la formula propria coincide con dutching.dynamic_cashout_single dove questa e' definita (> 1.01)
    from core.sl_tp_pnl import liquidable_pnl
    from dutching import dynamic_cashout_single
    ref = dynamic_cashout_single(matched_stake=10.0, matched_price=float(entry), current_price=float(close),
                                 commission=4.5, side=side)["net_profit"]
    assert liquidable_pnl(side, D("10"), D(entry), D(close), D("4.5")) == D(str(ref)).quantize(D("0.01"))
