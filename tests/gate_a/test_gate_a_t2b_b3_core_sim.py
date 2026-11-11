"""Gate A T2b batch b3: core/position_ledger.py, core/simulation_order_book.py,
core/simulation_matching_engine.py."""
from __future__ import annotations

import threading

import pytest

from core.position_ledger import PositionLedger
from core.simulation_matching_engine import SimulationMatchingEngine
from core.simulation_order_book import SimulationOrderBook
from core.simulation_state import SimulationState


def _ledger():
    return PositionLedger(market_id="1.1", runner_id=7)


# =========================================================
# PositionLedger
# =========================================================
def test_trg_628_b_0999():
    """ID: TRG-628-B-0999 / PF-B1-R2-LEDGER-DEDUPE

    Un fill_id gia' applicato ritorna duplicate PRIMA di validare il payload:
    anche con payload diverso/invalido nessuna eccezione e nessuna mutazione.
    """
    led = _ledger()
    assert led.apply_fill(fill_id="F1", side="BACK", price=3.0, size=10.0)["applied"] is True
    before = led.snapshot()
    for side, price, size in (("LAY", 2.0, 5.0), ("XX", float("nan"), -1.0), ("BACK", 0.5, 0.0)):
        out = led.apply_fill(fill_id="F1", side=side, price=price, size=size)
        assert out == {"applied": False, "duplicate": True, "realized_delta": 0.0, "snapshot": before}
    assert led.snapshot() == before


def test_trg_628_b_1000():
    """ID: TRG-628-B-1000 / PF-B1-R2-LEDGER-VALID

    Richiede fill_id, lato BACK/LAY, price finito > 1 e size finita > 0 PRIMA di
    qualsiasi mutazione; un fill rifiutato non consuma il fill_id.
    """
    led = _ledger()
    led.apply_fill(fill_id="F0", side="BACK", price=3.0, size=10.0)
    led.mark_to_market(mark_price=2.0)
    before = led.snapshot()
    bad = [
        dict(fill_id="", side="BACK", price=2.0, size=1.0),
        dict(fill_id="  ", side="BACK", price=2.0, size=1.0),
        dict(fill_id="F1", side="BUY", price=2.0, size=1.0),
        dict(fill_id="F1", side="", price=2.0, size=1.0),
        dict(fill_id="F1", side="LAY", price=float("nan"), size=1.0),
        dict(fill_id="F1", side="LAY", price=float("inf"), size=1.0),
        dict(fill_id="F1", side="LAY", price=1.0, size=1.0),
        dict(fill_id="F1", side="LAY", price=0.5, size=1.0),
        dict(fill_id="F1", side="LAY", price=2.0, size=0.0),
        dict(fill_id="F1", side="LAY", price=2.0, size=-1.0),
        dict(fill_id="F1", side="LAY", price=2.0, size=float("inf")),
        dict(fill_id="F1", side="LAY", price=2.0, size=float("nan")),
    ]
    for kwargs in bad:
        with pytest.raises(ValueError):
            led.apply_fill(**kwargs)
        assert led.snapshot() == before, kwargs           # nessuna mutazione (unrealized incluso)
    # il fill_id non e' stato consumato dai tentativi falliti
    assert led.apply_fill(fill_id="F1", side="back", price=2.0, size=1.0)["applied"] is True


def test_trg_628_b_1001():
    """ID: TRG-628-B-1001 / PF-B1-R2-LEDGER-SAME

    Fill sullo stesso lato: apre/accresce size e nozionale (prezzo medio
    ponderato) e azzera l'unrealized precedente.
    """
    led = _ledger()
    s = led.apply_fill(fill_id="A", side="BACK", price=2.0, size=10.0)["snapshot"]
    assert (s.open_side, s.open_size, s.avg_entry_price) == ("BACK", 10.0, 2.0)
    led.mark_to_market(mark_price=1.5)
    assert led.snapshot().unrealized_pnl == pytest.approx(5.0)
    s = led.apply_fill(fill_id="B", side="BACK", price=4.0, size=30.0)["snapshot"]
    assert s.open_side == "BACK"
    assert s.open_size == pytest.approx(40.0)
    assert s.avg_entry_price == pytest.approx((2.0 * 10 + 4.0 * 30) / 40)   # 3.5
    assert s.unrealized_pnl == 0.0
    assert s.realized_pnl == 0.0


def test_trg_628_b_1003():
    """ID: TRG-628-B-1003 / PF-B1-R2-LEDGER-REVERSE

    Fill opposto piu' grande della posizione: chiude tutto e apre il residuo
    sul lato nuovo al prezzo del fill.
    """
    led = _ledger()
    led.apply_fill(fill_id="A", side="BACK", price=3.0, size=10.0)
    s = led.apply_fill(fill_id="B", side="LAY", price=2.5, size=25.0)["snapshot"]
    assert s.open_side == "LAY"
    assert s.open_size == pytest.approx(15.0)
    assert s.avg_entry_price == pytest.approx(2.5)
    # chiusura esatta: nessun residuo, posizione piatta
    led2 = _ledger()
    led2.apply_fill(fill_id="A", side="LAY", price=3.0, size=10.0)
    s2 = led2.apply_fill(fill_id="B", side="BACK", price=3.0, size=10.0)["snapshot"]
    assert (s2.open_side, s2.open_size, s2.avg_entry_price) == ("", 0.0, 0.0)


def test_trg_628_b_1004():
    """ID: TRG-628-B-1004 / PF-B1-R2-LEDGER-LIABILITY

    Esposizione residua per gamba: BACK = size (stake); LAY = size*max(0,avg-1)
    (liability). Posizione piatta = 0.
    """
    back = _ledger()
    s = back.apply_fill(fill_id="A", side="BACK", price=3.0, size=10.0)["snapshot"]
    assert (s.exposure, s.liability) == (10.0, 10.0)
    lay = _ledger()
    s = lay.apply_fill(fill_id="A", side="LAY", price=3.0, size=10.0)["snapshot"]
    assert s.exposure == pytest.approx(20.0) and s.liability == pytest.approx(20.0)
    s = lay.apply_fill(fill_id="B", side="LAY", price=5.0, size=10.0)["snapshot"]   # avg 4.0
    assert s.liability == pytest.approx(20.0 * 3.0)
    s = lay.apply_fill(fill_id="C", side="BACK", price=4.0, size=20.0)["snapshot"]
    assert (s.exposure, s.liability) == (0.0, 0.0)


# =========================================================
# SimulationOrderBook
# =========================================================
def _book_payload(back=None, lay=None):
    return {"runners": [{"selectionId": 7, "ex": {
        "availableToBack": back if back is not None else [{"price": 2.0, "size": 50.0}],
        "availableToLay": lay if lay is not None else [{"price": 2.02, "size": 40.0}]}}]}


class _SpyLock:
    def __init__(self):
        self._lock = threading.RLock()
        self.depth = 0

    def __enter__(self):
        self._lock.acquire()
        self.depth += 1
        return self

    def __exit__(self, *exc):
        self.depth -= 1
        self._lock.release()
        return False


def test_trg_628_b_1006(monkeypatch):
    """ID: TRG-628-B-1006 / PF-B1-R2-BOOK-COPY

    update_market_book salva una deepcopy dell'input, eseguita sotto il lock:
    mutare l'input dopo l'update non altera il book memorizzato.
    """
    import core.simulation_order_book as mod

    ob = SimulationOrderBook()
    spy = _SpyLock()
    ob._lock = spy
    held = []
    real_deepcopy = mod.deepcopy
    monkeypatch.setattr(mod, "deepcopy", lambda x: (held.append(spy.depth), real_deepcopy(x))[1])

    payload = _book_payload()
    ob.update_market_book("1.1", payload)
    assert held and all(d >= 1 for d in held)
    payload["runners"][0]["ex"]["availableToBack"][0]["size"] = 0.0
    payload["runners"].append({"selectionId": 99})
    assert ob.get_available_to_back("1.1", 7) == [{"price": 2.0, "size": 50.0}]
    assert ob.get_runner("1.1", 99) is None


def test_trg_628_b_1007(monkeypatch):
    """ID: TRG-628-B-1007 / PF-B1-R2-BOOK-READ

    get_market_book restituisce una deepcopy sotto lock: mutare il risultato
    non altera lo store; mercato assente => {}.
    """
    import core.simulation_order_book as mod

    ob = SimulationOrderBook()
    ob.update_market_book("1.1", _book_payload())
    spy = _SpyLock()
    ob._lock = spy
    held = []
    real_deepcopy = mod.deepcopy
    monkeypatch.setattr(mod, "deepcopy", lambda x: (held.append(spy.depth), real_deepcopy(x))[1])

    got = ob.get_market_book(" 1.1 ")
    assert held and all(d >= 1 for d in held)
    got["runners"][0]["ex"]["availableToBack"][0]["size"] = 0.0
    got["runners"].clear()
    again = ob.get_market_book("1.1")
    assert again["runners"][0]["ex"]["availableToBack"] == [{"price": 2.0, "size": 50.0}]
    assert again is not got
    assert ob.get_market_book("9.9") == {}


def test_trg_628_b_1008():
    """ID: TRG-628-B-1008 / PF-B1-R2-BOOK-SIDE

    get_opposite_ladder: BACK legge availableToBack, LAY legge availableToLay
    (semantica Betfair, D4 owner 11/11: BACK resta BACK).
    """
    ob = SimulationOrderBook()
    ob.update_market_book("1.1", _book_payload())
    assert ob.get_opposite_ladder("1.1", 7, "BACK") == [{"price": 2.0, "size": 50.0}]
    assert ob.get_opposite_ladder("1.1", 7, " back ") == [{"price": 2.0, "size": 50.0}]
    assert ob.get_opposite_ladder("1.1", 7, "LAY") == [{"price": 2.02, "size": 40.0}]
    assert ob.get_opposite_ladder("1.1", 7, "lay") == [{"price": 2.02, "size": 40.0}]


def test_trg_628_b_1009():
    """ID: TRG-628-B-1009 / PF-B1-R2-BOOK-CONSUME

    consume_liquidity consuma solo i livelli a prezzo IDENTICO al matched, sul
    ladder del lato (BACK->availableToBack, LAY->availableToLay), e rimuove i
    livelli scesi a zero; nessun livello a prezzo diverso viene toccato.
    """
    ob = SimulationOrderBook()
    ob.update_market_book("1.1", _book_payload(
        back=[{"price": 2.0, "size": 5.0}, {"price": 1.98, "size": 7.0}, {"price": 2.0, "size": 3.0}],
        lay=[{"price": 2.02, "size": 4.0}]))
    out = ob.consume_liquidity("1.1", 7, "BACK", 2.0, 6.0)
    assert out == {"ok": True, "consumed": 6.0, "requested": 6.0, "remaining_unfilled": 0.0,
                   "book_key": "availableToBack"}
    assert ob.get_available_to_back("1.1", 7) == [{"price": 1.98, "size": 7.0}, {"price": 2.0, "size": 2.0}]
    assert ob.get_available_to_lay("1.1", 7) == [{"price": 2.02, "size": 4.0}]
    # prezzo non presente => nulla consumato
    out = ob.consume_liquidity("1.1", 7, "BACK", 1.99, 1.0)
    assert out["ok"] is False and out["consumed"] == 0.0 and out["remaining_unfilled"] == 1.0
    # LAY consuma availableToLay fino a zero e rimuove il livello
    out = ob.consume_liquidity("1.1", 7, "LAY", 2.02, 10.0)
    assert out["consumed"] == 4.0 and out["remaining_unfilled"] == 6.0 and out["book_key"] == "availableToLay"
    assert ob.get_available_to_lay("1.1", 7) == []


# =========================================================
# SimulationMatchingEngine
# =========================================================
def _engine(book, *, ratio=0.0, partial=True, ticks=0, consume=True):
    ob = SimulationOrderBook()
    ob.update_market_book("1.1", book)
    st = SimulationState()
    eng = SimulationMatchingEngine(order_book=ob, state=st, partial_fill_enabled=partial,
                                   consume_liquidity=consume, queue_ahead_ratio=ratio, slippage_ticks=ticks)
    return eng, ob, st


def test_trg_628_b_1011():
    """ID: TRG-628-B-1011 / PF-B1-R2-MATCH-CROSS

    BACK attraversa se order <= book (prezzo uguale o migliore per chi punta);
    LAY se order >= book. Mai abbinamento a una quota peggiore della richiesta.
    """
    eng, _, _ = _engine(_book_payload())
    assert eng._crosses(side="BACK", order_price=2.0, book_price=2.0)
    assert eng._crosses(side="BACK", order_price=1.9, book_price=2.0)
    assert not eng._crosses(side="BACK", order_price=2.1, book_price=2.0)
    assert eng._crosses(side="LAY", order_price=2.0, book_price=2.0)
    assert eng._crosses(side="LAY", order_price=2.1, book_price=2.0)
    assert not eng._crosses(side="LAY", order_price=1.9, book_price=2.0)
    # end-to-end: BACK 2.1 su book 2.0 non abbina; LAY 2.0 su lay 2.02 non abbina
    assert eng.submit_order(bet_id="b", market_id="1.1", selection_id=7, side="BACK",
                            price=2.1, size=1.0).matched_size == 0.0
    assert eng.submit_order(bet_id="l", market_id="1.1", selection_id=7, side="LAY",
                            price=2.0, size=1.0).matched_size == 0.0


def test_trg_628_b_1012():
    """ID: TRG-628-B-1012 / PF-B1-R2-MATCH-QUEUE

    La size disponibile di ogni livello si riduce di queue_ahead_ratio; i fill
    si accumulano sui livelli che attraversano fino alla size richiesta.
    """
    book = _book_payload(back=[{"price": 2.2, "size": 10.0}, {"price": 2.1, "size": 20.0},
                               {"price": 2.0, "size": 100.0}])
    eng, _, _ = _engine(book, ratio=0.5, consume=False)
    m = eng._simulate_match(market_id="1.1", selection_id=7, side="BACK", price=2.0, size=12.0)
    assert m["fills"] == [{"price": 2.2, "size": 5.0}, {"price": 2.1, "size": 7.0}]
    assert m["matched_size"] == pytest.approx(12.0) and m["remaining_size"] == 0.0
    assert m["status"] == "EXECUTION_COMPLETE"
    # liquidita' totale dopo coda insufficiente => PARTIAL col residuo esatto
    m = eng._simulate_match(market_id="1.1", selection_id=7, side="BACK", price=2.1, size=40.0)
    assert m["matched_size"] == pytest.approx(15.0) and m["remaining_size"] == pytest.approx(25.0)
    assert m["status"] == "PARTIAL"


def test_trg_628_b_1013():
    """ID: TRG-628-B-1013 / PF-B1-R2-MATCH-PARTIAL-FLAG

    Con partial_fill_enabled=False il matching esce dopo il PRIMO livello
    utile anche se incompleto (nessun abbinamento garantito oltre quel livello).
    """
    book = _book_payload(back=[{"price": 2.2, "size": 4.0}, {"price": 2.1, "size": 20.0}])
    eng, _, _ = _engine(book, partial=False, consume=False)
    m = eng._simulate_match(market_id="1.1", selection_id=7, side="BACK", price=2.0, size=10.0)
    assert m["fills"] == [{"price": 2.2, "size": 4.0}]
    assert m["remaining_size"] == pytest.approx(6.0) and m["status"] == "PARTIAL"
    eng_on, _, _ = _engine(book, partial=True, consume=False)
    assert eng_on._simulate_match(market_id="1.1", selection_id=7, side="BACK",
                                  price=2.0, size=10.0)["matched_size"] == pytest.approx(10.0)


def test_trg_628_b_1017():
    """ID: TRG-628-B-1017 / PF-B1-R2-MATCH-REPROCESS

    reprocess_open_orders abbina SOLO il residuo (size - matched) e pesa il
    prezzo medio con i fill pregressi; ordini completi non toccati.
    """
    eng, ob, st = _engine(_book_payload(back=[{"price": 2.0, "size": 4.0}]))
    r = eng.submit_order(bet_id="B1", market_id="1.1", selection_id=7, side="BACK", price=1.9, size=10.0)
    assert (r.matched_size, r.status) == (4.0, "PARTIAL")
    ob.update_market_book("1.1", _book_payload(back=[{"price": 3.0, "size": 100.0}]))
    out = eng.reprocess_open_orders("1.1")
    assert out["results"] == [{"bet_id": "B1", "newly_matched": 6.0, "total_matched": 10.0,
                               "avg_price_matched": pytest.approx((2.0 * 4 + 3.0 * 6) / 10),
                               "status": "EXECUTION_COMPLETE"}]
    pos = st.get_position("B1")
    assert pos.matched_size == 10.0 and pos.avg_price_matched == pytest.approx(2.6)
    assert eng.reprocess_open_orders("1.1")["processed_count"] == 0      # completo: nulla da abbinare


def test_trg_628_b_1018():
    """ID: TRG-628-B-1018 / PF-B1-R2-MATCH-CANCEL

    cancel_order calcola size_cancelled = max(0, requested - matched) e chiede
    cancel_position; bet ignoto => bet_not_found.
    """
    eng, ob, st = _engine(_book_payload(back=[{"price": 2.0, "size": 4.0}]))
    eng.submit_order(bet_id="B1", market_id="1.1", selection_id=7, side="BACK", price=2.0, size=10.0)
    out = eng.cancel_order("B1")
    assert out == {"ok": True, "bet_id": "B1", "size_cancelled": 6.0, "status": "EXECUTION_COMPLETE"}
    eng.submit_order(bet_id="B2", market_id="1.1", selection_id=7, side="BACK", price=5.0, size=3.0)
    assert eng.cancel_order("B2")["size_cancelled"] == 3.0
    assert st.get_position("B2").status == "CANCELLED"
    assert eng.cancel_order("NOPE") == {"ok": False, "reason": "bet_not_found", "size_cancelled": 0.0}


@pytest.mark.xfail(strict=True, reason=(
    "TRG-628-B-1018: il residuo dichiarato cancellato (size_cancelled) resta abbinabile: "
    "reprocess_open_orders lo riabbina perche' cancel_position lascia size invariata e stato "
    "EXECUTION_COMPLETE (PF-EXEC-01: ripuntare/abbinare solo dopo cancellazione confermata, mai il cancellato)"))
def test_trg_628_b_1018_cancelled_residual_never_matched():
    """ID: TRG-628-B-1018 / PF-B1-R2-MATCH-CANCEL

    Dopo cancel_order di un ordine parziale, il residuo cancellato non deve
    essere abbinato da un reprocess successivo.
    """
    eng, ob, st = _engine(_book_payload(back=[{"price": 2.0, "size": 4.0}]))
    eng.submit_order(bet_id="B1", market_id="1.1", selection_id=7, side="BACK", price=2.0, size=10.0)
    assert eng.cancel_order("B1")["size_cancelled"] == 6.0
    ob.update_market_book("1.1", _book_payload(back=[{"price": 2.0, "size": 100.0}]))
    eng.reprocess_open_orders("1.1")
    assert st.get_position("B1").matched_size == 4.0
