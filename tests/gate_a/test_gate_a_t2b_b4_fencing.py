"""Gate A T2b batch b4 — fencing/esposizioni: metodi reali di RuntimeController non avviato
(__new__), TableManager e DuplicationGuard reali, nessuna rete, nessun ordine."""
from __future__ import annotations

from core.duplication_guard import DuplicationGuard
from core.runtime_controller import RuntimeController
from core.table_manager import TableManager


def _rc(tm):
    rc = RuntimeController.__new__(RuntimeController)
    rc.table_manager = tm
    rc.duplication_guard = DuplicationGuard()
    return rc


def _act(tm, tid, key, exp, market_id="1.1", meta=None):
    tm.activate(table_id=tid, event_key=key, exposure=exp, market_id=market_id, selection_id=1, meta=meta)


def test_trg_628_b_0849():
    """ID: TRG-628-B-0849 / PF-B1-PR505-EVENT-EXPOSURE

    Somma i tavoli dello stesso evento: event_id primario quando presente su entrambi (due id diversi mai
    fusi, anche con stesso nome), fallback sui nomi (anche legacy `event_identity` name:) se un lato e' privo
    di id; esposizione negativa/non finita => inf."""
    tm = TableManager(table_count=5)
    _act(tm, 1, "k1", 10.0, meta={"event_id": "100", "event_name": "Inter - Milan"})
    _act(tm, 2, "k2", 5.0, meta={"event_id": "200", "event_name": "Inter - Milan"})
    _act(tm, 3, "k3", 2.0, meta={"event_identity": "name:inter - milan"})
    _act(tm, 4, "k4", 7.0, meta={"event_id": "300", "event_name": "Roma - Lazio"})
    rc = _rc(tm)
    # id 100 su entrambi -> solo t1; t3 (senza id) per nome; t2 id diverso escluso
    assert rc._event_current_exposure({"event_id": "100", "event_name": "Inter - Milan",
                                       "market_id": "9.9", "selection_id": 9}) == 12.0
    # segnale senza id: nome confronta con tutti gli alias -> t1+t2+t3
    assert rc._event_current_exposure({"event_name": "  INTER -  milan", "market_id": "9.9",
                                       "selection_id": 9}) == 17.0
    assert rc._event_current_exposure({}) == 0.0
    tm._tables[3].current_exposure = -1.0
    assert rc._event_current_exposure({"event_name": "Inter - Milan"}) == float("inf")


def test_trg_628_b_0850():
    """ID: TRG-628-B-0850 / PF-B1-PR505-MARKET-EXPOSURE

    Somma i tavoli attivi sul market id canonico; market id richiesto invalido, market id salvato invalido
    o esposizione non finita/negativa => inf (fail-closed)."""
    tm = TableManager(table_count=4)
    _act(tm, 1, "k1", 10.0, market_id="1.1")
    _act(tm, 2, "k2", 4.0, market_id=" 1.1 ")
    _act(tm, 3, "k3", 99.0, market_id="1.2")
    rc = _rc(tm)
    assert rc._market_current_exposure("1.1") == 14.0
    assert rc._market_current_exposure("1.9") == 0.0
    for bad in ("", None, True, float("nan")):
        assert rc._market_current_exposure(bad) == float("inf"), bad
    tm._tables[2].current_exposure = float("nan")
    assert rc._market_current_exposure("1.1") == float("inf")
    tm._tables[2].current_exposure = 4.0
    _act(tm, 4, "k4", 1.0, market_id="")
    assert rc._market_current_exposure("1.1") == float("inf")


def test_trg_628_b_0851():
    """ID: TRG-628-B-0851 / PF-B1-PR505-FILLED-EXPOSURE

    QUICK_BET_FILLED chiude l'ordine, non la posizione: tavolo, evento ed esposizione restano (cap mercato/
    evento invariati); un esito FAILED dello stesso intento invece libera il tavolo."""
    tm = TableManager(table_count=2)
    _act(tm, 1, "ek", 10.0, meta={"customer_ref": "q-1"})
    rc = _rc(tm)
    rc.duplication_guard.acquire("ek")
    payload = {"event_key": "ek", "table_id": 1, "customer_ref": "q-1"}
    rc._release_if_terminal(dict(payload), event_name="QUICK_BET_FILLED")
    t = tm.get_table(1)
    assert t.current_event_key == "ek" and t.current_exposure == 10.0
    assert rc._market_current_exposure("1.1") == 10.0
    rc._release_if_terminal(dict(payload), event_name="QUICK_BET_FAILED")
    assert tm.get_table(1).current_event_key == "" and tm.total_exposure() == 0.0
