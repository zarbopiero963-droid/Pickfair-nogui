"""F-03 (#586, K-7) — trailing stop: soglia mobile sul P&L liquidabile garantito, picco persistito, chiusura F-04.

Scheda F-03 (c.6504942027 / parte 4 c.6502715406): RED `test_trailing_moves_with_profit_and_never_back`,
`test_trailing_restart_keeps_peak`; esempio "profitto 10 -> 15 -> 12 con trailing 20 % => scatta a 12"; precedenza
hard-stop > SL > trailing > TP; quote assenti/stale/mercato sospeso => nessuna decisione; kill fra aggiornamento del
picco e persistenza => al riavvio picco <= ultimo persistito. Catena reale: RuntimeController + TradingEngine +
SimulationBroker + SQLite, cashout cablato come headless (harness di F-04). Quote SINTETICHE.
"""
from __future__ import annotations

import json
from decimal import Decimal as D

import pytest

from core import sl_tp_close, trailing_stop
from tests.acceptance.test_f04_sl_tp_table_close import MKT, SEL_A, SEL_B, _book, _chain


# ------------------------------------------------------------------ logica pura (Decimal)
def test_esempio_della_scheda_10_15_12_con_trailing_20() -> None:
    d1 = trailing_stop.evaluate(None, "10", "20")
    d2 = trailing_stop.evaluate(d1.peak, "15", "20")
    d3 = trailing_stop.evaluate(d2.peak, "12", "20")

    assert (d1.peak, d1.triggered, d1.threshold) == (D("10.00"), False, D("8.00"))
    assert (d2.peak, d2.peak_raised, d2.triggered, d2.threshold) == (D("15.00"), True, False, D("12.00"))
    assert d3.peak == D("15.00") and d3.peak_raised is False and d3.triggered is True


def test_picco_mai_indietro_e_nessuno_scatto_nel_tick_che_lo_alza() -> None:
    d = trailing_stop.evaluate("15.00", "14.50", "20")
    assert d.peak == D("15.00") and not d.peak_raised and not d.triggered
    d = trailing_stop.evaluate("15.00", "20", "100")       # pct 100: soglia = 0, il tick che alza non scatta
    assert d.peak == D("20.00") and d.peak_raised and not d.triggered


def test_non_armato_senza_profitto() -> None:
    d = trailing_stop.evaluate(None, "-3", "20")
    assert d.peak is None and not d.armed and not d.triggered


def test_soglia_al_centesimo_per_difetto() -> None:
    assert trailing_stop.threshold(D("1.37"), D("33")) == D("0.91")     # 0.9179 -> 0.91, mai sopra


@pytest.mark.parametrize("pct", [None, 0, -5, 150, "nan", True, "abc", "inf"])
def test_block_pct_non_valida_trailing_spento(pct) -> None:
    assert trailing_stop.evaluate(None, "10", pct) is None


@pytest.mark.parametrize("pnl", [None, "nan", True, "x"])
def test_block_pnl_non_valido_nessuna_decisione(pnl) -> None:
    assert trailing_stop.evaluate("5", pnl, "20") is None


@pytest.mark.parametrize("raw,atteso", [
    (None, (None, "")), ("", (None, "")),
    ("{rotto", (None, "TRAILING_STATE_CORRUPT")),
    (json.dumps({"v": 2, "peak": "3"}), (None, "TRAILING_STATE_CORRUPT")),
    (json.dumps({"v": 1, "peak": "nan"}), (None, "TRAILING_STATE_CORRUPT")),
    (json.dumps({"v": 1, "peak": "-1"}), (None, "TRAILING_STATE_CORRUPT")),
    (json.dumps({"v": 1, "peak": "1.25"}), (D("1.25"), "")),
])
def test_stato_persistito_letto_fail_closed(raw, atteso) -> None:
    assert trailing_stop.read_peak(raw) == atteso


# ------------------------------------------------------------------ catena reale
def _enter(c, selection, received_at, **extra):
    c.rc._on_signal_received({"market_id": MKT, "selection_id": selection, "price": 2.0, "bet_type": "BACK",
                              "simulation_mode": True, "chat_id": -100123, "received_at": received_at,
                              "event": "Alpha v Beta", "market": "Match Odds", "selection": f"S{selection}", **extra})


def _two_tables(c, **extra):
    """A (11) con trailing 20 %, B (12) sulla stessa partita senza protezioni; entrambi BACK 4 @ 2.0 abbinati."""
    _book(c.broker)
    _enter(c, SEL_A, "2026-11-05T10:00:00.000001+00:00", trailing_stop_pct=20, **extra)
    _enter(c, SEL_B, "2026-11-05T10:00:01.000001+00:00")
    tables = sorted(c.rc.table_manager.active_tables(), key=lambda t: t.table_id)
    assert len(tables) == 2 and tables[0].meta.get("trailing_stop_pct") == 20.0, [t.meta for t in tables]
    return tables


def _tick(c, lay, status="OPEN"):
    _book(c.broker, status, lay=(lay, 500.0))
    c.rc._monitor_sl_tp_trailing(dict(c.broker.get_market_book(MKT), marketId=MKT))


def _live(c, table):
    """Tavolo vivo (quello che legge il monitor): `active_tables`/`get_table` restituiscono copie."""
    return c.rc.table_manager._tables[table.table_id]


def _events(c, topic):
    return [p for t, p in c.bus.events if t == topic]


def _peak_db(c, table):
    raw = c.db.get_settings().get(trailing_stop.PEAK_KEY_PREFIX + table.meta["customer_ref"])
    return trailing_stop.read_peak(raw)[0]


def test_trailing_moves_with_profit_and_never_back(tmp_path) -> None:
    c = _chain(tmp_path)
    a, b = _two_tables(c)
    _tick(c, 1.6)                       # profitto
    p1 = _peak_db(c, a)
    _tick(c, 1.4)                       # piu' profitto: il picco sale ed e' persistito
    p2 = _peak_db(c, a)
    _tick(c, 1.45)                      # ritraccia poco: sopra soglia, picco invariato
    assert p1 is not None and p2 > p1 and _peak_db(c, a) == p2
    assert _events(c, trailing_stop.TRAILING_TRIGGERED) == []

    _tick(c, 1.6)                       # ritraccia sotto picco x 0,8 => scatta, chiude SOLO A
    ev = _events(c, trailing_stop.TRAILING_TRIGGERED)
    assert len(ev) == 1 and ev[0]["table_id"] == a.table_id and ev[0]["reason"] == trailing_stop.TRAILING_REASON
    assert D(str(ev[0]["threshold"])) == trailing_stop.threshold(p2, D("20"))
    reqs = c.bus.payloads("REQ_EXECUTE_CASHOUT")
    assert len(reqs) == 1 and str(reqs[0]["selection_id"]) == str(SEL_A)
    assert [t for t, _ in c.bus.events if t.startswith("SL_TP_CLOSE_")] == [sl_tp_close.SL_TP_CLOSE_ROUTED]
    _tick(c, 1.8)                       # nessun secondo scatto
    assert len(_events(c, trailing_stop.TRAILING_TRIGGERED)) == 1 and len(c.bus.payloads("REQ_EXECUTE_CASHOUT")) == 1
    assert _live(c, b).meta.get("sl_tp_triggered") is None and _live(c, a).meta.get("sl_tp_triggered") == "TRAILING_STOP_REACHED"


def _restart(c, tmp_path):
    c2 = _chain(tmp_path, broker=c.broker)
    for t in c.rc.table_manager.active_tables():
        snap = c.rc.table_manager.snapshot_table(t.table_id)
        table = dict(snap["table"])
        meta = dict(table.get("meta") or {})
        for k in ("trailing_peak", "trailing_threshold", "sl_tp_triggered"):   # stato in memoria perso col processo
            meta.pop(k, None)
        table["meta"] = meta
        c2.rc.table_manager.restore_table(dict(snap, table=table))
    return c2


def test_trailing_restart_keeps_peak(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b = _two_tables(c)
    _tick(c, 1.4)
    peak = _peak_db(c, a)
    c2 = _restart(c, tmp_path)

    _tick(c2, 1.6)                      # dopo il riavvio il picco persistito vale: la ritracciata scatta subito
    ev = _events(c2, trailing_stop.TRAILING_TRIGGERED)
    assert len(ev) == 1 and D(str(ev[0]["threshold"])) == trailing_stop.threshold(peak, D("20"))
    assert len(c2.bus.payloads("REQ_EXECUTE_CASHOUT")) == 1


def test_kill_fra_picco_e_persistenza_il_picco_non_cresce(tmp_path, monkeypatch) -> None:
    c = _chain(tmp_path)
    a, _b = _two_tables(c)
    _tick(c, 1.6)
    persisted = _peak_db(c, a)
    real = c.db.save_settings

    def kill(data):
        if any(k.startswith(trailing_stop.PEAK_KEY_PREFIX) for k in data):
            raise OSError("processo caduto prima della scrittura")
        return real(data)

    monkeypatch.setattr(c.db, "save_settings", kill)
    _tick(c, 1.4)                       # il picco salirebbe: scrittura fallita => non usato
    monkeypatch.undo()
    assert D(str(_live(c, a).meta.get("trailing_peak"))) == persisted
    c2 = _restart(c, tmp_path)
    tm = _live(c2, a)
    _tick(c2, 1.62)                     # profitto sotto il picco persistito: nessun nuovo picco
    assert D(str(tm.meta.get("trailing_peak"))) == persisted, "al riavvio il picco non e' l'ultimo persistito"
    assert _peak_db(c2, a) == persisted


@pytest.mark.parametrize("status", ["SUSPENDED", "CLOSED"])
def test_mercato_sospeso_nessuna_decisione_ne_picco(tmp_path, status) -> None:
    c = _chain(tmp_path)
    a, _b = _two_tables(c)
    _tick(c, 1.6)
    peak = _peak_db(c, a)
    _tick(c, 1.3, status)               # profitto apparente su mercato non negoziabile: ignorato
    _tick(c, 1.9, status)               # ritracciata su mercato non negoziabile: nessuno scatto
    assert _peak_db(c, a) == peak and _events(c, trailing_stop.TRAILING_TRIGGERED) == []


def test_quote_assenti_nessuna_decisione(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b = _two_tables(c)
    _tick(c, 1.4)
    peak = _peak_db(c, a)
    book = dict(c.broker.get_market_book(MKT), marketId=MKT)
    book["runners"] = [dict(r, ex={"availableToBack": [], "availableToLay": []}) for r in book["runners"]]
    c.rc._monitor_sl_tp_trailing(book)
    assert _peak_db(c, a) == peak and _events(c, trailing_stop.TRAILING_TRIGGERED) == []


def test_precedenza_sl_prima_del_trailing(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b = _two_tables(c, stop_loss_pct=10)
    _tick(c, 1.4)                       # picco
    _tick(c, 3.0)                       # perdita oltre lo SL e sotto la soglia del trailing: vince lo SL
    assert len(_events(c, "SL_TRIGGERED")) == 1 and _events(c, trailing_stop.TRAILING_TRIGGERED) == []
    assert len(c.bus.payloads("REQ_EXECUTE_CASHOUT")) == 1


def test_salita_scatta_il_tp_mai_il_trailing(tmp_path) -> None:
    """Sul tick che alza il picco il trailing non scatta: con TP 5 % (0,20) raggiunto scatta il TP."""
    c = _chain(tmp_path)
    _two_tables(c, take_profit_pct=5)
    _tick(c, 1.25)                      # P&L garantito 2,29 >= 0,20
    first = [t for t, _ in c.bus.events if t in ("TP_TRIGGERED", trailing_stop.TRAILING_TRIGGERED)]
    assert first == ["TP_TRIGGERED"], first


def test_precedenza_trailing_prima_del_tp(tmp_path) -> None:
    """Ritracciata in cui valgono sia trailing (1,63 <= 2,29 x 0,8 = 1,83) sia TP (1,63 >= 0,20): vince il trailing."""
    c = _chain(tmp_path)
    a, _b = _two_tables(c)
    _tick(c, 1.25)                      # picco 2,29, nessun TP armato
    _live(c, a).meta["take_profit"] = 0.20
    _tick(c, 1.4)
    events = [t for t, _ in c.bus.events if t in ("TP_TRIGGERED", trailing_stop.TRAILING_TRIGGERED)]
    assert events == [trailing_stop.TRAILING_TRIGGERED], events


def test_trailing_off_di_default(tmp_path) -> None:
    c = _chain(tmp_path)
    _book(c.broker)
    _enter(c, SEL_A, "2026-11-05T11:00:00.000001+00:00")
    _tick(c, 1.4)
    _tick(c, 1.9)
    t = _live(c, c.rc.table_manager.active_tables()[0])
    assert _events(c, trailing_stop.TRAILING_TRIGGERED) == [] and "trailing_peak" not in t.meta
    assert c.db.get_settings().get(trailing_stop.PEAK_KEY_PREFIX + t.meta["customer_ref"]) is None


def test_stato_persistito_corrotto_nessuna_decisione(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b = _two_tables(c)
    c.db.save_settings({trailing_stop.PEAK_KEY_PREFIX + a.meta["customer_ref"]: "{rotto"})
    _tick(c, 1.4)
    _tick(c, 1.9)
    assert _events(c, trailing_stop.TRAILING_TRIGGERED) == []
    assert c.db.get_settings()[trailing_stop.PEAK_KEY_PREFIX + a.meta["customer_ref"]] == "{rotto", "stato sovrascritto"


def test_evento_trailing_consumato_da_f04_con_contratto_v1() -> None:
    assert sl_tp_close.TRIGGER_REASONS[trailing_stop.TRAILING_TRIGGERED] == trailing_stop.TRAILING_REASON


def test_crash_dopo_lo_scatto_nessun_hedge_duplicato(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b = _two_tables(c)
    _tick(c, 1.4)
    _tick(c, 1.6)                       # scatta, hedge instradato
    assert len(c.bus.payloads("REQ_EXECUTE_CASHOUT")) == 1
    c2 = _restart(c, tmp_path)          # latch in memoria perso: lo scatto si ripete
    _tick(c2, 1.65)

    assert len(_events(c2, trailing_stop.TRAILING_TRIGGERED)) == 1
    assert c2.bus.payloads("REQ_EXECUTE_CASHOUT") == [], "secondo hedge dopo il riavvio"
    skipped = [p for t, p in c2.bus.events if t == sl_tp_close.SL_TP_CLOSE_SKIPPED]
    assert skipped and skipped[-1]["detail"] == "ALREADY_ROUTED"


def test_poller_sim_include_i_tavoli_con_solo_trailing(tmp_path) -> None:
    c = _chain(tmp_path)
    _two_tables(c)
    assert c.rc._sl_tp_poll_markets() == [MKT]


def test_book_che_non_copre_la_posizione_non_alza_il_picco(tmp_path) -> None:
    """Nessun profitto inventato: 1 EUR al prezzo di chiusura non copre l'hedge (~3,2): il picco resta."""
    c = _chain(tmp_path)
    a, _b = _two_tables(c)
    _tick(c, 1.6)
    peak = _peak_db(c, a)
    _book(c.broker, lay=(1.25, 1.0))
    c.rc._monitor_sl_tp_trailing(dict(c.broker.get_market_book(MKT), marketId=MKT))
    assert _peak_db(c, a) == peak and _events(c, trailing_stop.TRAILING_TRIGGERED) == []
