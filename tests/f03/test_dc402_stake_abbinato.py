"""D-C4-02 (#586, G7-S25): SL/TP/trailing sulla posizione ABBINATA, non sullo stake richiesto.

Prima: `sl_tp_pnl.position_stake` usava `meta['stake']` (richiesto): un LAY mai abbinato faceva scattare il trailing su
un profitto inesistente e la chiusura F-04 cancellava l'ingresso in attesa.

Contratto (Claude B, pfab-b-20261105-dc402-design-reply-001):
- lettura `listCurrentOrders` sullo STESSO percorso del cashout router (`betfair_service.list_current_orders`), una per
  giro del poller per tutti i mercati con protezione armata, limitata da `AutoThrottle`; nessun nuovo client;
- stake = somma `sizeMatched` dei bet_id del TAVOLO (per identita'), ingresso = prezzo medio abbinato confermato;
- abbinato 0 => nessuna decisione; parziale => solo la parte abbinata (il residuo resta al TTL);
- lettura illeggibile, malformata, bet_id del tavolo assente, prezzo medio mancante => nessuna decisione, nessun latch.
Catena reale (harness F-04), quote SINTETICHE.
"""
from __future__ import annotations

from decimal import Decimal as D

import pytest

from core import sl_tp_pnl, trailing_stop
from tests.acceptance.test_f04_sl_tp_table_close import _book, _chain
from tests.f03.test_f03_trailing_stop import _enter, _events, _live

MKT = "1.234"
LATCH = "sl_tp_triggered"


def _lay_entry(c, price=2.0, **extra):
    c.rc._on_signal_received({"market_id": MKT, "selection_id": 11, "price": price, "bet_type": "LAY",
                              "simulation_mode": True, "chat_id": -100123,
                              "received_at": "2026-11-08T10:00:00.000001+00:00",
                              "event": "Alpha v Beta", "market": "Match Odds", "selection": "S11", **extra})
    (t,) = c.rc.table_manager.active_tables()
    return t


def _feed(c, back, lay, status="OPEN"):
    _book(c.broker, status, back=(back, 500.0), lay=(lay, 500.0))
    c.rc._monitor_sl_tp_trailing(dict(c.broker.get_market_book(MKT), marketId=MKT))


def _poll(c, back, lay, status="OPEN"):
    _book(c.broker, status, back=(back, 500.0), lay=(lay, 500.0))
    c.rc._sl_tp_poll_once()


# ------------------------------------------------------------------ logica pura
def test_posizione_abbinata_somma_solo_i_bet_del_tavolo_al_prezzo_medio() -> None:
    rows = [{"betId": "B1", "sizeMatched": 1.0, "averagePriceMatched": 2.0, "sizeRemaining": 3.0},
            {"betId": "B2", "sizeMatched": 3.0, "averagePriceMatched": 2.4, "sizeRemaining": 0.0},
            {"betId": "X9", "sizeMatched": 50.0, "averagePriceMatched": 9.0}]          # altro tavolo
    pos, why = sl_tp_pnl.matched_position(rows, ["B1", "B2"])
    assert why == "" and pos == (D("4.00"), D("2.30"))                                  # (1*2 + 3*2.4)/4


@pytest.mark.parametrize("rows,bets,motivo", [
    ([{"betId": "B1", "sizeMatched": 0.0, "averagePriceMatched": 0.0, "sizeRemaining": 4.0}], ["B1"], "NOT_MATCHED"),
    ([], ["B1"], "BET_NOT_ON_EXCHANGE"),
    ([{"betId": "B1", "sizeMatched": "x", "averagePriceMatched": 2.0}], ["B1"], "MATCHED_INVALID"),
    ([{"betId": "B1", "sizeMatched": True, "averagePriceMatched": 2.0}], ["B1"], "MATCHED_INVALID"),
    ([{"betId": "B1", "sizeMatched": -1.0, "averagePriceMatched": 2.0}], ["B1"], "MATCHED_INVALID"),
    ([{"betId": "B1", "sizeMatched": 1.0}], ["B1"], "AVG_PRICE_INVALID"),
    ([{"betId": "B1", "sizeMatched": 1.0, "averagePriceMatched": 1.0}], ["B1"], "AVG_PRICE_INVALID"),
    ([{"betId": "B1", "sizeMatched": 1.0, "averagePriceMatched": 2.0}], [], "NO_BET_IDS"),
    (None, ["B1"], "ORDERS_UNREADABLE"),
])
def test_block_posizione_non_dimostrabile_nessuna_decisione(rows, bets, motivo) -> None:
    pos, why = sl_tp_pnl.matched_position(rows, bets)
    assert pos is None and why.startswith(motivo), (pos, why)


# ------------------------------------------------------------------ S25: LAY mai abbinato
def test_trailing_su_lay_mai_abbinato_non_scatta_e_l_ingresso_resta(tmp_path) -> None:
    c = _chain(tmp_path)
    _book(c.broker)                                    # lay 2.02 > 2.0: il LAY a 2.0 resta non abbinato
    a = _lay_entry(c, trailing_stop_pct=20)
    for back, lay in ((3.0, 3.02), (3.4, 3.42), (2.4, 2.42)):
        _poll(c, back, lay)
    assert _events(c, trailing_stop.TRAILING_TRIGGERED) == [] and LATCH not in _live(c, a).meta
    assert _live(c, a).meta["sl_tp_wait_reason"] == "NOT_MATCHED"
    entry = [o for o in c.broker.get_current_orders(None) if o["side"] == "LAY"]
    assert len(entry) == 1 and entry[0]["status"] == "EXECUTABLE" and float(entry[0]["sizeMatched"]) == 0.0


def test_sl_su_back_mai_abbinato_non_scatta(tmp_path) -> None:
    c = _chain(tmp_path)
    _book(c.broker, back=(1.9, 500.0))                 # BACK a 2.0 non abbinato
    a = _enter(c, 11, "2026-11-08T10:00:00.000001+00:00", stop_loss_pct=10)
    (a,) = c.rc.table_manager.active_tables()
    _poll(c, 1.9, 3.0)
    assert _events(c, "SL_TRIGGERED") == [] and LATCH not in _live(c, a).meta


def test_abbinamento_successivo_attiva_la_protezione(tmp_path) -> None:
    """Il LAY si abbina DOPO l'ingresso (quota scesa): da li' la protezione decide sull'abbinato."""
    c = _chain(tmp_path)
    _book(c.broker)
    a = _lay_entry(c, stop_loss_pct=10)
    _poll(c, 1.88, 1.9)                                # abbinato a 1.9 (perdita minima, sotto lo SL)
    assert _events(c, "SL_TRIGGERED") == []
    _poll(c, 1.3, 1.32)                                # perdita del bancatore oltre lo SL: scatta
    ev = _events(c, "SL_TRIGGERED")
    assert len(ev) == 1 and ev[0]["table_id"] == a.table_id


# ------------------------------------------------------------------ parziale: solo la parte abbinata
def test_sl_parziale_decide_sulla_sola_parte_abbinata(tmp_path) -> None:
    """BACK 4 abbinato per 1 a 2.0, SL 10 %. Soglia = 10 % del rischio ABBINATO (1 EUR) = 0,10, non 10 % del richiesto
    (0,40, l'importo calcolato all'ingresso). Chiudere a 2.15: perdita ~0,07 (sotto); a 2.3: ~0,13 (oltre) => scatta.
    Con la soglia sul richiesto (0,40) a 2.3 non scatterebbe."""
    c = _chain(tmp_path)
    _book(c.broker, back=(2.0, 1.0))
    _enter(c, 11, "2026-11-08T10:00:00.000001+00:00", stop_loss_pct=10)
    (a,) = c.rc.table_manager.active_tables()
    assert _live(c, a).meta["stop_loss"] == 0.4                       # importo F-02 sul richiesto
    _poll(c, 1.9, 2.15)
    assert _events(c, "SL_TRIGGERED") == []
    _poll(c, 1.9, 2.3)
    ev = _events(c, "SL_TRIGGERED")
    assert len(ev) == 1 and -0.14 < ev[0]["pnl"] < -0.12, ev


# ------------------------------------------------------------------ lettura dell'exchange: fail-closed e costo
def test_block_lettura_ordini_illeggibile_nessuna_decisione_poi_decide(tmp_path, monkeypatch) -> None:
    c = _chain(tmp_path)
    _book(c.broker)
    _enter(c, 11, "2026-11-08T10:00:00.000001+00:00", stop_loss_pct=10)
    (a,) = c.rc.table_manager.active_tables()
    real = c.svc.list_current_orders

    def boom(market_ids=None):
        raise RuntimeError("CURRENT_ORDERS_MALFORMED: timeout")
    monkeypatch.setattr(c.svc, "list_current_orders", boom)
    _poll(c, 1.9, 3.0)
    assert _events(c, "SL_TRIGGERED") == [] and LATCH not in _live(c, a).meta
    assert _live(c, a).meta["sl_tp_wait_reason"] == "ORDERS_UNREADABLE"          # motivo, non "nessun ordine"
    monkeypatch.setattr(c.svc, "list_current_orders", real)
    c.rc._sl_tp_orders_cache = {}
    _poll(c, 1.9, 3.0)
    assert len(_events(c, "SL_TRIGGERED")) == 1 and "sl_tp_wait_reason" not in _live(c, a).meta


def test_block_bet_del_tavolo_assente_dalla_risposta_nessuna_decisione(tmp_path, monkeypatch) -> None:
    c = _chain(tmp_path)
    _book(c.broker)
    _enter(c, 11, "2026-11-08T10:00:00.000001+00:00", stop_loss_pct=10)
    monkeypatch.setattr(c.svc, "list_current_orders", lambda market_ids=None: [])     # risposta parziale/vuota
    _poll(c, 1.9, 3.0)
    assert _events(c, "SL_TRIGGERED") == []


def test_una_sola_lettura_per_giro_per_tutti_i_mercati(tmp_path, monkeypatch) -> None:
    c = _chain(tmp_path)
    _book(c.broker)
    _enter(c, 11, "2026-11-08T10:00:00.000001+00:00", stop_loss_pct=10)
    _enter(c, 12, "2026-11-08T10:00:01.000001+00:00", stop_loss_pct=10)
    calls = []
    real = c.svc.list_current_orders

    def spy(market_ids=None):
        calls.append(list(market_ids or []))
        return real(market_ids)
    monkeypatch.setattr(c.svc, "list_current_orders", spy)
    _poll(c, 1.9, 2.1)
    assert calls == [[MKT]], calls
    for _ in range(5):                                 # lo stream (book a raffica) entro la finestra non rilegge
        _feed(c, 1.9, 2.1)
    assert len(calls) == 1, calls


def test_nessuna_lettura_su_mercato_non_negoziabile(tmp_path, monkeypatch) -> None:
    c = _chain(tmp_path)
    _book(c.broker)
    _enter(c, 11, "2026-11-08T10:00:00.000001+00:00", stop_loss_pct=10)
    calls = []
    monkeypatch.setattr(c.svc, "list_current_orders", lambda market_ids=None: calls.append(1) or [])
    _feed(c, 1.9, 3.0, "SUSPENDED")
    assert calls == []
