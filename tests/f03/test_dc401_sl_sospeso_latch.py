"""D-C4-01 (#586, G7-S24): SL/TP superato a mercato SOSPESO, poi riapertura.

Difetto (campagna 4, B3): il monitor valutava SL/TP anche su un book non negoziabile, impostava il latch
``sl_tp_triggered``; F-04 esitava NOT_EXECUTED (ritentabile) ma il latch impediva ogni nuovo scatto: alla riapertura,
con la perdita ancora oltre lo SL, la posizione restava aperta senza protezione.

Contratto (Claude B, pfab-b-20261105-g7-run-defects-ack-001):
(a) nessuna decisione SL/TP su book non negoziabile (``_market_book_tradable``, PF-EXEC-05): niente latch, niente ordine;
(b) il latch si rilascia SOLO su NOT_EXECUTED PROVATO: nessun ordine inviato (gate prima del router) o ogni cancel
    inviato dal router confermato dall'exchange. AMBIGUOUS/UNCERTAIN, cancel non confermato, conteggi assenti => latch
    trattenuto. Catena reale (harness F-04), quote SINTETICHE.
"""
from __future__ import annotations

import pytest

from core import sl_tp_close
from tests.acceptance.test_f04_sl_tp_table_close import _book, _chain, _three_tables, _trigger
from tests.f03.test_f03_trailing_stop import _events, _live, _tick, _two_tables

LATCH = "sl_tp_triggered"


# ------------------------------------------------------------------ (a) nessuna decisione su book non negoziabile
@pytest.mark.parametrize("status", ["SUSPENDED", "CLOSED"])
def test_sl_oltre_soglia_su_mercato_non_negoziabile_nessuna_decisione(tmp_path, status) -> None:
    c = _chain(tmp_path)
    a, _b = _two_tables(c, stop_loss_pct=10)
    _tick(c, 3.0, status)                            # perdita oltre lo SL, mercato non negoziabile
    assert _events(c, "SL_TRIGGERED") == [] and LATCH not in _live(c, a).meta
    assert c.bus.payloads("REQ_EXECUTE_CASHOUT") == []


def test_tp_su_mercato_sospeso_nessuna_decisione(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b = _two_tables(c, take_profit_pct=10)
    _tick(c, 1.4, "SUSPENDED")
    assert _events(c, "TP_TRIGGERED") == [] and LATCH not in _live(c, a).meta


def test_sl_superato_a_mercato_sospeso_chiude_una_volta_alla_riapertura(tmp_path) -> None:
    c = _chain(tmp_path)
    a, b = _two_tables(c, stop_loss_pct=10)
    _tick(c, 3.0, "SUSPENDED")
    _tick(c, 3.0, "SUSPENDED")
    assert c.bus.payloads("REQ_EXECUTE_CASHOUT") == []
    _tick(c, 3.0)                                    # riapre, perdita ancora oltre lo SL
    _tick(c, 3.0)
    assert len(_events(c, "SL_TRIGGERED")) == 1
    reqs = c.bus.payloads("REQ_EXECUTE_CASHOUT")
    assert len(reqs) == 1 and str(reqs[0]["selection_id"]) == str(a.selection_id)
    assert _live(c, a).meta.get(LATCH) == "STOP_LOSS_REACHED"      # instradato: latch trattenuto


# ------------------------------------------------------------------ (b) latch rilasciato solo su NOT_EXECUTED provato
def test_gate_rifiuta_latch_rilasciato_e_nuovo_scatto_chiude(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b = _two_tables(c, stop_loss_pct=10)
    c.rc._emergency_stopped = True
    _tick(c, 3.0)
    out = [p for t, p in c.bus.events if t == sl_tp_close.SL_TP_CLOSE_NOT_EXECUTED]
    assert len(out) == 1 and out[0]["detail"].startswith("GATE_REJECTED:")
    assert LATCH not in _live(c, a).meta, "NOT_EXECUTED provato (nessun ordine): il latch va rilasciato"
    c.rc._emergency_stopped = False
    _tick(c, 3.0)
    # nuovo tentativo senza ripubblicare lo scatto (dedup per tavolo+ragione): un solo SL_TRIGGERED, una chiusura
    assert len(_events(c, "SL_TRIGGERED")) == 1 and len(c.bus.payloads("REQ_EXECUTE_CASHOUT")) == 1
    assert [t for t, _p in c.bus.events if t == sl_tp_close.SL_TP_CLOSE_ROUTED] != []


def test_profondita_insufficiente_senza_cancel_latch_rilasciato(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b = _two_tables(c, stop_loss_pct=10)
    _book(c.broker, lay=(3.0, 1.0))                  # perdita oltre lo SL, profondita' insufficiente per l'hedge
    c.rc._monitor_sl_tp_trailing(dict(c.broker.get_market_book("1.234"), marketId="1.234"))
    out = [p for t, p in c.bus.events if t == sl_tp_close.SL_TP_CLOSE_NOT_EXECUTED]
    assert len(out) == 1 and LATCH not in _live(c, a).meta
    _tick(c, 3.0)                                    # profondita' tornata
    assert len(c.bus.payloads("REQ_EXECUTE_CASHOUT")) == 1


def _arm(c, table):
    _live(c, table).meta[LATCH] = "STOP_LOSS_REACHED"


def test_block_cancel_non_confermato_latch_trattenuto(tmp_path, monkeypatch) -> None:
    """Tavolo C solo a riposo: la chiusura cancella il resting. Cancel NON confermato => NOT_EXECUTED ma non provato."""
    c = _chain(tmp_path)
    _a, _b, tc = _three_tables(c)
    _arm(c, tc)
    monkeypatch.setattr(c.broker, "cancel_orders", lambda *a, **k: {"status": "FAILURE", "instructionReports": []})
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(tc))
    out = [p for t, p in c.bus.events if t == sl_tp_close.SL_TP_CLOSE_NOT_EXECUTED]
    assert len(out) == 1
    assert _live(c, tc).meta.get(LATCH) == "STOP_LOSS_REACHED", "cancel non confermato: latch da trattenere"


def test_cancel_confermato_latch_rilasciato(tmp_path) -> None:
    c = _chain(tmp_path)
    _a, _b, tc = _three_tables(c)
    _arm(c, tc)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(tc))
    assert [t for t, _p in c.bus.events if t.startswith("SL_TP_CLOSE_")][-1] == sl_tp_close.SL_TP_CLOSE_NOT_EXECUTED
    assert LATCH not in _live(c, tc).meta


def test_block_esito_incerto_latch_trattenuto(tmp_path, monkeypatch) -> None:
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    _arm(c, a)

    def boom(*_a, **_k):
        raise RuntimeError("risposta persa")
    monkeypatch.setattr(c.rc, "_on_signal_received", boom)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
    assert [t for t, _p in c.bus.events if t.startswith("SL_TP_CLOSE_")][-1] == sl_tp_close.SL_TP_CLOSE_UNCERTAIN
    assert _live(c, a).meta.get(LATCH) == "STOP_LOSS_REACHED"


def test_block_stato_finale_non_persistito_latch_trattenuto(tmp_path, monkeypatch) -> None:
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    _book(c.broker, "SUSPENDED")                     # il router non chiude: NOT_EXECUTED
    _arm(c, a)
    real = c.db.save_settings

    def save(values):
        if any(k.startswith(sl_tp_close.CLOSE_KEY_PREFIX) for k in values):
            raise OSError("disco pieno")
        return real(values)
    monkeypatch.setattr(c.db, "save_settings", save)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
    assert _live(c, a).meta.get(LATCH) == "STOP_LOSS_REACHED", "stato non persistito (resta CLAIMED): latch trattenuto"


def test_block_latch_di_un_altra_posizione_non_toccato(tmp_path) -> None:
    """Il rilascio vale solo per il tavolo, il customer_ref e la ragione dello scatto."""
    c = _chain(tmp_path)
    a, _b, _c = _three_tables(c)
    _live(c, a).meta[LATCH] = "TAKE_PROFIT_REACHED"   # latch di un'altra ragione
    c.rc._emergency_stopped = True
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
    assert _live(c, a).meta.get(LATCH) == "TAKE_PROFIT_REACHED"


# ------------------------------------------------------------------ prova di NOT_EXECUTED (logica pura)
@pytest.mark.parametrize("box,atteso", [
    ({"rejected": "emergency_stop_active"}, True),
    ({"result": {"published": 0, "cancels_sent": 0, "cancels_unconfirmed": 0}}, True),
    ({"result": {"published": 0, "cancels_sent": 2, "cancels_unconfirmed": 0}}, True),
    ({"result": {"published": 0, "cancels_sent": 1, "cancels_unconfirmed": 1}}, False),
    ({"result": {"published": 0}}, False),                                              # conteggi assenti
    ({"result": {"published": 0, "cancels_sent": "0", "cancels_unconfirmed": 0}}, False),
    ({"result": {"published": 0, "cancels_sent": 0, "cancels_unconfirmed": False}}, False),
    ({"result": {"published": 0, "cancels_sent": 0, "cancels_unconfirmed": -1}}, False),
    ({"result": {"published": 0, "cancels_sent": 1, "cancels_unconfirmed": 2}}, False),
    ({"result": {"published": 1, "cancels_sent": 0, "cancels_unconfirmed": 0}}, False),   # ROUTED
    ({"error": "timeout"}, False),                                                      # UNCERTAIN
    ({"rejected": "x", "error": "y"}, False),                                           # errore: mai provato
    ({}, False),
])
def test_latch_rilasciabile_solo_su_not_executed_provato(box, atteso) -> None:
    assert sl_tp_close.latch_releasable(box) is atteso


# ------------------------------------------------------------------ S24 esteso: direttiva owner PF-G7-CAMPAIGN-04-FIX
from tests.f03.test_f03_trailing_stop import _restart  # noqa: E402

MKT = "1.234"


def _hedges(c, table):
    """Hedge sull'exchange per la selezione del tavolo (lato opposto all'ingresso BACK): la prova di nessun duplicato."""
    return [o for o in c.broker.get_current_orders(None)
            if str(o.get("selectionId")) == str(table.selection_id) and str(o.get("side")).upper() == "LAY"]


def test_sospensione_prolungata_nessun_evento_poi_una_sola_chiusura(tmp_path) -> None:
    c = _chain(tmp_path)
    a, b = _two_tables(c, stop_loss_pct=10)
    for _ in range(10):
        _tick(c, 3.0, "SUSPENDED")
    assert _events(c, "SL_TRIGGERED") == [] and _hedges(c, a) == []
    for _ in range(5):                               # riapre: la perdita resta oltre lo SL per molti giri
        _tick(c, 3.0)
    assert len(_events(c, "SL_TRIGGERED")) == 1 and len(_hedges(c, a)) == 1
    _tick(c, 3.0, "SUSPENDED")                       # nuova sospensione e riapertura dopo la chiusura: nulla
    _tick(c, 3.0)
    assert len(_hedges(c, a)) == 1 and _hedges(c, b) == []


def test_sospensione_fra_scatto_e_router_poi_riavvio_una_sola_chiusura(tmp_path) -> None:
    """Corsa: il monitor vede un book OPEN, il router (book riletto) lo trova SOSPESO => NOT_EXECUTED senza ordini,
    latch rilasciato. Il processo cade; dopo il riavvio, a mercato riaperto, una sola chiusura."""
    c = _chain(tmp_path)
    a, _b = _two_tables(c, stop_loss_pct=10)
    _book(c.broker, lay=(3.0, 500.0))
    open_book = dict(c.broker.get_market_book(MKT), marketId=MKT)
    _book(c.broker, "SUSPENDED", lay=(3.0, 500.0))
    c.rc._monitor_sl_tp_trailing(open_book)
    out = [p for t, p in c.bus.events if t == sl_tp_close.SL_TP_CLOSE_NOT_EXECUTED]
    assert len(out) == 1 and _hedges(c, a) == [] and LATCH not in _live(c, a).meta
    c2 = _restart(c, tmp_path)
    _tick(c2, 3.0, "SUSPENDED")
    assert _hedges(c2, a) == []
    _tick(c2, 3.0)
    _tick(c2, 3.0)
    assert len(_hedges(c2, a)) == 1, "dopo il riavvio la posizione va chiusa una sola volta"


def test_riavvio_durante_la_sospensione_nessuna_rivendicazione_fantasma(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b = _two_tables(c, stop_loss_pct=10)
    _tick(c, 3.0, "SUSPENDED")
    raw = c.db.get_settings().get(sl_tp_close.CLOSE_KEY_PREFIX + a.meta["customer_ref"])
    assert raw is None, "nessuna chiusura rivendicata a mercato sospeso"
    c2 = _restart(c, tmp_path)
    _tick(c2, 3.0)
    assert len(_hedges(c2, a)) == 1


def test_riavvio_dopo_la_chiusura_instradata_nessun_secondo_hedge(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b = _two_tables(c, stop_loss_pct=10)
    _tick(c, 3.0, "SUSPENDED")
    _tick(c, 3.0)
    assert len(_hedges(c, a)) == 1
    c2 = _restart(c, tmp_path)
    _tick(c2, 3.0)
    _tick(c2, 3.0, "SUSPENDED")
    _tick(c2, 3.0)
    assert len(_hedges(c2, a)) == 1
    assert [t for t, _p in c2.bus.events if t.startswith("SL_TP_CLOSE_")] == [sl_tp_close.SL_TP_CLOSE_SKIPPED]


def test_ingresso_parziale_sl_a_mercato_sospeso_chiude_solo_l_abbinato(tmp_path) -> None:
    """Ingresso BACK 4 abbinato solo per 3 (resto a riposo; hedge 2,00 sopra il minimo exchange). SL superato a mercato sospeso: nulla; alla riapertura il
    resting si cancella (confermato) e l'hedge copre solo l'abbinato, una volta."""
    from tests.f03.test_f03_trailing_stop import _enter
    c = _chain(tmp_path)
    _book(c.broker, back=(2.0, 3.0))
    _enter(c, 11, "2026-11-05T10:00:00.000001+00:00", stop_loss_pct=10)
    (a,) = c.rc.table_manager.active_tables()
    entry = [o for o in c.broker.get_current_orders(None) if str(o.get("side")).upper() == "BACK"]
    assert len(entry) == 1 and float(entry[0]["sizeMatched"]) == 3.0, entry
    def tick(status="OPEN"):                         # back sotto 2.0: il resting dell'ingresso resta non abbinato
        _book(c.broker, status, back=(1.9, 500.0), lay=(3.0, 500.0))
        c.rc._monitor_sl_tp_trailing(dict(c.broker.get_market_book(MKT), marketId=MKT))
    tick("SUSPENDED")
    assert _events(c, "SL_TRIGGERED") == [] and _hedges(c, a) == []
    tick()
    tick()
    hedges = _hedges(c, a)
    assert len(hedges) == 1
    assert float(hedges[0]["sizeMatched"]) + float(hedges[0].get("sizeRemaining") or 0) == 2.0, hedges   # 3*2/3
    left = [o for o in c.broker.get_current_orders(None) if str(o.get("side")).upper() == "BACK"]
    assert all(float(o.get("sizeRemaining") or 0) == 0 for o in left), "resting dell'ingresso non cancellato"


# ------------------------------------------------------------------ microstake: hedge sotto il minimo exchange
def test_hedge_sotto_il_minimo_exchange_senza_executor_microstake_fallisce_esplicito(tmp_path) -> None:
    """Direttiva owner PF-D-C4-01-MICROSTAKE-GUARD. Ingresso BACK 4 abbinato solo per 1: hedge 0,67, sotto il minimo
    exchange (`EXCHANGE_MIN_ORDER`), passa dal Microstake nativo. QUESTO harness (F-04 acceptance) non cabla il
    Microstake executor: qui si verifica SOLO la proprieta' fail-closed (errore esplicito, nessun ordine d'importo
    diverso, nessun hedge parziale). L'E2E del percorso produttivo con Microstake cablato e' G7-S26
    (`tests/f026/test_roserpina_100_campagna4.py::test_b5_...`, HeadlessApp reale): in questo harness e' NOT_TESTED.
    Nessuna soglia nel test: la quota da 0,67 nasce dall'abbinamento parziale."""
    from core.stake_precision import EXCHANGE_MIN_ORDER
    from tests.f03.test_f03_trailing_stop import _enter
    c = _chain(tmp_path)
    _book(c.broker, back=(2.0, 1.0))
    _enter(c, 11, "2026-11-05T10:00:00.000001+00:00", stop_loss_pct=10)
    (a,) = c.rc.table_manager.active_tables()
    _book(c.broker, back=(1.9, 500.0), lay=(3.0, 500.0))
    c.rc._monitor_sl_tp_trailing(dict(c.broker.get_market_book(MKT), marketId=MKT))
    reqs = c.bus.payloads("REQ_EXECUTE_CASHOUT")
    assert len(reqs) == 1 and 0 < float(reqs[0]["stake"]) < float(EXCHANGE_MIN_ORDER), reqs
    failed = c.bus.payloads("CASHOUT_FAILED")
    assert [f.get("reason") for f in failed] == ["MICROSTAKE_EXECUTOR_UNAVAILABLE"], failed
    assert _hedges(c, a) == [], "senza executor microstake nessun ordine (mai un importo diverso)"


# ------------------------------------------------------------------ dedup degli eventi (condizione Claude B)
_NOISE = ("SL_TRIGGERED", "TP_TRIGGERED", "TRAILING_TRIGGERED", "SIGNAL_REJECTED", "CASHOUT_FAILED",
          sl_tp_close.SL_TP_CLOSE_NOT_EXECUTED, sl_tp_close.SL_TP_CLOSE_REJECTED, sl_tp_close.SL_TP_CLOSE_SKIPPED,
          sl_tp_close.SL_TP_CLOSE_UNCERTAIN)


def _noise(c):
    return [t for t, _p in c.bus.events if t in _NOISE]


def test_gate_che_rifiuta_per_n_giri_nessun_evento_ripetuto_poi_chiude(tmp_path) -> None:
    """pfab-b-20261105-dc402-design-reply-001: rilasciato il latch, finche' il gate rifiuta i giri del poller non
    ripetono eventi/allarmi (dedup per tavolo+ragione finche' lo stato non cambia); la rivalutazione continua e chiude
    appena il gate lo consente."""
    c = _chain(tmp_path)
    a, _b = _two_tables(c, stop_loss_pct=10)
    c.rc._emergency_stopped = True
    _tick(c, 3.0)
    first = _noise(c)
    assert first.count("SL_TRIGGERED") == 1 and first.count(sl_tp_close.SL_TP_CLOSE_NOT_EXECUTED) == 1
    for _ in range(10):
        _tick(c, 3.0)
    assert _noise(c) == first, "eventi ripetuti a ogni giro mentre il gate rifiuta"
    assert c.bus.payloads("REQ_EXECUTE_CASHOUT") == []
    c.rc._emergency_stopped = False
    _tick(c, 3.0)
    assert len(c.bus.payloads("REQ_EXECUTE_CASHOUT")) == 1 and len(_hedges(c, a)) == 1
    assert [t for t, _p in c.bus.events if t == sl_tp_close.SL_TP_CLOSE_ROUTED] != []


def test_profondita_insufficiente_per_n_giri_nessun_evento_ripetuto(tmp_path) -> None:
    c = _chain(tmp_path)
    a, _b = _two_tables(c, stop_loss_pct=10)

    def thin():
        _book(c.broker, lay=(3.0, 1.0))
        c.rc._monitor_sl_tp_trailing(dict(c.broker.get_market_book(MKT), marketId=MKT))
    thin()
    first = _noise(c)
    for _ in range(10):
        thin()
    assert _noise(c) == first
    _tick(c, 3.0)
    assert len(_hedges(c, a)) == 1


def test_cambio_di_motivo_produce_un_nuovo_esito(tmp_path) -> None:
    """Dedup solo finche' lo stato non cambia: un motivo diverso di NOT_EXECUTED e' un nuovo esito pubblicato."""
    c = _chain(tmp_path)
    _a, _b = _two_tables(c, stop_loss_pct=10)
    c.rc._emergency_stopped = True
    _tick(c, 3.0)
    _tick(c, 3.0)
    c.rc._emergency_stopped = False
    _book(c.broker, lay=(3.0, 1.0))                  # ora il motivo e' la profondita'
    c.rc._monitor_sl_tp_trailing(dict(c.broker.get_market_book(MKT), marketId=MKT))
    out = [p["detail"] for t, p in c.bus.events if t == sl_tp_close.SL_TP_CLOSE_NOT_EXECUTED]
    assert len(out) == 2 and out[0].startswith("GATE_REJECTED:") and not out[1].startswith("GATE_REJECTED:"), out


def test_block_segnale_esterno_con_sl_tp_quiet_non_silenzia_il_rifiuto(tmp_path) -> None:
    """Il silenzio vale solo per i tentativi INTERNI (token d'instradamento registrato): un segnale esterno che porta
    `sl_tp_quiet` non nasconde il proprio SIGNAL_REJECTED."""
    c = _chain(tmp_path)
    c.rc._reject_signal({"signal_type": "CASHOUT_TABLE", "sl_tp_quiet": True, "sl_tp_route_token": "falso"}, "x")
    assert [p["reason"] for t, p in c.bus.events if t == "SIGNAL_REJECTED"] == ["x"]
