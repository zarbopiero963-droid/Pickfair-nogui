"""PF-BLACKBOX-01 — oracolo di completezza finanziaria (Claude D), offline sul contratto §3/§4/§4-bis.

Per ogni scenario della campagna: registro completo => nessun problema (PASS); un anello tolto alla scrittura =>
l'oracolo lo segnala con il codice atteso (BLOCK). Il registro si legge solo via `query` (records_of).
"""
from __future__ import annotations

import threading

import pytest

from tests.blackbox_completeness import oracle
from tests.blackbox_completeness.fake_blackbox import FakeBlackBox, _now_iso as _now, records_of
from tests.blackbox_completeness.scenario import Scenario


def _check(sc: Scenario, secrets=()):
    return oracle.check(records_of(sc.bb), sc.exchange, sc.settlements, secrets)


def _campagna(skip=()):
    """Tre tavoli microstake, un ingresso parziale con TTL, SL / TP / trailing con hedge, settlement."""
    sc = Scenario(skip=set(skip))
    a = sc.microstake_entry("sig-a", "1.1", 0.5, 2.0, table_id=1)
    b = sc.microstake_entry("sig-b", "1.2", 0.3, 3.0, table_id=2)
    c = sc.microstake_entry("sig-c", "1.3", 0.8, 1.8, table_id=3)
    p = sc.place("sig-p", "1.4", "BACK", 2.0, 4.0, matched=1.0, table_id=4)            # parziale
    sc.cancel(p)                                                                     # residuo al TTL
    sc.protection_close(a, "SL_TRIGGERED", 0.33, 3.0)
    sc.protection_close(b, "TP_TRIGGERED", 0.45, 2.0)
    sc.protection_close(c, "TRAILING_TRIGGERED", 0.9, 1.6)
    for m, pnl in (("1.1", -0.17), ("1.2", 0.14), ("1.3", 0.07), ("1.4", 0.95)):
        sc.settle(m, pnl)
    return sc


def test_campagna_completa_nessun_anello_mancante() -> None:
    assert _check(_campagna()) == []


@pytest.mark.parametrize("step,codice", [
    ("request", "MISSING_REQUEST"),
    ("response", "MISSING_RESULT"),
    ("matched", "MATCH_MISSING"),
    ("cashout_request", "HEDGE_WITHOUT_CAUSE"),
    ("cashout_result", "CASHOUT_WITHOUT_RESULT"),
    ("close_outcome", "TRIGGER_WITHOUT_CLOSE_OUTCOME"),
    ("settlement", "SETTLEMENT_MISSING"),
])
def test_block_anello_mancante_segnalato(step, codice) -> None:
    problems = _check(_campagna(skip={step}))
    assert any(p.startswith(codice) for p in problems), problems


def test_block_abbinamento_registrato_diverso_dall_exchange() -> None:
    sc = _campagna()
    sc.exchange[-1]["sizeMatched"] = 0.5            # l'exchange dice altro rispetto all'ultimo ORDER_MATCHED
    assert any(p.startswith("MATCH_MISMATCH") for p in _check(sc))


def test_block_pnl_registrato_diverso_dal_settlement() -> None:
    sc = _campagna()
    sc.settlements[0]["net_pnl"] = -0.18
    assert "SETTLEMENT_MISMATCH:1.1" in _check(sc)


def test_block_conferma_senza_ordine_sull_exchange() -> None:
    sc = _campagna()
    sc.exchange = [o for o in sc.exchange if o["customerOrderRef"] != "sig-p"]
    assert any(p.startswith("PHANTOM_CONFIRMATION") for p in _check(sc))


# ------------------------------------------------------------------ crash durante l'invio
def test_crash_durante_l_invio_richiesta_presente_esito_dalla_riconciliazione() -> None:
    sc = Scenario()
    sc.crash_in_send("sig-x", "1.9", 4.0, 4.0)
    recs = records_of(sc.bb)
    assert [r["type"] for r in recs][:2] == ["ORDER_PLACE_REQUEST", "UNCLEAN_TERMINATION"]
    assert _check(sc) == []


def test_block_crash_senza_riconciliazione_registrata() -> None:
    sc = Scenario(skip={"reconciliation"})
    sc.crash_in_send("sig-x", "1.9", 4.0, 4.0)
    problems = _check(sc)
    assert "MISSING_RESULT:sig-x" in problems and any(p.startswith("MISSING_RESULT_FOR_BET") for p in problems)


def test_block_esito_ignoto_senza_riconciliazione() -> None:
    sc = Scenario()
    sc.place("sig-u", "1.8", "BACK", 2.0, 4.0, outcome="UNKNOWN")
    assert "MISSING_RECONCILIATION:sig-u" in _check(sc)


# ------------------------------------------------------------------ mercato sospeso e riaperto (D-C4-01)
def test_mercato_sospeso_protezione_in_attesa_poi_chiusura() -> None:
    sc = Scenario()
    e = sc.place("sig-s", "1.7", "BACK", 2.0, 4.0, matched=4.0)
    refs = {k: e[k] for k in ("table_id", "market_id", "selection_id", "order_ref")}
    sc.bb.record("MARKET_STATE_CHANGE", source="EXCHANGE", state_before="OPEN", state_after="SUSPENDED", **refs)
    sc.bb.record("PROTECTION_WAITING", source="STRATEGY", reason_code="MARKET_NOT_TRADABLE", **refs)
    sc.bb.record("MARKET_STATE_CHANGE", source="EXCHANGE", state_before="SUSPENDED", state_after="OPEN", **refs)
    sc.bb.record("PROTECTION_RESUMED", source="STRATEGY", **refs)
    sc.protection_close(e, "SL_TRIGGERED", 2.6, 3.0)
    recs = records_of(sc.bb)
    types = [r["type"] for r in recs]
    assert types.index("PROTECTION_WAITING") < types.index("SL_TRIGGERED")
    assert all(r["type"] != "ORDER_PLACE_REQUEST" or (r.get("data") or {}).get("purpose") != "HEDGE"
               for r in recs[:types.index("PROTECTION_RESUMED")]), "hedge registrato a mercato sospeso"
    assert _check(sc) == []


# ------------------------------------------------------------------ concorrenza multitavolo
def test_concorrenza_multitavolo_seq_senza_buchi_ne_duplicati() -> None:
    bb = FakeBlackBox()

    def worker(t):
        sc = Scenario(bb=bb)
        for i in range(25):
            sc.place(f"sig-{t}-{i}", f"1.{t}", "BACK", 2.0, 1.0, matched=1.0, table_id=t)
    threads = [threading.Thread(target=worker, args=(t,)) for t in range(1, 9)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    recs = records_of(bb)
    assert len(recs) == 8 * 25 * 3
    assert [p for p in oracle.check(recs) if p.startswith(("SEQ_", "CHAIN_"))] == []


def test_block_seq_duplicato_o_buco_rilevato() -> None:
    sc = _campagna()
    recs = records_of(sc.bb)
    assert "SEQ_GAP" in oracle.check(recs[:5] + recs[6:], sc.exchange, sc.settlements)
    dup = recs[:5] + [dict(recs[4])] + recs[5:]
    assert "SEQ_DUPLICATE" in oracle.check(dup, sc.exchange, sc.settlements)


def test_block_manomissione_rompe_la_catena() -> None:
    sc = _campagna()
    recs = records_of(sc.bb)
    recs[3] = dict(recs[3], values_after={"size_matched": 999})
    assert any(p.startswith("CHAIN_BROKEN") for p in oracle.check(recs, sc.exchange, sc.settlements))


# ------------------------------------------------------------------ disco pieno (semantica BB-1, due casi soltanto)
def _outage_case1(gap=True):
    sc = Scenario()
    e = sc.place("sig-d", "1.6", "BACK", 2.0, 4.0, matched=4.0)
    sc.bb.disk_full = True
    assert sc.try_entry_while_unavailable("sig-new", "1.5") is False          # nessun nuovo ingresso, nessun invio
    assert all(o["customerOrderRef"] != "sig-new" for o in sc.exchange)
    hedge = sc.exit_during_outage(e, 2.6, 3.0)                                # uscita protettiva: nel buffer critico
    if gap:
        sc.bb.recover_disk()
    else:                                                                     # scaricato senza lacuna dichiarata
        sc.bb.disk_full = False
        gap_less = sc.bb.buffer
        sc.bb.buffer, sc.bb.degraded = [], False
        for ev in gap_less:
            refs = ev.pop("refs")
            sc.bb._write(ev.pop("type"), data=dict(ev.pop("data") or {}, buffered=True), **ev, **refs)
    return sc, hedge


def test_disco_pieno_caso1_buffer_recuperato_richiesta_bufferizzata_e_lacuna() -> None:
    sc, hedge = _outage_case1()
    recs = records_of(sc.bb)
    req = [r for r in recs if r["type"] == "ORDER_PLACE_REQUEST" and r["order_ref"] == hedge["order_ref"]]
    assert len(req) == 1 and req[0]["data"]["buffered"] is True
    gap = [r for r in recs if r["type"] == "BLACKBOX_GAP"]
    assert len(gap) == 1 and hedge["order_ref"] in gap[0]["data"]["refs"] and gap[0]["seq"] > req[0]["seq"]
    # ordine al ripristino confermato per BB-1: eventi bufferizzati, poi GAP, poi BLACKBOX_RECOVERED
    tail = [r["type"] for r in recs if (r.get("data") or {}).get("buffered") or r["type"] in (
        "BLACKBOX_GAP", "BLACKBOX_RECOVERED")]
    assert tail[-2:] == ["BLACKBOX_GAP", "BLACKBOX_RECOVERED"] and all(
        (r.get("data") or {}).get("buffered") for r in recs if r["seq"] < gap[0]["seq"] and r["seq"] > req[0]["seq"] - 1
        and r["type"] not in ("BLACKBOX_GAP", "BLACKBOX_RECOVERED") and r["seq"] >= req[0]["seq"])
    assert all(isinstance(g["data"]["from_ts"], str) and isinstance(g["data"]["to_ts"], str) for g in gap)
    assert _check(sc) == []


def test_block_caso1_richiesta_bufferizzata_senza_lacuna() -> None:
    sc, hedge = _outage_case1(gap=False)
    assert f"BUFFERED_WITHOUT_GAP:{hedge['order_ref']}" in _check(sc)


def test_block_ingresso_bufferizzato_non_ammesso() -> None:
    sc = Scenario()
    sc.bb.record("ORDER_PLACE_REQUEST", source="STRATEGY", phase="ATTEMPT", outcome="PENDING", critical=True,
                 order_ref="sig-in", market_id="1.5", data={"purpose": "ENTRY", "buffered": True})
    assert "ENTRY_DURING_OUTAGE:sig-in" in _check(sc)


def _outage_case2(reconciled=True, inside=True):
    sc = Scenario()
    e = sc.place("sig-d", "1.6", "BACK", 2.0, 4.0, matched=4.0)
    if not inside:                                     # uscita partita PRIMA del guasto, senza richiesta
        sc.exchange.append({"betId": "BET-OUT", "customerOrderRef": "hedge:sig-d", "marketId": "1.6",
                            "side": "LAY", "sizeMatched": 2.6, "status": "EXECUTION_COMPLETE",
                            "placedDate": "2000-01-01T00:00:00.000000+00:00"})
    sc.bb.disk_full = True
    sc.bb.record("ORDER_PLACE_REQUEST", source="STRATEGY")  # attiva il guasto (non critico: nel buffer)
    if inside:
        sc.exit_during_outage(e, 2.6, 3.0)             # inviata dal buffer critico...
    sc.bb.crash_losing_buffer()                        # ...poi crash prima dello scarico: buffer perso
    hedge = [o for o in sc.exchange if o["customerOrderRef"] == "hedge:sig-d"][0]
    if reconciled:
        sc.reconcile_found(hedge["betId"], "hedge:sig-d", "1.6")
    return sc, hedge


def test_disco_pieno_caso2_buffer_perso_lacuna_unknown_e_riconciliazione() -> None:
    sc, _h = _outage_case2()
    recs = records_of(sc.bb)
    assert [r["type"] for r in recs if r["type"] in ("UNCLEAN_TERMINATION", "BLACKBOX_GAP")] == [
        "UNCLEAN_TERMINATION", "BLACKBOX_GAP"]
    gap = [r for r in recs if r["type"] == "BLACKBOX_GAP"][0]
    assert gap["outcome"] == "UNKNOWN" and gap["data"]["refs"] == []                # riferimenti ignoti = lista vuota
    assert _check(sc) == []


def test_block_caso2_senza_riconciliazione() -> None:
    sc, h = _outage_case2(reconciled=False)
    assert f"MISSING_REQUEST:{h['betId']}" in _check(sc)


def test_block_caso2_ordine_fuori_dall_intervallo_della_lacuna() -> None:
    sc, h = _outage_case2(inside=False)
    assert f"MISSING_REQUEST:{h['betId']}" in _check(sc)


def test_block_regola_vecchia_lacuna_con_ref_ma_senza_richiesta_bufferizzata() -> None:
    """La regola precedente («uscita senza richiesta se una lacuna successiva ne dichiara il ref») non basta piu'."""
    sc = Scenario()
    sc.place("sig-d", "1.6", "BACK", 2.0, 4.0, matched=4.0)
    sc.exchange.append({"betId": "BET-H2", "customerOrderRef": "hedge:sig-d", "marketId": "1.6", "side": "LAY",
                        "sizeMatched": 2.6, "status": "EXECUTION_COMPLETE", "placedDate": "2026-11-08T00:00:00+00:00"})
    sc.bb.record("BLACKBOX_GAP", source="SYSTEM", outcome="VERIFIED", data={"refs": ["hedge:sig-d"]})
    assert "MISSING_REQUEST:BET-H2" in _check(sc)


def test_block_ingresso_senza_richiesta_anche_dentro_una_lacuna_unknown() -> None:
    sc = Scenario()
    sc.bb.disk_full = True
    sc.bb.record("X", source="SYSTEM")
    sc.exchange.append({"betId": "BET-E", "customerOrderRef": "sig-e", "marketId": "1.6", "side": "BACK",
                        "sizeMatched": 4.0, "status": "EXECUTION_COMPLETE", "placedDate": _now()})
    sc.bb.crash_losing_buffer()
    sc.reconcile_found("BET-E", "sig-e", "1.6")
    assert "MISSING_REQUEST:BET-E" in _check(sc), "un ingresso non e' mai un'uscita protettiva"


def test_block_richiesta_non_critica() -> None:
    sc = Scenario()
    sc.bb.record("ORDER_PLACE_REQUEST", source="STRATEGY", phase="ATTEMPT", outcome="PENDING", critical=False,
                 order_ref="sig-nc", market_id="1.5")
    assert any(p.startswith("REQUEST_NOT_DURABLE") for p in _check(sc))


# ------------------------------------------------------------------ segreti
def test_block_segreto_nel_registro() -> None:
    sc = _campagna()
    sc.bb.record("CONFIG_CHANGE", source="USER", values_after={"app_key": "SEGRETO-XYZ"})
    assert "SECRET_LEAK" in _check(sc, secrets=["SEGRETO-XYZ"])
    assert "SECRET_LEAK" not in _check(_campagna(), secrets=["SEGRETO-XYZ"])


def test_vocabolario_nuovo_tutto_o_niente() -> None:
    """I tipi nuovi richiesti dall'oracolo (NEW_TYPES, lista consegnata a Claude B) sono tutti assenti prima di BB-1 e
    tutti presenti da BB-1 in poi: mai una parte sola (un anello della catena non registrabile)."""
    from core.event_journal import EVENT_CATEGORIES                       # {categoria: frozenset(tipi)}
    known = {t for types in EVENT_CATEGORIES.values() for t in types}
    present = set(oracle.NEW_TYPES) & known
    assert present in (set(), set(oracle.NEW_TYPES)), sorted(set(oracle.NEW_TYPES) - present)
    for t in (oracle.REQUEST, oracle.RESPONSE, oracle.MATCHED, oracle.CANCEL_REQUEST, oracle.CANCEL_RESPONSE,
              oracle.CASHOUT_REQUEST, oracle.CASHOUT_EXECUTED, oracle.SETTLEMENT):
        assert t in known, t


def test_block_richiesta_bufferizzata_coperta_solo_da_una_lacuna_unknown() -> None:
    """Il caso 1 vuole una lacuna con riferimenti NOTI: una lacuna UNKNOWN (caso 2) non copre una richiesta bufferizzata."""
    sc = Scenario()
    sc.bb.record("ORDER_PLACE_REQUEST", source="STRATEGY", phase="ATTEMPT", outcome="PENDING", critical=True,
                 order_ref="hedge:sig-z", market_id="1.5", data={"purpose": "HEDGE", "buffered": True})
    sc.bb.record("BLACKBOX_GAP", source="SYSTEM", outcome="UNKNOWN", data={"refs": ["hedge:sig-z"]})
    assert "BUFFERED_WITHOUT_GAP:hedge:sig-z" in _check(sc)


def test_block_richiesta_successiva_all_esito() -> None:
    """L'esito risale a una richiesta solo se la richiesta lo precede (seq): una richiesta registrata dopo non vale."""
    sc = Scenario()
    o = sc.place("sig-r", "1.5", "BACK", 2.0, 4.0, matched=4.0, cause=None)
    recs = records_of(sc.bb)
    res = [r for r in recs if r["type"] == "ORDER_PLACE_RESPONSE"][0]
    sc2 = Scenario()
    sc2.exchange = sc.exchange
    sc2.bb.record("ORDER_PLACE_RESPONSE", source="EXCHANGE", phase="RESULT", outcome="VERIFIED",
                  bet_id=o["bet"], order_ref="sig-r", market_id="1.5")
    sc2.bb.record("ORDER_PLACE_REQUEST", source="STRATEGY", phase="ATTEMPT", outcome="PENDING", critical=True,
                  order_ref="sig-r", market_id="1.5", data={"purpose": "ENTRY"})
    assert res and f"MISSING_REQUEST:{o['bet']}" in _check(sc2)
