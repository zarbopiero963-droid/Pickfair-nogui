"""PF-BLACKBOX-01 — oracolo di completezza finanziaria (Claude D), offline sul contratto §3/§4/§4-bis.

Per ogni scenario della campagna: registro completo => nessun problema (PASS); un anello tolto alla scrittura =>
l'oracolo lo segnala con il codice atteso (BLOCK). Il registro si legge solo via `query` (records_of).
"""
from __future__ import annotations

import threading

import pytest

from tests.blackbox_completeness import oracle
from tests.blackbox_completeness.fake_blackbox import FakeBlackBox, records_of
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


# ------------------------------------------------------------------ disco pieno
def test_disco_pieno_nessun_nuovo_ingresso_uscita_coperta_da_lacuna_dichiarata() -> None:
    sc = Scenario()
    e = sc.place("sig-d", "1.6", "BACK", 2.0, 4.0, matched=4.0)
    sc.bb.disk_full = True
    assert sc.try_entry_while_unavailable("sig-new", "1.5") is False          # fail-closed: nessun invio
    assert all(o["customerOrderRef"] != "sig-new" for o in sc.exchange)
    # uscita protettiva consentita durante il guasto (buffer critico in memoria, §2): ordine sull'exchange senza record
    sc.exchange.append({"betId": "BET-H", "customerOrderRef": "hedge:sig-d", "marketId": "1.6", "side": "LAY",
                        "sizeMatched": 2.6, "status": "EXECUTION_COMPLETE"})
    sc.bb.recover_disk()
    sc.bb.record("BLACKBOX_GAP", source="SYSTEM", outcome="UNKNOWN", data={"refs": ["hedge:sig-d"]},
                 reason_code="DISK_FULL")
    assert _check(sc) == []
    assert e["bet"] in {o["betId"] for o in sc.exchange}


def test_block_uscita_durante_il_guasto_senza_lacuna_dichiarata() -> None:
    sc = Scenario()
    sc.place("sig-d", "1.6", "BACK", 2.0, 4.0, matched=4.0)
    sc.exchange.append({"betId": "BET-H2", "customerOrderRef": "hedge:sig-d", "marketId": "1.6", "side": "LAY",
                        "sizeMatched": 2.6, "status": "EXECUTION_COMPLETE"})
    assert "MISSING_RESULT_FOR_BET:BET-H2" in _check(sc)


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


def test_vocabolario_nuovo_dichiarato_e_assente_da_event_categories() -> None:
    """I tipi che l'oracolo richiede e che oggi mancano sono esattamente quelli in NEW_TYPES (lista per Claude B)."""
    from core.event_journal import EVENT_CATEGORIES                       # {categoria: frozenset(tipi)}
    known = {t for types in EVENT_CATEGORIES.values() for t in types}
    assert not (set(oracle.NEW_TYPES) & known), set(oracle.NEW_TYPES) & known
    for t in (oracle.REQUEST, oracle.RESPONSE, oracle.MATCHED, oracle.CANCEL_REQUEST, oracle.CANCEL_RESPONSE,
              oracle.CASHOUT_REQUEST, oracle.CASHOUT_EXECUTED, oracle.SETTLEMENT):
        assert t in known, t
