"""PF-BLACKBOX-01 — oracolo indipendente di Claude D sul REGISTRO REALE di BB-1 (`core.blackbox`, `core.blackbox_query`).

Richiesta di Claude B (pfab-b-20261109-bb1-pr617-oracle-001, PR #617). Su un albero senza BB-1 il modulo e' saltato.
Indipendenza: la catena di hash e il formato UTC sono verificati con una reimplementazione propria (non con
`compute_hash`/`verify` del modulo); il registro si legge solo con `BlackBoxReader.query` (vista interna per la
riconciliazione, esterna per D1). Il comportamento del motore e' simulato con lo scenario dell'oracolo: verifica del
REGISTRO, non del cablaggio di BB-3/BB-4 (non ancora esistente).
"""
from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import os
import re
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time

import pytest

bb = pytest.importorskip("core.blackbox")
bq = pytest.importorskip("core.blackbox_query")

from tests.blackbox_completeness import oracle                      # noqa: E402
from tests.blackbox_completeness.fake_blackbox import EXIT_PURPOSES, EXIT_TYPES  # noqa: E402
from tests.blackbox_completeness.scenario import Scenario           # noqa: E402

GENESIS = "0" * 64
CANONICAL_UTC = re.compile(r"\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z\Z")
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def independent_hash(prev: str, rec: dict) -> str:
    """sha256(prev_hash + JSON canonico di {seq, campi del record}); i campi sono quelli che il lettore restituisce."""
    body = {k: v for k, v in rec.items() if k not in ("hash", "prev_hash", "not_recorded", "causation_chain")}
    canon = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False, default=str)
    return hashlib.sha256((prev + canon).encode("utf-8")).hexdigest()


def read_all(box, external=False):
    reader = bq.BlackBoxReader(box)
    out, after = [], 0
    while True:
        page = reader.query({}, after_seq=after, limit=500, external=external)
        out.extend(page["records"])
        after = page["next_after_seq"]
        if not after:
            return out


def raw_prev_hash(path):
    con = sqlite3.connect(path)
    try:
        return {s: p for s, p in con.execute("SELECT seq, prev_hash FROM events")}
    finally:
        con.close()


def with_prev(box, recs):
    prev = raw_prev_hash(box.path)
    return [dict(r, prev_hash=prev[r["seq"]]) for r in recs]


class RealAdapter:
    """Interfaccia `record` dello scenario sopra `core.blackbox.BlackBox`.

    `exit_outcomes_critical=True` (requisito proposto per BB-3): ogni evento di un'uscita protettiva, anche l'ESITO
    (risposta, abbinamento, esito del cashout), e' critico e bufferizzabile; altrimenti in un guasto BB-1 lo scarta
    (conteggiato solo come `dropped_non_critical` nella GAP) e l'anello si perde."""

    def __init__(self, box, exit_outcomes_critical=True):
        self.box, self.session_id = box, box.session_id
        self.exit_outcomes_critical = exit_outcomes_critical

    def record(self, type, *, source, phase="OBSERVATION", outcome="NOT_APPLICABLE", critical=False, data=None, **kw):
        exit_related = (isinstance(data, dict) and data.get("purpose") in EXIT_PURPOSES) or type in EXIT_TYPES \
            or str(kw.get("order_ref") or "").startswith("hedge:")
        if exit_related and self.exit_outcomes_critical:
            critical = True
        eid = self.box.record(type, source=source, phase=phase, outcome=outcome, critical=critical,
                              buffer_on_failure=bool(critical and exit_related), data=data, **kw)
        return {"event_id": eid}


def _box(tmp_path, name="bb.sqlite", **kw):
    return bb.BlackBox(str(tmp_path / name), **kw).open()


def _oracle(box, sc, **kw):
    recs = with_prev(box, read_all(box))
    found = oracle.check(recs, sc.exchange, sc.settlements, hash_fn=independent_hash, **kw)
    # il record non dichiara se la scrittura era critica: la durabilita' prima dell'invio e' provata a parte
    return [p for p in found if not p.startswith("REQUEST_NOT_DURABLE")]


def _campagna(sc):
    a = sc.microstake_entry("sig-a", "1.1", 0.5, 2.0, table_id=1)
    b = sc.microstake_entry("sig-b", "1.2", 0.3, 3.0, table_id=2)
    p = sc.place("sig-p", "1.4", "BACK", 2.0, 4.0, matched=1.0, table_id=4)
    sc.cancel(p)
    sc.protection_close(a, "SL_TRIGGERED", 0.33, 3.0)
    sc.protection_close(b, "TRAILING_TRIGGERED", 0.45, 2.0)
    sc.settle("1.1", -0.17)
    sc.settle("1.2", 0.14)
    sc.settle("1.4", 0.95)
    return sc


# ------------------------------------------------------------------ catena finanziaria, seq, hash, causation
def test_registro_reale_campagna_completa_nessun_anello_mancante(tmp_path) -> None:
    box = _box(tmp_path)
    box.start_session()
    sc = _campagna(Scenario(bb=RealAdapter(box)))
    assert _oracle(box, sc) == []
    assert box.verify()["ok"]


@pytest.mark.parametrize("step,codice", [("request", "MISSING_REQUEST"), ("response", "MISSING_RESULT"),
                                         ("matched", "MATCH_MISSING"), ("cashout_request", "HEDGE_WITHOUT_CAUSE"),
                                         ("close_outcome", "TRIGGER_WITHOUT_CLOSE_OUTCOME"),
                                         ("settlement", "SETTLEMENT_MISSING")])
def test_block_registro_reale_anello_mancante(tmp_path, step, codice) -> None:
    box = _box(tmp_path)
    sc = _campagna(Scenario(bb=RealAdapter(box), skip={step}))
    assert any(p.startswith(codice) for p in _oracle(box, sc))


def test_causation_e_correlation_risolvibili_e_catena_leggibile(tmp_path) -> None:
    box = _box(tmp_path)
    sc = _campagna(Scenario(bb=RealAdapter(box)))
    recs = read_all(box)
    ids = {r["event_id"]: r["seq"] for r in recs}
    for r in recs:
        if r.get("causation_id"):
            assert r["causation_id"] in ids and ids[r["causation_id"]] < r["seq"], r
    hedge_res = [r for r in recs if r["type"] == "ORDER_PLACE_RESPONSE" and str(r.get("order_ref")).startswith("hedge:")]
    chain = bq.BlackBoxReader(box).get(hedge_res[0]["event_id"], chain_depth=10, external=False)["causation_chain"]
    assert any(c.get("type") == "CASHOUT_REQUEST" for c in chain) and not any(c.get("missing") for c in chain)
    cid = "AABBCCDD-0000-4000-8000-000000000001"
    eid = box.record("CONFIG_CHANGE", source="USER", correlation_id=cid)
    got = bq.BlackBoxReader(box).query({"correlation_id": cid.lower()}, external=False)["records"]
    assert [r["event_id"] for r in got] == [eid]


def _writer(path, n, tag):
    import core.blackbox as b
    box = b.BlackBox(path).open()
    for i in range(n):
        box.record("CONFIG_CHANGE", source="SYSTEM", data={"w": tag, "i": i})
    box.close()


def test_seq_contigua_e_unica_con_piu_processi(tmp_path) -> None:
    path = str(tmp_path / "multi.sqlite")
    bb.BlackBox(path).open().close()
    ctx = mp.get_context("spawn")
    procs = [ctx.Process(target=_writer, args=(path, 60, t)) for t in range(4)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(120)
    assert all(p.exitcode == 0 for p in procs)
    box = bb.BlackBox(path).open()
    recs = with_prev(box, read_all(box))
    assert [r["seq"] for r in recs] == list(range(1, 241))
    assert len({(r["data"]["w"], r["data"]["i"]) for r in recs}) == 240
    assert [p for p in oracle.check(recs, hash_fn=independent_hash) if p.startswith(("SEQ_", "CHAIN_"))] == []


def test_block_manomissione_rilevata_dalla_verifica_indipendente(tmp_path) -> None:
    box = _box(tmp_path)
    _campagna(Scenario(bb=RealAdapter(box)))
    con = sqlite3.connect(box.path)
    with pytest.raises(sqlite3.DatabaseError):
        con.execute("UPDATE events SET reason = 'x' WHERE seq = 3")          # append-only (trigger)
    con.execute("DROP TRIGGER events_no_update")
    con.execute("UPDATE events SET values_after = '{\"size_matched\":999}' WHERE seq = 3")
    con.commit()
    con.close()
    recs = with_prev(box, read_all(box))
    assert any(p.startswith("CHAIN_BROKEN") for p in oracle.check(recs, hash_fn=independent_hash))
    assert not box.verify()["ok"]


# ------------------------------------------------------------------ lacune: caso 1 (buffer recuperato)
def _caso1(tmp_path, monkeypatch, exit_outcomes_critical=True):
    box = _box(tmp_path)
    box.start_session()
    sc = Scenario(bb=RealAdapter(box, exit_outcomes_critical))
    e = sc.place("sig-d", "1.6", "BACK", 2.0, 4.0, matched=4.0)
    real_insert = box._insert

    def broken(rec):
        raise sqlite3.OperationalError("database or disk is full")
    monkeypatch.setattr(box, "_insert", broken)
    with pytest.raises(bb.BlackBoxUnavailable):                              # nuovo ingresso: fail-closed
        sc.place("sig-new", "1.5", "BACK", 2.0, 4.0, matched=4.0)
    assert all(o["customerOrderRef"] != "sig-new" for o in sc.exchange)
    hedge = sc.exit_during_outage(e, 2.6, 3.0)                               # uscita: nel buffer critico
    assert box.health()["degraded"] and box.health()["buffered"] > 0
    monkeypatch.setattr(box, "_insert", real_insert)
    box.record("CONFIG_CHANGE", source="SYSTEM")                             # primo evento dopo il ripristino
    return box, sc, hedge


def test_lacuna_caso1_guasto_di_scrittura_ingresso_rifiutato_uscita_bufferizzata(tmp_path, monkeypatch) -> None:
    box, sc, hedge = _caso1(tmp_path, monkeypatch)
    recs = read_all(box)
    gap = [r for r in recs if r["type"] == "BLACKBOX_GAP"]
    # esito della GAP di guasto: VERIFIED su 3b6848d6, UNKNOWN dopo D4; l'oracolo non dipende dall'esito
    assert len(gap) == 1 and hedge["order_ref"] in gap[0]["data"]["refs"] and gap[0]["reason_code"] == "BLACKBOX_OUTAGE"
    assert [r["type"] for r in recs if r["seq"] > gap[0]["seq"]][:1] == ["BLACKBOX_RECOVERED"]
    assert _oracle(box, sc) == []


# ------------------------------------------------------------------ crash, UNCLEAN_TERMINATION, caso 2
_CRASH = textwrap.dedent("""
    import sys, time
    sys.path.insert(0, {repo!r})
    import core.blackbox as b
    box = b.BlackBox({path!r}).open()
    box.start_session()
    print("READY", flush=True)
    i = 0
    while True:
        box.record("CONFIG_CHANGE", source="SYSTEM", data={{"i": i}})
        i += 1
""")


def test_crash_durante_la_scrittura_poi_unclean_termination_e_catena_integra(tmp_path) -> None:
    path = str(tmp_path / "crash.sqlite")
    proc = subprocess.Popen([sys.executable, "-c", _CRASH.format(repo=REPO, path=path)], stdout=subprocess.PIPE,
                            text=True)
    assert proc.stdout.readline().strip() == "READY"
    time.sleep(0.5)
    proc.send_signal(signal.SIGKILL)
    proc.wait(30)
    box = bb.BlackBox(path).open()
    box.start_session()
    recs = read_all(box)
    types = [r["type"] for r in recs]
    i = types.index("UNCLEAN_TERMINATION")
    assert types[i + 1] == "BLACKBOX_GAP" and recs[i + 1]["outcome"] == "UNKNOWN" and recs[i + 1]["data"]["refs"] == []
    assert types[i + 2] == "PROCESS_START" and recs[i]["session_id"] != recs[0]["session_id"]
    recs = with_prev(box, recs)
    assert [p for p in oracle.check(recs, hash_fn=independent_hash) if p.startswith(("SEQ_", "CHAIN_"))] == []
    assert box.verify()["ok"]


def test_lacuna_caso2_buffer_perso_uscita_dentro_la_gap_unknown_con_riconciliazione(tmp_path) -> None:
    path = str(tmp_path / "c2.sqlite")
    box = bb.BlackBox(path).open()
    box.start_session()
    sc = Scenario(bb=RealAdapter(box))
    sc.place("sig-d", "1.6", "BACK", 2.0, 4.0, matched=4.0)
    box.close()                                                             # crash: nessun SHUTDOWN
    time.sleep(0.01)
    sc.exchange.append({"betId": "BET-X", "customerOrderRef": "hedge:sig-d", "marketId": "1.6", "side": "LAY",
                        "sizeMatched": 2.6, "status": "EXECUTION_COMPLETE",
                        "placedDate": bb.utc_iso(time.time())})            # uscita partita dal buffer poi perso
    time.sleep(0.01)
    box2 = bb.BlackBox(path).open()
    box2.start_session()
    sc.bb = RealAdapter(box2)
    sc.reconcile_found("BET-X", "hedge:sig-d", "1.6")
    assert _oracle(box2, sc) == []
    sc.exchange[-1]["placedDate"] = "2000-01-01T00:00:00.000000Z"           # BLOCK: fuori dall'intervallo
    assert "MISSING_REQUEST:BET-X" in _oracle(box2, sc)


# ------------------------------------------------------------------ D1, D2, D3
def test_d1_alias_ord_stabile_non_enumerabile_e_vista_esterna_senza_ref(tmp_path) -> None:
    box = _box(tmp_path)
    eid = box.record("ORDER_PLACE_REQUEST", source="STRATEGY", phase="ATTEMPT", outcome="PENDING", critical=True,
                     order_id=42, order_ref="sig-segreto", market_id="1.1",
                     data={"purpose": "ENTRY", "customerOrderRef": "sig-segreto"})
    alias = box.order_alias(42)
    assert re.fullmatch(r"ord_[0-9a-f]{32}", alias) and "42" not in alias[4:8]
    assert box.order_alias(43) != alias
    path = box.path
    box.close()
    box2 = bb.BlackBox(path).open()
    assert box2.order_alias(42) == alias                                    # stabile al riavvio
    ext = read_all(box2, external=True)
    assert all("order_ref" not in r for r in ext) and "sig-segreto" not in json.dumps(ext)
    row = [r for r in ext if r["event_id"] == eid][0]
    assert row["order_id"] == alias
    got = bq.BlackBoxReader(box2).query({"order_id": alias})["records"]
    assert [r["event_id"] for r in got] == [eid]
    assert bq.BlackBoxReader(box2).query({"order_id": "42"})["records"] == []      # mai per id interno


def test_d2_ts_utc_canonico_e_exchange_ts_solo_utc(tmp_path) -> None:
    box = _box(tmp_path)
    box.start_session()
    _campagna(Scenario(bb=RealAdapter(box)))
    assert all(CANONICAL_UTC.match(r["ts_utc"]) for r in read_all(box))
    box.record("ORDER_MATCHED", source="EXCHANGE", exchange_ts="2026-11-09T14:00:00.123456Z")
    for bad in ("2026-11-09T15:00:00.123456+01:00", "2026-11-09 14:00:00", "2026-11-09T14:00:00Z", 1731160800):
        with pytest.raises(ValueError):
            box.record("ORDER_MATCHED", source="EXCHANGE", exchange_ts=bad)


_SECOND = textwrap.dedent("""
    import sys
    sys.path.insert(0, {repo!r})
    import core.blackbox as b
    try:
        b.BlackBox({path!r}).open().start_session()
        print("STARTED")
    except b.RuntimeAlreadyActive as exc:
        print("REFUSED", exc)
""")


def test_d3_secondo_runtime_rifiutato_poi_ammesso_dopo_la_chiusura(tmp_path) -> None:
    path = str(tmp_path / "rt.sqlite")
    first = bb.BlackBox(path).open()
    first.start_session()
    out = subprocess.run([sys.executable, "-c", _SECOND.format(repo=REPO, path=path)], capture_output=True, text=True,
                         timeout=60).stdout
    assert out.startswith("REFUSED") and "RUNTIME_ALREADY_ACTIVE" in out, out
    first.end_session()
    first.close()
    out = subprocess.run([sys.executable, "-c", _SECOND.format(repo=REPO, path=path)], capture_output=True, text=True,
                         timeout=60).stdout
    assert out.startswith("STARTED"), out


def test_durabilita_prima_dell_invio_richiesta_critica_fail_closed(tmp_path, monkeypatch) -> None:
    """Il record non dichiara `critical`: la durabilita' prima dell'invio si prova sul comportamento."""
    box = _box(tmp_path)
    monkeypatch.setattr(box, "_insert", lambda rec: (_ for _ in ()).throw(sqlite3.OperationalError("I/O")))
    with pytest.raises(bb.BlackBoxUnavailable):
        box.record("ORDER_PLACE_REQUEST", source="STRATEGY", phase="ATTEMPT", outcome="PENDING", critical=True,
                   order_ref="sig-z", data={"purpose": "ENTRY"})
    assert box.record("CONFIG_CHANGE", source="SYSTEM") is None and box.health()["dropped_non_critical"] == 1


def test_block_esiti_delle_uscite_non_critici_persi_nel_guasto(tmp_path, monkeypatch) -> None:
    """Requisito per BB-3: se l'esito di un'uscita e' registrato come NON critico, in un guasto BB-1 lo scarta (lo conta
    soltanto in `dropped_non_critical`) e l'oracolo segnala l'anello mancante."""
    box, sc, hedge = _caso1(tmp_path, monkeypatch, exit_outcomes_critical=False)
    gap = [r for r in read_all(box) if r["type"] == "BLACKBOX_GAP"][0]
    assert gap["data"]["dropped_non_critical"] > 0
    problems = _oracle(box, sc)
    assert f"MISSING_RESULT:{hedge['order_ref']}" in problems and any(
        p.startswith("CASHOUT_WITHOUT_RESULT") for p in problems), problems


# ------------------------------------------------------------------ PR #617 head d1454af3 (I2 di Claude C)
def test_gap_di_guasto_unknown_e_ref_cumulativi_con_scarico_a_meta(tmp_path, monkeypatch) -> None:
    """Tre uscite nel buffer; lo scarico riesce per la prima e fallisce sulla seconda; al ripristino completo la GAP
    finale elenca TUTTI i ref ed e' UNKNOWN (D4); l'oracolo copre ogni richiesta bufferizzata."""
    box = _box(tmp_path)
    box.start_session()
    sc = Scenario(bb=RealAdapter(box))
    entries = [sc.place(f"sig-{i}", f"1.{i}", "BACK", 2.0, 4.0, matched=4.0, table_id=i) for i in (1, 2, 3)]
    real_insert = box._insert
    state = {"mode": "down", "ok": 0}

    def flaky(rec):
        if state["mode"] == "down":
            raise sqlite3.OperationalError("database or disk is full")
        if state["mode"] == "half":
            if state["ok"] >= 1:
                raise sqlite3.OperationalError("disk full again")
            state["ok"] += 1
        return real_insert(rec)
    monkeypatch.setattr(box, "_insert", flaky)
    hedges = [sc.exit_during_outage(e, 2.6, 3.0) for e in entries]
    state["mode"] = "half"
    assert box.record("CONFIG_CHANGE", source="SYSTEM") is None              # scarico a meta': fallisce di nuovo
    state["mode"] = "up"
    box.record("CONFIG_CHANGE", source="SYSTEM")                             # ripristino completo
    recs = read_all(box)
    gaps = [r for r in recs if r["type"] == "BLACKBOX_GAP"]
    assert len(gaps) == 1 and gaps[0]["outcome"] == "UNKNOWN" and gaps[0]["reason_code"] == "BLACKBOX_OUTAGE"
    assert {h["order_ref"] for h in hedges} <= set(gaps[0]["data"]["refs"])
    assert _oracle(box, sc) == []
    ext = read_all(box, external=True)
    g_ext = [r for r in ext if r["type"] == "BLACKBOX_GAP"][0]
    assert "refs" not in (g_ext["data"] or {}) and g_ext["data"].get("refs_count") == len(gaps[0]["data"]["refs"])


def test_evento_di_un_lettore_dopo_lo_shutdown_non_e_un_crash(tmp_path) -> None:
    path = str(tmp_path / "rd.sqlite")
    box = bb.BlackBox(path).open()
    box.start_session()
    box.end_session()
    box.close()
    reader_box = bb.BlackBox(path).open()                                    # lettore (Control API): niente runtime
    assert bq.BlackBoxReader(reader_box).request_hook({"phase": "ATTEMPT", "route": "/v1/blackbox/status"})
    reader_box.close()
    box2 = bb.BlackBox(path).open()
    box2.start_session()
    assert "UNCLEAN_TERMINATION" not in [r["type"] for r in read_all(box2)]


def test_finestra_della_gap_parte_dall_ultimo_evento_del_runtime_caduto(tmp_path) -> None:
    path = str(tmp_path / "win.sqlite")
    box = bb.BlackBox(path).open()
    box.start_session()
    box.record("CONFIG_CHANGE", source="SYSTEM")
    box.close()                                                              # crash: nessuno SHUTDOWN
    last_runtime_ts = read_all(bb.BlackBox(path).open())[-1]["ts_utc"]
    time.sleep(0.01)
    reader_box = bb.BlackBox(path).open()
    bq.BlackBoxReader(reader_box).request_hook({"phase": "ATTEMPT", "route": "/v1/blackbox/query"})
    reader_box.close()
    box2 = bb.BlackBox(path).open()
    box2.start_session()
    recs = read_all(box2)
    gap = [r for r in recs if r["type"] == "BLACKBOX_GAP"][0]
    assert "UNCLEAN_TERMINATION" in [r["type"] for r in recs]
    assert gap["data"]["from_ts"] == last_runtime_ts and gap["reason_code"] == "UNCLEAN_TERMINATION"


# ------------------------------------------------------------------ PR #617 head dd7b7369
def test_recupero_riprendibile_gap_mancante_scritta_al_riavvio_successivo(tmp_path, monkeypatch) -> None:
    """UNCLEAN_TERMINATION scritto ma la sua GAP no (guasto a meta' recupero): il riavvio successivo scrive la GAP
    UNKNOWN mancante con from_ts = data.from_ts dell'UNCLEAN, senza un secondo UNCLEAN e con la catena integra."""
    path = str(tmp_path / "res.sqlite")
    box = bb.BlackBox(path).open()
    box.start_session()
    box.record("CONFIG_CHANGE", source="SYSTEM")
    box.close()                                                              # crash
    box2 = bb.BlackBox(path).open()
    real_insert = box2._insert

    def no_gap(rec):
        if rec.get("type") == "BLACKBOX_GAP":
            raise sqlite3.OperationalError("disk full")
        return real_insert(rec)
    monkeypatch.setattr(box2, "_insert", no_gap)
    with pytest.raises(bb.BlackBoxUnavailable):
        box2.start_session()
    box2.close()
    box3 = bb.BlackBox(path).open()
    box3.start_session()
    recs = read_all(box3)
    types = [r["type"] for r in recs]
    assert types.count("UNCLEAN_TERMINATION") == 1 and types.count("BLACKBOX_GAP") == 1
    unclean = recs[types.index("UNCLEAN_TERMINATION")]
    gap = recs[types.index("BLACKBOX_GAP")]
    assert gap["outcome"] == "UNKNOWN" and gap["reason_code"] == "UNCLEAN_TERMINATION"
    assert unclean["data"]["from_ts"] and gap["data"]["from_ts"] == unclean["data"]["from_ts"]
    assert gap["seq"] > unclean["seq"]
    recs = with_prev(box3, recs)
    assert [p for p in oracle.check(recs, hash_fn=independent_hash) if p.startswith(("SEQ_", "CHAIN_"))] == []


class _FailSelectOnce:
    """Connessione che fa fallire UNA volta la lettura dell'ultimo seq dentro la transazione (prima dell'incremento)."""

    def __init__(self, conn):
        self._conn, self.armed = conn, True

    def execute(self, sql, *args):
        if self.armed and sql.startswith("SELECT seq, hash FROM events"):
            self.armed = False
            raise sqlite3.OperationalError("database is locked")
        return self._conn.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._conn, name)


@pytest.mark.parametrize("fail_on", ["read", "write"])
def test_session_seq_contigua_anche_dopo_una_lettura_o_scrittura_fallita(tmp_path, monkeypatch, fail_on) -> None:
    """Rilievo Codex #617: dopo una LETTURA fallita nella transazione session_seq veniva riusato (1, 1, 2, 3)."""
    box = _box(tmp_path)
    box.start_session()
    box.record("CONFIG_CHANGE", source="SYSTEM", data={"i": 0})
    if fail_on == "read":
        monkeypatch.setattr(box, "_conn", _FailSelectOnce(box._conn))
    else:
        real_insert, calls = box._insert, {"n": 0}

        def once_broken(rec):
            calls["n"] += 1
            if calls["n"] == 1:
                raise sqlite3.OperationalError("I/O")
            return real_insert(rec)
        monkeypatch.setattr(box, "_insert", once_broken)
    assert box.record("CONFIG_CHANGE", source="SYSTEM", data={"i": 1}) is None         # non critico: perso
    for i in range(2, 5):
        box.record("CONFIG_CHANGE", source="SYSTEM", data={"i": i})
    monkeypatch.undo()
    by_session = {}
    for r in read_all(box):
        by_session.setdefault(r["session_id"], []).append(r["session_seq"])
    for sid, seqs in by_session.items():
        assert seqs == list(range(1, len(seqs) + 1)), (sid, seqs)


def test_ts_utc_anteriore_a_un_seq_minore_ammesso_l_ordine_e_seq(tmp_path) -> None:
    """D2: EMERGENCY_STOP registra l'ATTEMPT dopo la cancellazione con ts_utc = ora della richiesta: l'orario puo'
    precedere eventi con seq minore. Fa fede seq; l'oracolo non deve segnalare nulla e la query per intervallo lo trova."""
    box = _box(tmp_path)
    box.start_session()
    t_req = bb.utc_iso(time.time())
    time.sleep(0.01)
    box.record("CONFIG_CHANGE", source="SYSTEM")
    eid = box.record("EMERGENCY_STOP", source="USER", phase="ATTEMPT", outcome="PENDING", ts_utc=t_req)
    box.record("EMERGENCY_STOP", source="SYSTEM", phase="RESULT", outcome="VERIFIED", causation_id=eid)
    recs = with_prev(box, read_all(box))
    seqs = [r["seq"] for r in recs]
    assert seqs == sorted(seqs) and recs[-2]["ts_utc"] < recs[-3]["ts_utc"]
    assert oracle.check(recs, hash_fn=independent_hash) == []
    got = bq.BlackBoxReader(box).query({"from_ts": t_req, "to_ts": t_req}, external=False)["records"]
    assert [r["event_id"] for r in got] == [eid]
