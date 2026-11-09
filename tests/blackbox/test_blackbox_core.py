"""PF-BLACKBOX-01 (BB-1): nucleo della scatola nera. SQLite reale su tmp_path, offline, deterministico."""
import json
import multiprocessing
import os
import sqlite3
import threading

import pytest

from core import blackbox as bb
from core import blackbox_query as bq
from core import event_log


class _Clock:
    def __init__(self, t=1_700_000_000.0):
        self.t = t

    def __call__(self):
        self.t += 0.001
        return self.t


def _box(tmp_path, name="bb.sqlite", **kw):
    return bb.BlackBox(str(tmp_path / name), clock=kw.pop("clock", _Clock()), **kw).open()


def _rows(box):
    return box._conn.execute("SELECT * FROM events ORDER BY seq").fetchall()


# ================================================================== integrita' e append-only
def test_events_are_appended_with_contiguous_seq_and_a_valid_hash_chain(tmp_path):
    box = _box(tmp_path)
    ids = [box.record("API_REQUEST", source="EXCHANGE", component="betfair", data={"i": i}) for i in range(5)]
    rows = _rows(box)
    assert [r["seq"] for r in rows] == [1, 2, 3, 4, 5]
    assert [r["event_id"] for r in rows] == ids
    assert [r["session_seq"] for r in rows] == [1, 2, 3, 4, 5]
    assert rows[0]["prev_hash"] == bb.GENESIS_HASH and rows[1]["prev_hash"] == rows[0]["hash"]
    assert rows[0]["ts_utc"].endswith("Z") and len(rows[0]["ts_utc"]) == len("2023-11-14T22:13:20.001000Z")
    assert box.verify() == {"ok": True, "last_seq": 5, "count": 5, "problems": []}


def test_update_and_delete_are_refused_by_the_store(tmp_path):
    box = _box(tmp_path)
    box.record("API_REQUEST", data={"x": 1})
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        box._conn.execute("UPDATE events SET reason = 'falso' WHERE seq = 1")
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        box._conn.execute("DELETE FROM events WHERE seq = 1")


@pytest.mark.parametrize("tamper", ["update_field", "delete_middle", "swap_hash"])
def test_tampering_that_bypasses_the_triggers_is_detected_by_verify(tmp_path, tamper):
    box = _box(tmp_path)
    for i in range(3):
        box.record("API_REQUEST", data={"i": i}, reason=f"r{i}")
    c = box._conn
    c.execute("DROP TRIGGER events_no_update"); c.execute("DROP TRIGGER events_no_delete")
    if tamper == "update_field":
        c.execute("UPDATE events SET reason = 'falsificato' WHERE seq = 2")
        expect = "HASH_MISMATCH"
    elif tamper == "delete_middle":
        c.execute("DELETE FROM events WHERE seq = 2")
        expect = "SEQ_GAP"
    else:
        c.execute("UPDATE events SET hash = ? WHERE seq = 2", ("f" * 64,))
        expect = "HASH_MISMATCH"
    rep = box.verify()
    assert rep["ok"] is False and expect in {p["problem"] for p in rep["problems"]}


def test_unknown_type_source_phase_outcome_or_field_is_refused(tmp_path):
    box = _box(tmp_path)
    with pytest.raises(ValueError):
        box.record("NON_ESISTE")
    with pytest.raises(ValueError):
        box.record("API_REQUEST", source="HACKER")
    with pytest.raises(ValueError):
        box.record("API_REQUEST", phase="MAYBE")
    with pytest.raises(ValueError):
        box.record("API_REQUEST", outcome="OK")
    with pytest.raises(ValueError):
        box.record("API_REQUEST", campo_inventato=1)
    assert box.verify()["count"] == 0


# ================================================================== redazione
def test_secrets_never_reach_the_store(tmp_path):
    secret = "S3cr3t-literal-value-XYZ"
    event_log.register_secret(secret)
    box = _box(tmp_path)
    box.record("CONFIG_CHANGE", source="USER",
               values_before={"password": "PWD-VECCHIA-111", "app_key": "APPKEY-222", "stake": 1.0},
               values_after={"password": "PWD-NUOVA-333", "nested": {"session_token": "SESS-444"}, "stake": 2.0},
               reason=f"cambiato {secret}", data={"note": f"valore {secret}"})
    raw = json.dumps([dict(r) for r in _rows(box)])
    for s in ("PWD-VECCHIA-111", "APPKEY-222", "PWD-NUOVA-333", "SESS-444", secret):
        assert s not in raw
    row = _rows(box)[0]
    assert json.loads(row["values_after"])["stake"] == 2.0, "i valori non sensibili restano"


# ================================================================== guasti di scrittura
class _FailingConn:
    """Connessione che fallisce sulle scritture come un disco pieno, finche' `fail` e' vero."""

    def __init__(self, real):
        self.real, self.fail = real, True

    def execute(self, sql, *a):
        if self.fail and sql.startswith(("BEGIN", "INSERT")):
            raise sqlite3.OperationalError("database or disk is full")
        return self.real.execute(sql, *a)

    def __getattr__(self, name):
        return getattr(self.real, name)


def _break(box):
    fc = _FailingConn(box._conn)
    box._conn = fc
    return fc


def test_critical_write_failure_raises_and_nothing_is_recorded(tmp_path):
    box = _box(tmp_path)
    fc = _break(box)
    with pytest.raises(bb.BlackBoxUnavailable):
        box.record("ORDER_PLACE_REQUEST", critical=True, source="STRATEGY", phase="ATTEMPT", order_ref="r1")
    fc.fail = False
    assert box.health()["degraded"] is True
    assert [r["type"] for r in _rows(box)] == []


def test_non_critical_failure_is_counted_and_declared_as_a_gap_on_recovery(tmp_path):
    box = _box(tmp_path)
    fc = _break(box)
    assert box.record("PRICE_SNAPSHOT", data={"p": 2.0}) is None
    assert box.dropped == 1
    fc.fail = False
    box.record("API_REQUEST")
    types = [r["type"] for r in _rows(box)]
    assert types == ["BLACKBOX_GAP", "BLACKBOX_RECOVERED", "API_REQUEST"]
    gap = json.loads(_rows(box)[0]["data"])
    assert gap["dropped_non_critical"] == 1 and box.verify()["ok"]


def test_protective_exit_is_buffered_then_flushed_before_gap_and_recovered(tmp_path):
    """Caso 1 concordato con Claude D: buffer critico -> eventi bufferizzati (ora originale, buffered=true) ->
    BLACKBOX_GAP con from/to e ref -> BLACKBOX_RECOVERED."""
    box = _box(tmp_path)
    fc = _break(box)
    eid = box.record("CASHOUT_REQUEST", critical=True, buffer_on_failure=True, source="STRATEGY",
                     phase="ATTEMPT", order_ref="hedge:ref-1")
    assert eid and box.health()["buffered"] == 1
    with pytest.raises(bb.BlackBoxUnavailable):                  # un ingresso non si bufferizza mai
        box.record("ORDER_PLACE_REQUEST", critical=True, source="STRATEGY", phase="ATTEMPT", order_ref="ref-2")
    fc.fail = False
    box.record("API_REQUEST")
    rows = _rows(box)
    assert [r["type"] for r in rows] == ["CASHOUT_REQUEST", "BLACKBOX_GAP", "BLACKBOX_RECOVERED", "API_REQUEST"]
    assert rows[0]["event_id"] == eid and json.loads(rows[0]["data"])["buffered"] is True
    gap = json.loads(rows[1]["data"])
    assert gap["refs"] == ["hedge:ref-1"] and gap["from_ts"] <= gap["to_ts"] and rows[1]["outcome"] == "VERIFIED"
    assert rows[0]["ts_utc"] < rows[1]["ts_utc"], "ora originale dell'evento bufferizzato"
    assert box.verify()["ok"] and box.health()["buffered"] == 0


# ================================================================== sessioni
def test_clean_restart_has_no_unclean_termination(tmp_path):
    a = _box(tmp_path, session_id="s1")
    a.start_session(); a.end_session("STOP"); a.close()
    b = _box(tmp_path, session_id="s2")
    b.start_session()
    assert [r["type"] for r in _rows(b)] == ["PROCESS_START", "SHUTDOWN", "PROCESS_START"]


def test_restart_after_a_crash_records_unclean_termination_and_an_unknown_gap(tmp_path):
    a = _box(tmp_path, session_id="s1")
    a.start_session(); a.record("ORDER_PLACE_REQUEST", critical=True, phase="ATTEMPT", order_ref="r1")
    a.close()                                                     # nessun SHUTDOWN: crash
    b = _box(tmp_path, session_id="s2")
    b.start_session()
    rows = _rows(b)
    assert [r["type"] for r in rows][-3:] == ["UNCLEAN_TERMINATION", "BLACKBOX_GAP", "PROCESS_START"]
    gap = rows[-2]
    assert gap["outcome"] == "UNKNOWN" and json.loads(gap["data"])["refs"] == []
    assert json.loads(rows[-3]["data"])["previous_session"] == "s1"
    sessions = bq.BlackBoxReader(b).sessions()
    assert [(s["session_id"], s["clean_shutdown"]) for s in sessions] == [("s1", False), ("s2", False)]


# ================================================================== concorrenza
def test_multithread_writes_keep_a_contiguous_unique_sequence(tmp_path):
    box = _box(tmp_path, clock=__import__("time").time)

    def worker(n):
        for i in range(40):
            box.record("API_REQUEST", table_id=n, data={"i": i})

    ts = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
    [t.start() for t in ts]; [t.join() for t in ts]
    rep = box.verify()
    assert rep["ok"] and rep["count"] == 320 and rep["last_seq"] == 320


def _proc_writer(path, n):
    box = bb.BlackBox(path, session_id=f"p{n}").open()
    for i in range(30):
        box.record("API_REQUEST", table_id=n, data={"i": i})
    box.close()


def test_multiprocess_writes_keep_a_contiguous_unique_sequence(tmp_path):
    path = str(tmp_path / "mp.sqlite")
    bb.BlackBox(path).open().close()
    ctx = multiprocessing.get_context("spawn")
    ps = [ctx.Process(target=_proc_writer, args=(path, n)) for n in range(4)]
    [p.start() for p in ps]; [p.join(60) for p in ps]
    assert all(p.exitcode == 0 for p in ps)
    rep = bb.BlackBox(path).open().verify()
    assert rep["ok"] and rep["count"] == 120 and rep["last_seq"] == 120


# ================================================================== lettura (contratto Claude C 1.13.0)
def _populated(tmp_path):
    box = _box(tmp_path, session_id="s1")
    box.start_session()
    req = box.record("ORDER_PLACE_REQUEST", critical=True, source="STRATEGY", phase="ATTEMPT", table_id=3,
                     market_id="1.5", order_ref="ref-9", data={"customer_ref": "ref-9", "stake": 2.0})
    box.record("ORDER_PLACE_RESPONSE", source="EXCHANGE", phase="RESULT", outcome="VERIFIED", table_id=3,
               market_id="1.5", bet_id="B1", order_ref="ref-9", causation_id=req)
    for i in range(5):
        box.record("PRICE_SNAPSHOT", source="EXCHANGE", phase="OBSERVATION", market_id="1.7", data={"i": i})
    return box, req


def test_query_filters_paginates_and_hides_customer_ref_externally(tmp_path):
    box, _ = _populated(tmp_path)
    r = bq.BlackBoxReader(box)
    page = r.query({"market_id": "1.5"}, limit=1)
    assert [e["type"] for e in page["records"]] == ["ORDER_PLACE_REQUEST"] and page["next_after_seq"] == 2
    page2 = r.query({"market_id": "1.5"}, after_seq=page["next_after_seq"], limit=1)
    assert [e["type"] for e in page2["records"]] == ["ORDER_PLACE_RESPONSE"]
    last = r.query({"market_id": "1.5"}, after_seq=page2["records"][-1]["seq"], limit=5)
    assert last["records"] == [] and last["next_after_seq"] == 0 and last["as_of_seq"] == 8
    ext = page["records"][0]
    assert "order_ref" not in ext and "customer_ref" not in ext["data"] and ext["data"]["stake"] == 2.0
    assert "bet_id" in ext["not_recorded"]
    internal = r.query({"order_ref": "ref-9"}, external=False)
    assert len(internal["records"]) == 2 and internal["records"][0]["order_ref"] == "ref-9"


def test_query_rejects_unknown_filters_and_bad_limits(tmp_path):
    box, _ = _populated(tmp_path)
    r = bq.BlackBoxReader(box)
    for bad in ({"order_ref": "x"}, {"sql": "1=1"}):
        with pytest.raises(bq.QueryError):
            r.query(bad)
    for kw in ({"limit": 0}, {"limit": bq.MAX_LIMIT + 1}, {"after_seq": -1}, {"limit": True}):
        with pytest.raises(bq.QueryError):
            r.query({}, **kw)
    assert r.query({})["as_of_seq"] == 8 and bq.BlackBoxReader(_box(tmp_path, "empty.sqlite")).query({})["as_of_seq"] == 0


def test_get_returns_the_causation_chain(tmp_path):
    box, req = _populated(tmp_path)
    r = bq.BlackBoxReader(box)
    resp = r.query({"type": "ORDER_PLACE_RESPONSE"})["records"][0]
    ev = r.get(resp["event_id"], chain_depth=3)
    assert [c["event_id"] for c in ev["causation_chain"]] == [req]
    assert r.get("non-esiste") is None
    with pytest.raises(bq.QueryError):
        r.get(req, chain_depth=bq.MAX_CHAIN_DEPTH + 1)


def test_gaps_in_range_are_reported_not_reconstructed(tmp_path):
    a = _box(tmp_path, session_id="s1")
    a.start_session(); a.close()
    b = _box(tmp_path, session_id="s2"); b.start_session()
    out = bq.BlackBoxReader(b).query({})
    assert len(out["gaps_in_range"]) == 1 and out["gaps_in_range"][0]["outcome"] == "UNKNOWN"


def test_request_hook_records_attempt_and_result_and_coalesces_a_flood(tmp_path):
    box = _box(tmp_path)
    t = [0.0]
    r = bq.BlackBoxReader(box, clock=lambda: t[0])
    rec = {"route": "/v1/blackbox/events", "params": {"limit": 10}, "status": 200, "code": "OK",
           "request_id": "req-1", "duration_ms": 3}
    assert r.request_hook({**rec, "phase": "ATTEMPT"}) and r.request_hook({**rec, "phase": "RESULT"})
    assert r.request_hook({**rec, "phase": "BOH"}) is False
    for _ in range(100):                                          # raffica nello stesso istante
        assert r.request_hook({**rec, "phase": "ATTEMPT"})
    written = [x for x in _rows(box) if x["type"] == "CONTROL_API_REQUEST"]
    assert len(written) == int(bq.BURST)
    t[0] = 10.0
    r.request_hook({**rec, "phase": "RESULT"})
    last = json.loads(_rows(box)[-1]["data"])
    assert last["coalesced_before"] == 102 - int(bq.BURST)
    assert all(x["source"] == "CONTROL_API" for x in written)


def test_request_hook_returns_false_when_the_store_is_unavailable(tmp_path):
    box = _box(tmp_path)
    _break(box)
    assert bq.BlackBoxReader(box).request_hook({"phase": "ATTEMPT", "route": "/v1/x", "request_id": "r"}) is False


def test_order_id_is_recorded_indexed_and_exposed_outside_only_as_a_stable_alias(tmp_path):
    """D1 (owner PF-BB1-IDENTITY-TIMESTAMP-SESSION-20261109): l'order_id del ledger e' un intero progressivo
    (`orders.id`): fuori da Pickfair esce solo l'alias `ord_<hmac>`, stabile ai riavvii e non enumerabile;
    dall'esterno si filtra per alias; la vista interna conserva id e alias."""
    from core import blackbox_query as q
    path = str(tmp_path / "b.sqlite")
    box = bb.BlackBox(path).open()
    box.start_session()
    box.record("ORDER_PLACE_REQUEST", source="STRATEGY", phase="ATTEMPT", outcome="PENDING", order_id=42,
               order_ref="cust-ref-riservato")
    box.record("ORDER_PLACE_REQUEST", source="STRATEGY", phase="ATTEMPT", outcome="PENDING", order_id=43)
    idx = [r[1] for r in box._conn.execute("PRAGMA index_list(events)")]
    assert "ix_events_order_id" in idx and "ix_events_order_alias" in idx
    alias42 = box.order_alias(42)
    assert alias42.startswith("ord_") and len(alias42) == 36 and "42" not in alias42[4:6]
    r = q.BlackBoxReader(box)
    ext = r.query({"order_id": alias42})["records"]
    assert [e["order_id"] for e in ext] == [alias42] and "order_alias" not in ext[0] and "order_ref" not in ext[0]
    assert r.query({"order_id": "42"})["records"] == [], "l'id interno non e' un filtro esterno"
    internal = r.query({"order_id": "42"}, external=False)["records"]
    assert internal[0]["order_id"] == "42" and internal[0]["order_alias"] == alias42
    box.close()
    again = bb.BlackBox(path).open()
    assert again.order_alias(42) == alias42, "alias stabile dopo il riavvio"
    assert bb.BlackBox(str(tmp_path / "altro.sqlite")).open().order_alias(42) != alias42, "chiave per registro"
    assert again.verify()["ok"]
    with pytest.raises(ValueError):
        again.record("ORDER_PLACE_REQUEST", source="STRATEGY", phase="ATTEMPT", outcome="PENDING", order_alias="x")


def test_get_and_uuid_filters_accept_the_dashed_uuid_form(tmp_path):
    from core import blackbox_query as q
    box = bb.BlackBox(str(tmp_path / "b.sqlite")).open()
    box.start_session()
    eid = box.record("API_REQUEST", source="CONTROL_API", phase="RESULT", outcome="VERIFIED",
                     correlation_id="0123456789abcdef0123456789abcdef")
    dashed = f"{eid[:8]}-{eid[8:12]}-{eid[12:16]}-{eid[16:20]}-{eid[20:]}".upper()
    r = q.BlackBoxReader(box)
    assert r.get(dashed)["event_id"] == eid
    assert r.query({"correlation_id": "01234567-89ab-cdef-0123-456789abcdef"})["records"][0]["event_id"] == eid
    assert r.get("non-un-uuid") is None


def test_dashed_and_compact_uuid_are_one_identifier_on_write_too(tmp_path):
    """Owner: le due forme dell'uuid devono risolversi allo stesso id interno, senza duplicati."""
    from core import blackbox_query as q
    box = bb.BlackBox(str(tmp_path / "b.sqlite")).open()
    box.start_session()
    dashed = "ABCDEF01-2345-6789-ABCD-EF0123456789"
    eid = box.record("API_REQUEST", source="CONTROL_API", phase="RESULT", outcome="VERIFIED", event_id=dashed,
                     causation_id="00000000-0000-0000-0000-000000000001")
    assert eid == "abcdef0123456789abcdef0123456789"
    with pytest.raises(Exception):                                     # stesso id nell'altra forma: nessun duplicato
        box.record("API_REQUEST", source="CONTROL_API", phase="RESULT", outcome="VERIFIED", critical=True,
                   event_id="abcdef0123456789abcdef0123456789")
    r = q.BlackBoxReader(box)
    assert r.get(dashed)["event_id"] == r.get(eid)["event_id"] == eid
    assert r.query({"causation_id": "00000000000000000000000000000001"})["records"][0]["event_id"] == eid
    assert len(r.query({"type": "API_REQUEST"})["records"]) == 1


def test_external_view_never_exposes_the_customer_reference_at_any_depth(tmp_path):
    """Analisi preventiva BB-1: il riferimento cliente non esce dalla vista esterna nemmeno se annidato in `data` o
    presente in `values_*`, con qualunque variante del nome; la vista interna lo conserva."""
    from core import blackbox_query as q
    box = bb.BlackBox(str(tmp_path / "b.sqlite")).open()
    box.start_session()
    box.record("ORDER_PLACE_REQUEST", source="STRATEGY", phase="ATTEMPT", outcome="PENDING",
               data={"req": {"customer_ref": "CREF-NESTED", "items": [{"Customer-Order-Ref": "CREF-LIST"}]}},
               values_after={"customerOrderRef": "CREF-VALUES", "stake": 2})
    r = q.BlackBoxReader(box)
    ext = json.dumps(r.query({})["records"])
    for secret in ("CREF-NESTED", "CREF-LIST", "CREF-VALUES"):
        assert secret not in ext
    assert '"stake": 2' in ext
    assert "CREF-NESTED" in json.dumps(r.query({}, external=False)["records"])


def test_request_hook_records_attempt_and_result_with_returned_and_keeps_non_uuid_request_ids(tmp_path):
    """Contratto MCP 1.15.0: ATTEMPT e RESULT registrati; `returned` nel RESULT; un request_id non uuid
    (`mcp-<24 hex>`) resta com'e', senza cifre inventate."""
    from core import blackbox_query as q
    box = bb.BlackBox(str(tmp_path / "b.sqlite")).open()
    box.start_session()
    r = q.BlackBoxReader(box)
    rid = "mcp-0123456789abcdef01234567"
    assert r.request_hook({"phase": "ATTEMPT", "route": "/v1/blackbox/events", "request_id": rid})
    assert r.request_hook({"phase": "RESULT", "route": "/v1/blackbox/events", "status": 200, "returned": 13,
                           "request_id": rid})
    recs = r.query({"type": "CONTROL_API_REQUEST"})["records"]
    assert [(e["phase"], e["outcome"]) for e in recs] == [("ATTEMPT", "PENDING"), ("RESULT", "VERIFIED")]
    assert all(e["correlation_id"] == rid and e["data"]["request_id"] == rid for e in recs)
    assert recs[1]["data"]["returned"] == 13



@pytest.mark.parametrize("bad", ["2026-11-09T12:34:56Z", "2026-11-09T12:34:56.123Z", "2026-11-09 12:34:56.123456Z",
                                 "2026-11-09T12:34:56.123456+01:00", "2026-02-30T12:34:56.123456Z",
                                 "2026-11-09T24:00:00.000000Z", "9/11/2026", ""])
def test_timestamps_must_be_canonical_utc_rfc3339_microseconds(tmp_path, bad):
    """D2: ts_utc ed exchange_ts forniti dal chiamante solo come YYYY-MM-DDTHH:MM:SS.ffffffZ validi."""
    box = bb.BlackBox(str(tmp_path / "b.sqlite")).open()
    for field in ("ts_utc", "exchange_ts"):
        with pytest.raises(ValueError):
            box.record("API_REQUEST", source="SYSTEM", **{field: bad})


def test_exchange_time_is_distinct_and_never_invented(tmp_path):
    """D2: l'ora dell'exchange resta separata dall'ora osservata; se manca e' null (mai inventata); l'ordine fa fede
    su seq, non sull'orario (anche con un orario osservato fornito fuori ordine o a cavallo di mezzanotte)."""
    from core import blackbox_query as q
    box = bb.BlackBox(str(tmp_path / "b.sqlite")).open()
    box.start_session()
    box.record("ORDER_MATCHED", source="EXCHANGE", phase="OBSERVATION", outcome="VERIFIED",
               ts_utc="2026-11-10T00:00:00.000001Z", exchange_ts="2026-11-09T23:59:59.999999Z")
    box.record("ORDER_MATCHED", source="EXCHANGE", phase="OBSERVATION", outcome="VERIFIED",
               ts_utc="2026-11-09T23:59:59.000000Z")
    recs = [e for e in q.BlackBoxReader(box).query({"type": "ORDER_MATCHED"})["records"]]
    assert [e["seq"] for e in recs] == sorted(e["seq"] for e in recs)
    assert recs[0]["exchange_ts"] == "2026-11-09T23:59:59.999999Z" and recs[0]["ts_utc"] == "2026-11-10T00:00:00.000001Z"
    assert recs[1]["exchange_ts"] is None and "exchange_ts" in recs[1]["not_recorded"]
    t = bb.utc_iso(1762732799.999999)
    assert bb.check_utc_ts(t, "ts_utc") == t


def test_a_second_financial_runtime_on_the_same_register_is_refused(tmp_path):
    """D3: un solo runtime per registro. Il secondo riceve RuntimeAlreadyActive con motivo esplicito; i lettori non
    prendono il lock; chiuso il primo, un nuovo runtime parte."""
    from core import blackbox_query as q
    path = str(tmp_path / "b.sqlite")
    first = bb.BlackBox(path).open()
    first.start_session()
    second = bb.BlackBox(path).open()
    with pytest.raises(bb.RuntimeAlreadyActive, match="RUNTIME_ALREADY_ACTIVE"):
        second.start_session()
    reader = q.BlackBoxReader(bb.BlackBox(path))
    assert reader.status()["as_of_seq"] >= 1, "un lettore non e' un runtime e non viene rifiutato"
    first.end_session("STOP"); first.close()
    third = bb.BlackBox(path).open()
    third.start_session()
    types = [r["type"] for r in third._conn.execute("SELECT type FROM events ORDER BY seq")]
    assert types[-1] == "PROCESS_START" and "UNCLEAN_TERMINATION" not in types
