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
