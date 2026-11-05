"""MCP-01i: stato della chiusura SL/TP (F-04) in ``GET /v1/exposure`` sulla catena REALE di F-04.

Catena e scenari presi dai test di accettazione di F-04 (Claude D): Roserpina con 100 EUR, tre tavoli sulla
stessa partita, SL del tavolo A instradato, SL del tavolo C (solo non abbinato) non eseguito. La vista legge
solo ciò che il core ha persistito; il ``customer_ref`` e i nomi degli eventi non escono mai.
"""

from __future__ import annotations

import builtins
import hashlib
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from control_api.views import exposure_view
from core import sl_tp_close
from tests.acceptance.test_f04_sl_tp_table_close import _chain, _three_tables, _trigger


def closes(rc: Any) -> dict[str, Any]:
    return exposure_view(rc)["sl_tp_closes"]


def test_no_close_yet(tmp_path: Path) -> None:
    c = _chain(tmp_path)
    _three_tables(c)
    assert closes(c.rc) == {"state": "OK", "reason": None, "closes": [], "omitted": 0, "truncated": False}


def test_routed_stop_loss_closes_only_table_a(tmp_path: Path) -> None:
    c = _chain(tmp_path)
    a, b, tc = _three_tables(c)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
    record = json.loads(c.db.get_settings()[sl_tp_close.CLOSE_KEY_PREFIX + a.meta["customer_ref"]])
    view = closes(c.rc)
    assert view["state"] == "OK" and len(view["closes"]) == 1
    item = view["closes"][0]
    assert (item["table_id"], item["market_id"], item["selection_id"], item["side"]) == (
        a.table_id, str(a.market_id), str(a.selection_id), "BACK")
    assert (item["trigger"], item["close_state"], item["retryable"]) == ("STOP_LOSS", "ROUTED", False)
    assert item["bet_ids"] == record["bet_ids"] and item["detail"] is None
    assert item["recorded_at"] == pytest.approx(record["at"])
    assert item["position"]["orders"] >= 1 and item["position"]["unreadable"] == 0
    hedges = item["hedges"]
    assert hedges["requests"] == len(record["cashout_request_ids"]) == 1  # l'hedge della sola posizione A
    rows = [r for r in c.db._execute("SELECT status, payload_json FROM orders", fetch=True)
            if json.loads(r["payload_json"] or "{}").get("cashout_request_id")
            in record["cashout_request_ids"]]
    assert len(rows) == 1 and hedges["not_in_ledger"] == 0
    key = {"HEDGE_PENDING": "pending", "HEDGE_PLACED": "placed", "HEDGE_UNMATCHED": "unmatched",
           "HEDGE_AMBIGUOUS": "ambiguous", "HEDGE_FAILED": "failed"}[rows[0]["status"]]
    assert hedges[key] == 1 and sum(hedges[k] for k in ("pending", "placed", "unmatched", "ambiguous",
                                                         "failed")) == 1
    table = c.rc.table_manager.get_table(a.table_id)
    holds = table.status in ("ACTIVE", "RECOVERY") and table.meta.get("customer_ref") == a.meta["customer_ref"]
    assert item["residual_source"] == ("TABLE" if holds else "TABLE_RELEASED")
    text = json.dumps(exposure_view(c.rc))
    for ref in (a.meta["customer_ref"], b.meta["customer_ref"], tc.meta["customer_ref"]):
        assert ref not in text
    assert "Alpha" not in text and "event_id" not in text


def test_not_executed_close_is_retryable_with_its_reason(tmp_path: Path) -> None:
    c = _chain(tmp_path)
    _a, _b, tc = _three_tables(c)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(tc))  # solo non abbinato: nessun hedge pubblicato
    item = closes(c.rc)["closes"][0]
    record = json.loads(c.db.get_settings()[sl_tp_close.CLOSE_KEY_PREFIX + tc.meta["customer_ref"]])
    assert record["state"] == sl_tp_close.STATE_NOT_EXECUTED
    assert (item["close_state"], item["retryable"]) == ("NOT_EXECUTED", True)
    assert item["detail"] is not None and item["detail"] == item["detail"].upper()
    assert item["hedges"] == {"requests": 0, "pending": 0, "placed": 0, "unmatched": 0, "ambiguous": 0,
                              "failed": 0, "not_in_ledger": 0}


def test_take_profit_and_unknown_values(tmp_path: Path) -> None:
    c = _chain(tmp_path)
    a, b, _tc = _three_tables(c)
    c.bus.publish(sl_tp_close.TP_TRIGGERED, _trigger(a, sl_tp_close.TP_TRIGGERED, pnl=2.0))
    key = sl_tp_close.CLOSE_KEY_PREFIX + b.meta["customer_ref"]
    rec = json.loads(sl_tp_close.state_record(
        sl_tp_close.parse_trigger(sl_tp_close.SL_TRIGGERED, _trigger(b))[0], "SOMETHING_NEW",
        bet_ids=["999"], at=time.time()))
    rec["trigger"] = "WEIRD"
    c.db.save_settings({key: json.dumps(rec)})
    by_table = {i["table_id"]: i for i in closes(c.rc)["closes"]}
    assert by_table[a.table_id]["trigger"] == "TAKE_PROFIT"
    assert (by_table[b.table_id]["trigger"], by_table[b.table_id]["close_state"]) == ("UNKNOWN", "UNKNOWN")
    assert by_table[b.table_id]["retryable"] is False


def test_unreadable_record_is_counted_never_hidden(tmp_path: Path) -> None:
    c = _chain(tmp_path)
    a, b, _tc = _three_tables(c)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
    c.db.save_settings({sl_tp_close.CLOSE_KEY_PREFIX + "broken": "{not json"})
    c.db.save_settings({sl_tp_close.CLOSE_KEY_PREFIX + "other": json.dumps({"customer_ref": "mismatch"})})
    view = closes(c.rc)
    assert (view["state"], view["reason"], view["omitted"]) == ("PARTIAL", "RECORDS_UNREADABLE", 2)
    assert len(view["closes"]) == 1


def test_without_f04_the_section_is_unavailable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    c = _chain(tmp_path)
    real_import = builtins.__import__

    def no_f04(name: str, globals_: Any = None, locals_: Any = None, fromlist: Any = (), level: int = 0) -> Any:
        if name == "core" and fromlist and "sl_tp_close" in fromlist:
            raise ImportError("sl_tp_close")
        return real_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", no_f04)
    assert closes(c.rc) == {"state": "UNAVAILABLE", "reason": "CORE_PENDING_F04"}


def test_tables_lock_held_makes_residual_unavailable(tmp_path: Path) -> None:
    c = _chain(tmp_path)
    a, _b, _tc = _three_tables(c)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
    held, release = threading.Event(), threading.Event()

    def holder() -> None:
        with c.rc.table_manager.locked():
            held.set()
            release.wait(10)

    t = threading.Thread(target=holder, daemon=True)
    t.start()
    assert held.wait(5)
    try:
        item = closes(c.rc)["closes"][0]
    finally:
        release.set()
        t.join(5)
    assert (item["residual_source"], item["residual_exposure"]) == ("UNAVAILABLE", None)
    assert item["close_state"] == "ROUTED"  # il record persistito resta leggibile


def test_reads_leave_the_database_untouched(tmp_path: Path) -> None:
    c = _chain(tmp_path)
    a, _b, _tc = _three_tables(c)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))

    def digest() -> str:
        h = hashlib.sha256()
        for p in sorted(tmp_path.glob("f04.db*")):
            if not p.name.endswith("-shm"):
                h.update(p.name.encode() + p.read_bytes())
        conn = sqlite3.connect(f"file:{tmp_path / 'f04.db'}?mode=ro", uri=True)
        try:
            for line in conn.iterdump():
                h.update(line.encode())
        finally:
            conn.close()
        return h.hexdigest()

    before, events = digest(), len(c.bus.events)
    for _ in range(20):
        exposure_view(c.rc)
    assert digest() == before and len(c.bus.events) == events


def test_view_passes_the_server_contract(tmp_path: Path) -> None:
    from pickfair_control_api.contract import response_schema
    from pickfair_control_api.schema_check import validate

    c = _chain(tmp_path)
    a, _b, tc = _three_tables(c)
    validate(exposure_view(c.rc), response_schema("exposure"))
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(tc))
    validate(exposure_view(c.rc), response_schema("exposure"))


def test_hedges_link_only_by_request_id(tmp_path: Path) -> None:
    c = _chain(tmp_path)
    a, b, _tc = _three_tables(c)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(a))  # hedge reale di A
    key = sl_tp_close.CLOSE_KEY_PREFIX + b.meta["customer_ref"]
    trig = sl_tp_close.parse_trigger(sl_tp_close.SL_TRIGGERED, _trigger(b))[0]
    # record di B con una richiesta mai scritta nel ledger, e la stessa selezione di nessun hedge
    c.db.save_settings({key: sl_tp_close.state_record(trig, sl_tp_close.STATE_UNCERTAIN, bet_ids=["777"],
                                                      at=time.time(), request_ids=["req-missing"])})
    by_table = {i["table_id"]: i for i in closes(c.rc)["closes"]}
    hb = by_table[b.table_id]["hedges"]
    assert (hb["requests"], hb["not_in_ledger"]) == (1, 1)
    assert sum(hb[k] for k in ("pending", "placed", "unmatched", "ambiguous", "failed")) == 0
    # forma precedente di F-04 (senza cashout_request_ids): legame non dimostrabile
    rec = json.loads(c.db.get_settings()[key])
    rec.pop("cashout_request_ids")
    c.db.save_settings({key: json.dumps(rec)})
    assert {i["table_id"]: i for i in closes(c.rc)["closes"]}[b.table_id]["hedges"] is None


def test_record_of_another_position_is_never_attributed(tmp_path: Path) -> None:
    c = _chain(tmp_path)
    _a, b, _tc = _three_tables(c)
    trig = sl_tp_close.parse_trigger(sl_tp_close.SL_TRIGGERED, _trigger(b))[0]
    # record completo e valido di B, ma salvato sotto la chiave di un'altra posizione
    c.db.save_settings({sl_tp_close.CLOSE_KEY_PREFIX + "someone-else": sl_tp_close.state_record(
        trig, sl_tp_close.STATE_ROUTED, bet_ids=["1"], at=time.time(), request_ids=[])})
    view = closes(c.rc)
    assert (view["state"], view["omitted"], view["closes"]) == ("PARTIAL", 1, [])


def test_residual_only_from_the_table_that_still_holds_the_position(tmp_path: Path) -> None:
    c = _chain(tmp_path)
    _a, _b, tc = _three_tables(c)
    c.bus.publish(sl_tp_close.SL_TRIGGERED, _trigger(tc))  # NOT_EXECUTED: il tavolo C resta occupato
    table = c.rc.table_manager.get_table(tc.table_id)
    assert table.status == "ACTIVE" and table.meta["customer_ref"] == tc.meta["customer_ref"]
    item = closes(c.rc)["closes"][0]
    assert item["residual_source"] == "TABLE"
    assert item["residual_exposure"] == format(table.current_exposure, "g") or float(
        item["residual_exposure"]) == pytest.approx(table.current_exposure)
    # tavolo liberato e riassegnato a un'altra posizione (API del TableManager)
    c.rc.table_manager.force_unlock(tc.table_id)
    c.rc.table_manager.activate(table_id=tc.table_id, event_key="other-event", exposure=9.0,
                                market_id="1.999", selection_id="99", meta={"customer_ref": "reassigned-ref"})
    item = closes(c.rc)["closes"][0]
    assert (item["residual_source"], item["residual_exposure"]) == ("TABLE_RELEASED", None)
