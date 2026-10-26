"""PF-STAKE-PRECISION-MICROSTAKE-01 — prova end-to-end su HeadlessApp reale (SIM, SQLite su disco, crash veri).

Il difetto e' emerso dalla prima sessione integrata della Fase 4: dopo un settlement il money management chiedeva
1.9647599999999998 e l'ordine finiva FAILED/SUBMIT_FAILED (MICROSTAKE_PRE_SEND_BLOCK:invalid_size_precision). Qui la
catena intera: segnale -> Roserpina -> engine -> Microstake nativo SIM -> exchange SIM -> riconciliazione.
Il LIVE ha il suo adapter (core/microstake_live_port.py, RECTIFICATION-01), certificato offline in
tests/core/test_microstake_live_adapter.py; NON VERIFICATO su Betfair reale.
"""
import tempfile
from decimal import Decimal as D

from tests.f026.test_f026_financial import run

TABLES = {"roserpina.table_count": 10}


def _exch(s, m):
    return [o for o in s["exch_orders"] if o["market_id"] == m]


def _live(s, m):
    return [o for o in _exch(s, m) if o["status"] not in ("CANCELLED", "LAPSED")]


def _row(s, m):
    rows = [o for o in s["orders"] if o["market_id"] == m]
    assert len(rows) == 1, rows
    return rows[0]


def test_sub_minimum_stake_after_a_settlement_is_executed_floored():
    steps = [{"op": "signal", "m": f"1.52{i}", "price": 2.0, "event": f"E{i} v X"} for i in range(8)]
    steps += [{"op": "settle", "m": "1.520", "win": True}, {"op": "signal", "m": "1.529", "price": 2.0, "event": "Z v W"}]
    s = run(steps, settings=TABLES)["snaps"][-1]
    row = _row(s, "1.529")
    assert row["status"] == "SUBMITTED", f"prima: FAILED/SUBMIT_FAILED per precisione — {row}"
    assert D(str(row["stake"])) == D(str(row["stake"])).quantize(D("0.01")) and row["stake"] < 2.0
    (live,) = _live(s, "1.529")
    assert live["size"] == row["stake"] and live["matched"] == row["stake"]


def test_roserpina_generates_and_executes_a_ten_cent_microstake():
    s = run([{"op": "signal", "m": "1.531", "price": 2.0}],
            settings=dict(TABLES, **{"roserpina.target_profit_cycle_pct": 0.1}))["snaps"][-1]   # 100 * 0.1% / (2-1)
    row = _row(s, "1.531")
    assert row["status"] == "SUBMITTED" and row["stake"] == 0.1
    (live,) = _live(s, "1.531")
    assert live["size"] == 0.1 and live["matched"] == 0.1
    assert s["funds"]["exposure"] == 0.1 and s["funds"]["available"] == 99.9    # stub temporaneo rilasciato
    assert [t["exposure"] for t in s["tables"]] == [0.1]


def test_crash_right_after_the_microstake_request_never_duplicates_on_restart():
    home = tempfile.mkdtemp(prefix="precision_crash_")
    first = run([{"op": "crash_on_next_place"}, {"op": "signal", "m": "1.541", "price": 2.0, "stake": 0.10599}],
                home=home, settings=TABLES)
    assert str(first.get("crashed_at", "")).startswith("PLACE_BET:"), "precondizione: crash dopo l'invio"
    s = run([{"op": "wait", "s": 2}, {"op": "signal", "m": "1.541", "price": 2.0, "stake": 0.10599}],
            home=home, settings=TABLES)["snaps"][-1]
    live = _live(s, "1.541")
    assert len(live) == 1 and live[0]["size"] == 0.1, f"ordine duplicato o importo diverso dopo il riavvio: {live}"
    assert [(t["market_id"], t["exposure"]) for t in s["tables"]] == [("1.541", 0.1)]
    assert s["funds"]["exposure"] == 0.1


def test_partial_fill_keeps_floored_stake_reservation_and_ledger_coherent():
    s = run([{"op": "mode", "m": "1.551", "mode": "partial"},
             {"op": "signal", "m": "1.551", "price": 2.0, "stake": 3.08595}], settings=TABLES)["snaps"][-1]
    row = _row(s, "1.551")
    assert row["status"] == "SUBMITTED" and row["stake"] == 3.08 and row["matched"] == 1.0
    (live,) = _live(s, "1.551")
    assert live["size"] == 3.08 and live["matched"] == 1.0
    assert [t["exposure"] for t in s["tables"]] == [3.08]
    assert s["funds"]["exposure"] == 3.08 and s["funds"]["available"] == 96.92


def test_microstake_position_is_hedged_by_the_cashout():
    r = run([{"op": "signal", "m": "1.561", "price": 2.0, "stake": 0.10599}, {"op": "cashout", "m": "1.561"}],
            settings=TABLES)
    s = r["snaps"][-1]
    sides = sorted((o["side"], o["size"], o["matched"]) for o in _live(s, "1.561"))
    assert sides == [("BACK", 0.1, 0.1), ("LAY", 0.1, 0.1)], f"prima: nessun hedge (positions 0) — {sides}"
    assert any(e["e"] == "CASHOUT_SUCCESS" for e in r["events"])
