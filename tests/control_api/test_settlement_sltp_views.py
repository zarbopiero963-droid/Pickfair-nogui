"""MCP-01h: SL/TP per tavolo, settlement e barriere di cashout su un RuntimeController REALE.

SL/TP: sul `main` nessun percorso arma SL/TP; il test usa anche la forma esatta che F-02 di Claude D
(`keep/f02-sl-tp-path-20261103`, `_sl_tp_meta`) scrive nel `meta` del tavolo, così il contratto è
provato compatibile prima dell'integrazione. Settlement: solo conteggi, nessun payload o riferimento.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from control_api.views import exposure_view, reconciliation_view, status_view
from core.runtime_controller import RuntimeController


def tables(rc: RuntimeController) -> dict[int, dict[str, Any]]:
    return {t["table_id"]: t for t in exposure_view(rc)["tables"]}


def activate(rc: RuntimeController, table_id: int, meta: dict[str, Any]) -> None:
    rc.table_manager.activate(table_id=table_id, event_key="Juventus v Inter", exposure=10.0,
                              market_id="1.234567", selection_id="47972",
                              meta={"event_name": "Juventus v Inter", "customer_ref": "cref-x",
                                    **meta})


# ------------------------------------------------------------------ SL/TP


def test_tables_on_main_are_not_armed(runtime: RuntimeController) -> None:
    activate(runtime, 1, {"bet_type": "BACK", "price": 2.0})  # meta come lo scrive il main
    t = tables(runtime)[1]["sl_tp"]
    assert t == {"state": "NOT_ARMED", "stop_loss": None, "take_profit": None,
                 "stop_loss_pct": None, "take_profit_pct": None, "trailing_stop_pct": None}


def test_f02_meta_shape_is_reported_as_armed(runtime: RuntimeController) -> None:
    # forma di RuntimeController._sl_tp_meta di F-02: importi al centesimo, percentuali e stake
    activate(runtime, 2, {"stake": 10.0, "stop_loss_pct": 25.0, "stop_loss": 2.5,
                          "take_profit_pct": 50.0, "take_profit": 5.0, "trailing_stop_pct": 3.0})
    t = tables(runtime)[2]["sl_tp"]
    assert t == {"state": "ARMED", "stop_loss": "2.5", "take_profit": "5",
                 "stop_loss_pct": "25", "take_profit_pct": "50", "trailing_stop_pct": "3"}
    text = json.dumps(exposure_view(runtime))
    assert "Juventus" not in text and "cref-x" not in text  # solo le chiavi SL/TP del meta


def test_triggered_and_unreadable_values(runtime: RuntimeController) -> None:
    activate(runtime, 1, {"stop_loss": 2.5, "sl_tp_triggered": True})
    activate(runtime, 2, {"take_profit": float("nan")})
    activate(runtime, 3, {"stop_loss": -1.0})
    activate(runtime, 4, {"sl_tp_triggered": True})  # scattato ma senza importi: non dimostrabile
    t = tables(runtime)
    assert t[1]["sl_tp"]["state"] == "TRIGGERED" and t[1]["sl_tp"]["stop_loss"] == "2.5"
    assert t[2]["sl_tp"]["state"] == "UNKNOWN" and t[2]["sl_tp"]["take_profit"] is None
    assert t[3]["sl_tp"]["state"] == "UNKNOWN"
    assert t[4]["sl_tp"]["state"] == "UNKNOWN"


# ------------------------------------------------------------------ barriere di cashout


def test_cashout_barriers_are_counted(runtime: RuntimeController) -> None:
    assert status_view(runtime)["engine"]["queues"]["cashout_barriers"] == 0
    runtime._cashout_barriers = {1: frozenset({("global",)}), 2: frozenset({("m", "1.2")})}
    assert status_view(runtime)["engine"]["queues"]["cashout_barriers"] == 2


# ------------------------------------------------------------------ settlement


def _insert_effect(rc: RuntimeController, key: str, outcome: str, realized: int) -> None:
    rc.db._execute(
        "INSERT INTO settlement_effects(settlement_key, outcome, table_id, event_key, customer_ref, "
        "reservation_id, bet_id, market_id, pnl, instance_id, realized_applied, recorded_at, "
        "payload_json) VALUES (?, ?, 1, 'Juventus v Inter', 'cref-secret', 'r', 'b', '1.2', 1.5, "
        "'i', ?, 1, '{\"secret\": 1}')", (key, outcome, realized))


def _insert_checkpoint(rc: RuntimeController, key: str, stage: str, submit: str,
                       ambiguous: int = 0) -> None:
    rc.db._execute(
        "INSERT INTO cycle_recovery_checkpoints(settlement_key, settlement_correlation_id, cycle_id, "
        "table_id, strategy_context_json, checkpoint_stage, next_trade_submission_status, reason, "
        "is_ambiguous, created_at, updated_at) VALUES (?, 'Juventus v Inter', 'c', 1, '{}', ?, ?, "
        "'free text reason', ?, 1, 1)", (key, stage, submit, ambiguous))


def test_settlement_counts_from_the_core_tables(runtime: RuntimeController) -> None:
    rc = runtime
    _insert_effect(rc, "s1", "held", 0)
    _insert_effect(rc, "s2", "applied", 1)
    _insert_effect(rc, "s3", "applied", 0)
    _insert_checkpoint(rc, "s1", "NEXT_TRADE_SUBMIT_CONFIRMED", "CONFIRMED")
    _insert_checkpoint(rc, "s2", "CYCLE_AMBIGUOUS", "AMBIGUOUS", ambiguous=1)
    _insert_checkpoint(rc, "s3", "SOMETHING_NEW", "NOT_ATTEMPTED")
    s = reconciliation_view(rc)["settlement"]
    assert s["effects"] == {"held": 1, "not_realized": 2, "by_outcome": [
        {"outcome": "APPLIED", "count": 2}, {"outcome": "HELD", "count": 1}]}
    assert s["checkpoints"] == {"by_stage": [
        {"stage": "CYCLE_AMBIGUOUS", "count": 1}, {"stage": "NEXT_TRADE_SUBMIT_CONFIRMED", "count": 1},
        {"stage": "UNKNOWN", "count": 1}], "cycle_ambiguous": 1, "submit_ambiguous": 1}
    text = json.dumps(reconciliation_view(rc))
    for secret in ("cref-secret", "Juventus", "free text", "secret"):
        assert secret not in text


def test_in_memory_settlement_structures(runtime: RuntimeController) -> None:
    s = reconciliation_view(runtime)["settlement"]
    assert (s["unrecorded_settlements"], s["held_batches"]) == (0, 0)  # create alla prima scrittura
    runtime._unrecorded_settlements = {"k1": {"payload": {"secret": 1}, "pnl": 1.0}}
    runtime._held_batch_ids = {"b-1", "b-2"}
    s = reconciliation_view(runtime)["settlement"]
    assert (s["unrecorded_settlements"], s["held_batches"]) == (1, 2)
    assert "secret" not in json.dumps(s)


def test_sim_settlements_only_with_a_simulation_broker(runtime: RuntimeController,
                                                       sim_runtime: Any) -> None:
    s = reconciliation_view(sim_runtime)["settlement"]
    assert s["sim_settlements"] == len(sim_runtime.betfair_service.simulation_broker.state.settlements)
    sim_runtime.betfair_service.simulation_mode = False  # LIVE: nessun registro in memoria
    s = reconciliation_view(sim_runtime)["settlement"]
    assert s["sim_settlements"] is None and "sim_settlements" in s["unavailable"]


def test_settlement_lock_held_makes_the_section_unavailable(runtime: RuntimeController) -> None:
    held, release = threading.Event(), threading.Event()

    def holder() -> None:
        with runtime._settlement_lock:
            held.set()
            release.wait(10)

    t = threading.Thread(target=holder, daemon=True)
    t.start()
    assert held.wait(5)
    try:
        started = time.monotonic()
        view = reconciliation_view(runtime)
        elapsed = time.monotonic() - started
    finally:
        release.set()
        t.join(timeout=5)
    assert view["settlement"] is None and "settlement" in view["unavailable"]
    assert elapsed < 2.0


def test_database_unreadable_parts_are_declared(runtime: RuntimeController,
                                                monkeypatch: pytest.MonkeyPatch) -> None:
    from control_api import views

    def broken(_rc: Any) -> Any:
        raise views._SnapshotMissing("UNAVAILABLE", "DB_UNREADABLE")

    monkeypatch.setattr(views, "_ro_connection", broken)
    s = reconciliation_view(runtime)["settlement"]
    assert s["effects"] is None and s["checkpoints"] is None
    assert {"effects", "checkpoints"} <= set(s["unavailable"])


def test_settlement_reads_leave_the_database_untouched(runtime: RuntimeController,
                                                       tmp_path: Path) -> None:
    _insert_effect(runtime, "s1", "held", 0)

    def digest() -> str:
        h = hashlib.sha256()
        for p in sorted(tmp_path.glob("pickfair.db*")):
            if not p.name.endswith("-shm"):  # indice WAL: marcatori di lettura, nessun dato
                h.update(p.name.encode() + p.read_bytes())
        conn = sqlite3.connect(f"file:{tmp_path / 'pickfair.db'}?mode=ro", uri=True)
        try:
            for line in conn.iterdump():
                h.update(line.encode())
        finally:
            conn.close()
        return h.hexdigest()

    before = digest()
    for _ in range(20):
        reconciliation_view(runtime)
    assert digest() == before


def test_views_pass_the_server_contract(sim_runtime: Any) -> None:
    from pickfair_control_api.contract import response_schema
    from pickfair_control_api.schema_check import validate

    activate(sim_runtime, 1, {"stop_loss": 2.5, "take_profit": 5.0})
    validate(exposure_view(sim_runtime), response_schema("exposure"))
    validate(reconciliation_view(sim_runtime), response_schema("reconciliation_status"))
    validate(status_view(sim_runtime), response_schema("status"))
