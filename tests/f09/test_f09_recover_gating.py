"""F-09 (#586, RST-01/REC-08) — l'esito della fase recovery/reconcile all'avvio blocca l'ACTIVE (RED prima della patch).

Prima `RuntimeController.start()` registrava solo nel log l'esito di `StateRecovery.recover()` e della riconciliazione
dei batch aperti: `ok=False` (ghost non riconciliati, F-08), un'eccezione o un batch fallito lasciavano partire il
runtime in ACTIVE e ammettere nuovi ingressi. Atteso (#506 RST-001, #514 D-B): stessa semantica di F018 =>
PAUSED + RECONCILIATION_REQUIRED, `startup_mode` nel payload `STARTUP_POSITIONS_REBUILT`, e `resume()` non lo
scavalca: `recover()` ripubblica le saghe pendenti (`RECOVER_ORDER`), quindi non si ritenta in un resume ma serve un
riavvio con recovery riuscito (come RECOVERY_STATE_UNREADABLE).
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "f026"))

pytestmark = [pytest.mark.safety]


def _start(fault):
    from test_f026_financial import run
    return run([{"op": "signal", "m": "1.901", "price": 2.0}], startup_recovery_fault=fault)


def _orders_on(r, market):
    return [o for o in r["snaps"][-1]["orders"] if o["market_id"] == market]


def test_recover_not_ok_pauses_runtime():
    r = _start("not_ok")
    assert r["mode_after_start"] == "PAUSED", r["rebuild"]
    assert r["rebuild"]["ok"] is False
    assert r["rebuild"]["reason"] == "STATE_RECOVERY_NOT_OK:STARTUP_RECONCILIATION_REQUIRED", r["rebuild"]
    assert r["rebuild"]["startup_mode"] == "BLOCKED"
    assert _orders_on(r, "1.901") == [], "nuovo ingresso con recovery non riuscito"


def test_recover_exception_pauses_runtime():
    r = _start("raise")
    assert r["mode_after_start"] == "PAUSED", r["rebuild"]
    assert r["rebuild"]["ok"] is False and r["rebuild"]["reason"] == "STATE_RECOVERY_ERROR:RuntimeError", r["rebuild"]
    assert _orders_on(r, "1.901") == []


def test_open_batch_reconciliation_failure_pauses_runtime():
    # REC-08: un batch aperto non riconciliato all'avvio non lascia proseguire in ACTIVE
    r = _start("batches_failed")
    assert r["mode_after_start"] == "PAUSED", r["rebuild"]
    assert r["rebuild"]["ok"] is False and r["rebuild"]["reason"] == "OPEN_BATCH_RECONCILIATION_FAILED:1", r["rebuild"]
    assert _orders_on(r, "1.901") == []


def test_single_batch_not_ok_without_exception_pauses_runtime():
    # Codex P1 su f21943f: reconcile_all_open_batches reale; reconcile_batch ok=False senza sollevare lascia il
    # riepilogo a ok=True / failed_count=0, ma il batch NON e' riconciliato
    r = _start("batch_result_not_ok")
    assert r["mode_after_start"] == "PAUSED", r["rebuild"]
    assert r["rebuild"]["ok"] is False and r["rebuild"]["reason"] == "OPEN_BATCH_RECONCILIATION_FAILED:1", r["rebuild"]
    assert _orders_on(r, "1.901") == []


@pytest.mark.parametrize("summary,expected", [
    ({"ok": True, "failed_count": 0, "reconciled": [{"ok": True}, {"ok": True, "reason_code": "IDEMPOTENT_SKIP"}]}, 0),
    ({"ok": True, "failed_count": 0, "reconciled": [{"ok": True}, {"ok": False, "error": "BATCH_NOT_FOUND"}]}, 1),
    ({"ok": False, "failed_count": 2, "reconciled": [{"ok": False}]}, 3),
    ({"ok": True, "failed_count": 0, "reconciled": ["x", None]}, 2),
    ({"ok": False, "failed_count": 0, "reconciled": []}, 1),
    ({"ok": True, "failed_count": "?", "reconciled": []}, 1),
    ({"ok": True, "reconciled": "rotto"}, "dict"),
    (None, "NoneType"),
])
def test_startup_batch_failures_counts_every_unreconciled_batch(summary, expected):
    from core.runtime_controller import RuntimeController
    assert RuntimeController._startup_batch_failures(summary) == expected


def test_successful_recovery_still_starts_active_with_startup_mode():
    r = _start(None)
    assert r["mode_after_start"] == "ACTIVE", r["rebuild"]
    assert r["rebuild"]["ok"] is True and r["rebuild"]["startup_mode"] == "LIVE_FLAT", r["rebuild"]


def _paused_controller(failure):
    from core.runtime_controller import RuntimeController, RuntimeMode
    rc = RuntimeController.__new__(RuntimeController)
    rc.mode = RuntimeMode.PAUSED
    rc.bus = SimpleNamespace(publish=lambda *a, **k: None)
    rc._reconciliation_required = True
    rc._startup_recovery_failure = failure
    rc._monitor_daily_loss_breach = lambda **k: {"breached": False}
    rc._loss_block_reason = lambda: ""
    rc._recover_native_microstake = lambda: {"ok": True, "status": "RECONCILED", "pending": []}
    rc._rebuild_open_positions = lambda: {"ok": True, "reason": "", "rebuilt": [], "uncertain": [], "held": []}
    rc.get_status = lambda: {"mode": rc.mode.value}
    rc._safe_status_snapshot = lambda: {"mode": rc.mode.value}
    return rc, RuntimeMode


@pytest.mark.parametrize("failure", ["STATE_RECOVERY_NOT_OK:STARTUP_RECONCILIATION_REQUIRED",
                                     "STATE_RECOVERY_ERROR:RuntimeError", "OPEN_BATCH_RECONCILIATION_FAILED:1"])
def test_resume_does_not_bypass_a_failed_startup_recovery(failure):
    rc, mode = _paused_controller(failure)
    out = rc.resume()
    assert out["resumed"] is False and failure in out["reason"], out
    assert rc.mode is mode.PAUSED and rc._reconciliation_required is True
