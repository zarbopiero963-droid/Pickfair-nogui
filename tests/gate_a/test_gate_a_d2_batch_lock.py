"""Decisione owner D2 (PF-OWNER-GATEA-FIVE-DECISIONS-20261111): un lock di riconciliazione non si rimuove mentre
un'operazione lo tiene, se cio' permette un accesso concorrente non protetto. ID: TRG-628-B-0975 /
PF-B1-R2-BATCH-CLEANUP (rettificata da D2: da NEEDS_OWNER_DECISION a CODE_DEFECT).

Su main c535c1d5 `cleanup_batch` (core/batch_lock_manager.py:63), chiamato da reconcile_batch mentre il lock e'
ancora tenuto (core/reconciliation_engine.py ~1982), toglie il lock dal registro: un secondo acquire crea un lock
nuovo e ottiene True. Il caso di difetto resta xfail(strict) fino alla correzione (B)."""
import threading

import pytest

from core.batch_lock_manager import _BatchLockManager


@pytest.mark.xfail(strict=True, reason="TRG-628-B-0975 / D2: cleanup_batch rimuove un lock ancora tenuto")
def test_trg_628_b_0975_cleanup_while_held_keeps_mutual_exclusion():
    """ID: TRG-628-B-0975 / PF-B1-R2-BATCH-CLEANUP"""
    mgr = _BatchLockManager()
    with mgr.acquire("B1") as first:
        assert first is True
        mgr.cleanup_batch("B1")                       # come fa reconcile_batch su stato terminale, lock tenuto
        result = {}

        def contender():
            with mgr.acquire("B1") as got:
                result["got"] = got
        t = threading.Thread(target=contender)
        t.start(); t.join(2)
        assert result.get("got") is False, "secondo reconcile ammesso mentre il primo tiene il lock"


def test_trg_628_b_0975_cleanup_idle_batch_is_removed():
    """ID: TRG-628-B-0975 — parte non controversa: un lotto inattivo viene rimosso dal registro."""
    mgr = _BatchLockManager()
    with mgr.acquire("B2"):
        pass
    mgr.cleanup_batch("B2")
    assert "B2" not in mgr._batch_locks
