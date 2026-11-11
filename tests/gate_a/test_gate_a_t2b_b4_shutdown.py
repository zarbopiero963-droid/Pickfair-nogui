"""Gate A T2b batch b4 — shutdown_manager.ShutdownManager (puro, nessun I/O)."""
from __future__ import annotations

from shutdown_manager import ShutdownManager


def test_trg_628_b_0729():
    """ID: TRG-628-B-0729 / PF-B1-SHUTDOWN-REPLACE

    Un hook con lo stesso nome sostituisce il precedente (una sola esecuzione, la nuova funzione);
    l'esecuzione segue (priority, name)."""
    sm = ShutdownManager()
    ran = []
    sm.register("b", lambda: ran.append("b-old"), priority=1)
    sm.register("z", lambda: ran.append("z"), priority=5)
    sm.register("a", lambda: ran.append("a"), priority=5)
    sm.register("b", lambda: ran.append("b-new"), priority=10)
    assert [h["name"] for h in sm.snapshot()] == ["a", "z", "b"]
    sm.shutdown()
    assert ran == ["a", "z", "b-new"]


def test_trg_628_b_0730():
    """ID: TRG-628-B-0730 / PF-B1-SHUTDOWN-READINESS

    is_ready() e' True prima dello shutdown e False gia' DURANTE l'esecuzione degli hook (has_run impostato
    prima di eseguirli) e dopo; un secondo shutdown non riesegue gli hook."""
    sm = ShutdownManager()
    seen = []
    sm.register("probe", lambda: seen.append(sm.is_ready()))
    assert sm.is_ready() is True
    sm.shutdown()
    assert seen == [False]
    assert sm.is_ready() is False
    assert sm.shutdown() == [] and seen == [False]
