"""Gate A T2b batch b4 — roserpina: core.table_manager.TableManager (in memoria)."""
from __future__ import annotations

from core.table_manager import TableManager


def _act(tm, tid, key, exp=10.0):
    tm.activate(table_id=tid, event_key=key, exposure=exp, market_id="1.1", selection_id=1)


def test_trg_628_b_0829():
    """ID: TRG-628-B-0829 / PF-B1-TABLE-RESIZE

    Riducendo il numero, si tolgono solo i tavoli liberi e puliti oltre il nuovo count; un tavolo oltre il
    count con evento/esposizione o perdite resta (con il suo stato) e sparisce solo quando torna pulito."""
    tm = TableManager(table_count=5)
    _act(tm, 4, "ev4", 12.5)
    tm.release(5, pnl=-3.0)                       # tavolo 5: perdita in memoria, RECOVERY
    tm.resize(2)
    ids = sorted(t["table_id"] for t in tm.snapshot())
    assert ids == [1, 2, 4, 5], ids
    assert tm.get_table(4).current_exposure == 12.5 and tm.get_table(4).current_event_key == "ev4"
    assert tm.get_table(5).loss_amount == 3.0
    assert tm.total_exposure() == 12.5
    tm.release(4, pnl=0.0)                        # tornato pulito => potato
    assert tm.get_table(4) is None
    tm.resize(6)
    assert sorted(t["table_id"] for t in tm.snapshot()) == [1, 2, 3, 4, 5, 6]
    assert tm.get_table(5).loss_amount == 3.0, "memoria perdite persa su resize"


def test_trg_628_b_0830():
    """ID: TRG-628-B-0830 / PF-B1-TABLE-ALLOCATE

    allocate preferisce un FREE entro table_count; altrimenti (allow_recovery) un tavolo in recovery NON
    occupato entro count, ordinato per (loss_amount, table_id); allow_recovery=False => None."""
    tm = TableManager(table_count=3)
    tm.release(3, pnl=-1.0)
    assert tm.allocate(event_key="e").table_id == 1
    _act(tm, 1, "a")
    _act(tm, 2, "b")
    tm.release(2, pnl=-5.0)                       # recovery 2 (loss 5), recovery 3 (loss 1)
    assert tm.allocate(event_key="e").table_id == 3
    tm.release(1, pnl=-1.0)                       # 1: loss 1 come il 3 -> vince id minore
    assert tm.allocate(event_key="e").table_id == 1
    _act(tm, 1, "occ")                            # recovery occupato: escluso
    assert tm.allocate(event_key="e").table_id == 3
    assert tm.allocate(event_key="e", allow_recovery=False) is None
    # tavolo libero oltre count (rimasto dopo riduzione) non si riassegna
    tm2 = TableManager(table_count=2)
    _act(tm2, 1, "x")
    _act(tm2, 2, "y")
    tm2._tables[3] = type(tm2._tables[1])(table_id=3)
    assert tm2.allocate(event_key="e", allow_recovery=False) is None


def test_trg_628_b_0833():
    """ID: TRG-628-B-0833 / PF-B1-TABLE-EXPOSURE

    total_exposure somma le esposizioni con clamp a >=0 per tavolo (mai sotto-stima) e restituisce inf se
    un'esposizione non e' finita (stato corrotto => cap blocca)."""
    tm = TableManager(table_count=3)
    _act(tm, 1, "a", 10.0)
    _act(tm, 2, "b", -4.0)
    assert tm.total_exposure() == 10.0
    tm._tables[3].current_exposure = float("nan")
    assert tm.total_exposure() == float("inf")
    tm._tables[3].current_exposure = float("inf")
    assert tm.total_exposure() == float("inf")
