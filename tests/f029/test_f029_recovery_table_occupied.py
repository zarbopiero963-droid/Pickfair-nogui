"""PF-ALL-F029 — un tavolo in RECOVERY gia' occupato non si riassegna (codice reale TableManager).

Riprodotto dalla campagna PF-ROSERPINA-41-FULL-SIMULATION-02 (S00200, S00870): con un solo tavolo e recovery
attivo, dopo una perdita il tavolo va in RECOVERY; il primo ingresso lo occupa e il secondo lo riceveva di nuovo da
``allocate``. ``activate`` sovrascriveva la posizione aperta: esposizione persa dal totale (cap aggirati), due
posizioni sullo stesso tavolo, tavolo non piu' rilasciabile al settlement della prima.
"""
import pytest

from core.table_manager import TableManager


def _recovery_table_with_open_position():
    tm = TableManager(table_count=1)
    tm.activate(table_id=1, event_key="E0", exposure=5.0, market_id="1.0", selection_id=11)
    tm.release(1, pnl=-5.0)                                  # perdita: tavolo in RECOVERY, libero
    t = tm.allocate(event_key="E1", allow_recovery=True)
    assert t is not None and t.table_id == 1                 # il recovery libero si assegna
    tm.activate(table_id=1, event_key="E1", exposure=4.0, market_id="1.1", selection_id=11)
    return tm


def test_occupied_recovery_table_is_not_allocated_again():
    tm = _recovery_table_with_open_position()
    assert tm.allocate(event_key="E2", allow_recovery=True) is None, "tavolo occupato: nessuna riassegnazione"


def test_open_position_exposure_is_not_lost():
    tm = _recovery_table_with_open_position()
    t = tm.allocate(event_key="E2", allow_recovery=True)
    if t is not None:                                        # come fa il runtime: allocate poi activate
        tm.activate(table_id=t.table_id, event_key="E2", exposure=3.0, market_id="1.2", selection_id=11)
    assert tm.total_exposure() == pytest.approx(4.0)
    assert tm.find_by_event_key("E1") is not None


def test_free_recovery_table_is_still_allocated_with_its_loss():
    tm = TableManager(table_count=2)
    tm.activate(table_id=1, event_key="E0", exposure=5.0, market_id="1.0", selection_id=11)
    tm.activate(table_id=2, event_key="E9", exposure=1.0, market_id="1.9", selection_id=11)
    tm.release(1, pnl=-5.0)
    t = tm.allocate(event_key="E1", allow_recovery=True)
    assert t is not None and t.table_id == 1 and t.loss_amount == pytest.approx(5.0)


def test_recovery_beyond_table_count_is_not_allocated():
    tm = TableManager(table_count=2)
    tm.activate(table_id=2, event_key="E0", exposure=5.0, market_id="1.0", selection_id=11)
    tm.release(2, pnl=-5.0)
    tm.activate(table_id=1, event_key="E1", exposure=1.0, market_id="1.1", selection_id=11)
    tm.resize(1)
    assert tm.allocate(event_key="E2", allow_recovery=True) is None
