"""PR #512 — rilievi Codex sul head 034af59 (ciclo 8). Un test per rilievo, scritto PRIMA della patch
(RED sul codice precedente), codice reale del progetto, deterministico e offline.

1. `BetfairService._well_formed_current_orders`: riga senza `betId`/`marketId` => CURRENT_ORDERS_MALFORMED
   (la ricostruzione F018 la leggeva come "posizione chiusa").
2. `NativeSimMicrostakeExecutor`: snapshot finale del broker non persistito => saga AMBIGUOUS, mai ACTIVE/SUCCESS.
3. `DutchingController._leg_liquidity_shortfall`: liquidita' ignota in modalita' blocco => shortfall LIQUIDITY_UNKNOWN.
4. `MicrostakeCoordinator.recover`: ROLLBACK_PENDING => cancel_all_remaining -> ROLLED_BACK, mai replace.
5. `reset_cycle`/`_maybe_auto_reset`: persistenza fallita => `_recovery_persist_failed`, evento, `persisted: False`.
6. `start()`: stato di recovery illeggibile => PAUSED (RECOVERY_STATE_UNREADABLE) e nessuna sovrascrittura.
7. docstring di `core/microstake_safe.py` veritiero.
10. `RoserpinaMoneyManagement._desk_mode`: crescita ignota => NORMAL anche con soglia 0.
"""
import json
import os
import sys
import threading
from decimal import Decimal as D

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tests", "f026"))

from services.betfair_service import BetfairService  # noqa: E402


# ------------------------------------------------------------------ 1. currentOrders senza identita'
class _OrdersBroker:
    def __init__(self, orders):
        self._orders = orders

    def get_current_orders(self, market_ids=None):
        return self._orders


@pytest.mark.parametrize("row", [
    {"marketId": "1.1"},                       # senza betId
    {"betId": "B1"},                           # senza marketId
    {"betId": "", "marketId": "1.1"},          # betId vuoto
    {"betId": "B1", "marketId": "  "},         # marketId vuoto
    {"betId": None, "marketId": "1.1"},
])
def test_f1_current_order_row_without_identity_is_malformed(row):
    with pytest.raises(RuntimeError, match="CURRENT_ORDERS_MALFORMED"):
        BetfairService._well_formed_current_orders([{"betId": "B0", "marketId": "1.0"}, row])


def test_f1_rows_with_identity_pass_in_both_spellings():
    rows = [{"betId": "B1", "marketId": "1.1"}, {"bet_id": "B2", "market_id": "1.2"}]
    assert BetfairService._well_formed_current_orders(rows) == rows


def test_f1_sim_facade_raises_on_row_without_bet_id_instead_of_closing_positions():
    svc = BetfairService.__new__(BetfairService)
    svc.simulation_mode = True
    svc.get_client = lambda: _OrdersBroker([{"marketId": "1.1"}])
    with pytest.raises(RuntimeError, match="CURRENT_ORDERS_MALFORMED"):
        svc.list_current_orders()


def test_f1_rebuild_goes_paused_on_row_without_bet_id():
    """Il percorso reale: F018 con una riga senza betId deve dare EXCHANGE_ORDERS_UNAVAILABLE, non 'closed'."""
    from test_f018_rebuild_units import _Exchange, _rc, _row

    class _Facade(_Exchange):
        def list_current_orders(self, market_ids=None):
            self.calls += 1
            return [BetfairService._normalize_startup_order(o)
                    for o in BetfairService._well_formed_current_orders(list(self.current))]

    rc = _rc([_row("r1", "SUBMITTED", "1.1", 1, "K1", bet_id="B1")], _Facade([{"marketId": "1.1"}]))
    res = rc._rebuild_open_positions()
    assert res["ok"] is False and res["reason"].startswith("EXCHANGE_ORDERS_UNAVAILABLE"), res
    assert res["closed"] == []
