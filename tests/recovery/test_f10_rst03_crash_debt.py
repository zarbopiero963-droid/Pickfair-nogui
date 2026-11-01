"""F-10 (#586, RST-03, #582 D-582-R4-01) — un crash dopo un settlement LOSS non perde il debito di recovery.

Runtime vero (HeadlessApp in sottoprocesso, SimulationBroker, SQLite reale su disco: harness F026). Scenario: il
mercato e' gia' CLOSED (LOSS) nel broker quando il processo muore, PRIMA che il runtime riceva
`RUNTIME_CLOSE_POSITION`; poi riavvio. Atteso: dopo il riavvio la perdita e' presente nel ciclo di recovery (tavolo
non FREE o `loss_amount` > 0) e un secondo settlement dello stesso mercato non la conta due volte.
"""
from __future__ import annotations

import os
import sys
import tempfile
from decimal import Decimal as D

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "f026"))

pytestmark = [pytest.mark.safety]

M = "1.851"


def _crash_then_restart():
    from test_f026_financial import run
    home = tempfile.mkdtemp(prefix="f10_")
    first = run([{"op": "signal", "m": M, "price": 2.0}, {"op": "crash_before_close_position"},
                 {"op": "settle", "m": M, "win": False}], home=home)
    second = run([{"op": "wait", "s": 4}], home=home)
    return first, second, home


def _order(snap):
    rows = [o for o in snap["orders"] if o["market_id"] == M and o["status"] not in ("DUPLICATE_BLOCKED",)]
    assert rows, f"precondizione: nessun ordine sul mercato {M}"
    return rows[0]


def _loss_on_tables(snap):
    return sum(D(str(t["loss"] or 0)) for t in snap["tables"])


def test_crash_after_market_closed_keeps_loss_debt():
    first, second, _home = _crash_then_restart()
    assert first.get("crashed_at") == "BEFORE_RUNTIME_CLOSE_POSITION", "precondizione: crash prima della chiusura"
    s = second["snaps"][-1]
    stake = D(str(_order(first["snaps"][-1])["matched"] or _order(first["snaps"][-1])["stake"]))
    loss = _loss_on_tables(s)
    assert loss == stake, f"debito di recovery dopo il riavvio = {loss}, atteso {stake} (perdita del settlement LOSS)"
    closes = [e for e in second["events"] if e["e"] == "RUNTIME_CLOSE_POSITION" and e["market_id"] == M]
    assert len(closes) == 1, f"il settlement va contabilizzato una volta dopo il riavvio, visto {len(closes)}"


def test_second_settlement_after_restart_does_not_double_count_the_debt():
    from test_f026_financial import run
    first, second, home = _crash_then_restart()
    third = run([{"op": "resettle", "m": M}], home=home)
    before = _loss_on_tables(second["snaps"][-1])
    after = _loss_on_tables(third["snaps"][-1])
    assert before > 0 and after == before, f"debito {before} -> {after}: doppio conteggio al secondo settlement"


# ------------------------------------------------------------------ attribuzione dal solo ordine: casi di blocco
class _DB:
    def __init__(self, rows=None, fail=False):
        self.rows, self.fail = rows or [], fail

    def _execute(self, *_a, **_k):
        if self.fail:
            raise RuntimeError("db down")
        return self.rows


def _row(market="1.9", ref="sig-a", ev="1.9:11:BACK:x", table_id=1, **extra):
    import json
    p = {"market_id": market, "event_key": ev, "table_id": table_id, "reservation_id": "r-1", **extra}
    return {"customer_ref": ref, "payload_json": json.dumps(p), "status": "SUBMITTED"}


def _rc(rows=None, fail=False, tables=2, hedged=None):
    from core.runtime_controller import RuntimeController
    from core.table_manager import TableManager
    rc = RuntimeController.__new__(RuntimeController)
    rc.db = _DB(rows, fail)
    rc.table_manager = TableManager(table_count=tables)
    rc._hedged_markets = dict(hedged or {})
    rc._hedged_markets_unreliable = False
    return rc


def test_settlement_without_table_is_attributed_to_the_persisted_reservation():
    owner = _rc([_row()])._resolve_market_settlement("1.9")
    assert owner == {"table_id": 1, "event_key": "1.9:11:BACK:x", "customer_ref": "sig-a",
                     "reservation_id": "r-1", "bet_id": ""}


@pytest.mark.parametrize("rc_kwargs,why_transient", [
    ({"rows": [_row(), _row(ref="sig-b", ev="1.9:12:BACK:y")]}, False),   # due ordini del bot sul mercato
    ({"rows": [_row(batch_id="B1")]}, False),                             # gamba di un lotto Dutching
    ({"rows": [_row()], "hedged": {"1.9": 1.0}}, False),                  # mercato con hedge/cashout
    ({"rows": [_row(table_id=None)]}, False),                             # tavolo della riserva non persistito
    ({"rows": [_row(ref="")]}, False),                                    # identita' assente
    ({"rows": [_row(table_id=7)]}, True),                                 # tavolo della riserva inesistente
    ({"fail": True}, True),                                               # ordini illeggibili
])
def test_settlement_without_table_stays_held_when_identity_is_not_proven(rc_kwargs, why_transient):
    rc = _rc(**rc_kwargs)
    why = []
    assert rc._resolve_market_settlement("1.9", why=why) is None
    assert (why == ["transient"]) is why_transient


def test_unreliable_hedge_registry_never_attributes_from_the_order():
    rc = _rc([_row()])
    rc._hedged_markets_unreliable = True
    assert rc._resolve_market_settlement("1.9") is None


def test_unreadable_order_row_never_attributes():
    rows = [{"customer_ref": "sig-a", "payload_json": "{rotto", "status": "SUBMITTED"}]
    assert _rc(rows)._resolve_market_settlement("1.9") is None
