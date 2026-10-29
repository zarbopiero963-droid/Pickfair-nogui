"""SIM-DUP-005 — duplicati e identita': lo stesso intento ripubblicato produce UN solo ordine (C-01, DUP-01, PR29-a)."""
import tempfile

import pytest

from tests.fullscope.harness import oracle as O
from tests.fullscope.harness import runner as H

pytestmark = [pytest.mark.fullscope, pytest.mark.e2e]


def test_replayed_command_and_repeated_signal_place_one_order():
    home = tempfile.mkdtemp(prefix="pf_dup005_")
    r = H.run([{"op": "signal", "m": "1.821", "price": 2.0, "stake": 3.0},
               {"op": "replay_cmd", "m": "1.821"},                               # stesso CMD_QUICK_BET ripubblicato
               {"op": "signal", "m": "1.821", "price": 2.0, "stake": 3.0},       # stesso segnale di nuovo
               {"op": "wait", "s": 1}], home=home, scenario="sim_dup_005")
    assert all(w["met"] for w in r["waits"]), r["waits"]
    s = r["snaps"][-1]
    live = [o for o in s["orders"] if o["market_id"] == "1.821" and o["status"] in ("SUBMITTED", "COMPLETED", "INFLIGHT", "AMBIGUOUS")]
    assert len(live) == 1, "intento duplicato inviato piu' volte: " + O.describe(s)
    assert len([o for o in s["exch_orders"] if o["market_id"] == "1.821"]) == 1, "due ordini sull'exchange: " + O.describe(s)
    assert all(o["customer_ref"] for o in live), "ordine senza customer_ref: " + O.describe(s)
    O.check_no_duplicate_intents(s, ["1.821"])
