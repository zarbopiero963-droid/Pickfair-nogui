"""F-33 — test NEGATIVI dell'oracolo: ogni invariante deve FALLIRE su uno snapshot manomesso (nessun controllo vacuo)."""
import copy
from decimal import Decimal as D

import pytest

from tests.fullscope.harness import oracle as O

pytestmark = [pytest.mark.unit, pytest.mark.fullscope, pytest.mark.invariant]

BASE = {
    "tag": "t", "mode": "ACTIVE",
    "funds": {"available": 101.91, "exposure": 0.0, "total": 101.91},
    "realized": 1.91, "bankroll": 101.91, "peak": 101.91, "drawdown_pct": 0.0,
    "orders": [{"market_id": "1.1", "side": "BACK", "stake": 2.0, "price": 2.0, "matched": 2.0, "status": "SUBMITTED", "customer_ref": "a"}],
    "tables": [],
    "exch_orders": [{"market_id": "1.1", "side": "BACK", "matched": 2.0, "price": 2.0, "ref": "a"}],
    "exch_settlements": [{"market_id": "1.1", "settlement_id": "S1", "gross_pnl": 2.0, "net_pnl": 1.91}],
}
COMM = D("0.045")


def _oracle(snap, outcomes={"1.1": True}):
    return O.Oracle(snap, outcomes, start=D("100"), commission=COMM)


def test_baseline_snapshot_passes_all_checks():
    s = copy.deepcopy(BASE)
    o = _oracle(s)
    O.check_conservation(s, o)
    O.check_realized(s, o)
    O.check_single_settlement_per_market(s)
    O.check_no_duplicate_intents(s, ["1.1"])
    O.check_tables_match_open_positions(s, o)
    assert O.q(o.realized()) == D("1.910")


def test_pnl_formulas_back_and_lay():
    assert O.side_pnl("BACK", D(2), D(2), True, COMM) == D("1.91")
    assert O.side_pnl("BACK", D(2), D(2), False, COMM) == D(-2)
    assert O.side_pnl("LAY", D(2), D(3), True, COMM) == D(-4)          # la selezione vince: liability 2*(3-1)
    assert O.side_pnl("LAY", D(2), D(3), False, COMM) == D("1.91")      # la selezione perde: stake netto


def test_conservation_detects_missing_money():
    s = copy.deepcopy(BASE)
    s["funds"]["available"] = 100.0          # 1.91 scomparsi
    with pytest.raises(AssertionError, match="conservazione"):
        O.check_conservation(s, _oracle(s))


def test_realized_detects_invented_profit():
    s = copy.deepcopy(BASE)
    s["realized"] = 5.0
    with pytest.raises(AssertionError, match="realizzato"):
        O.check_realized(s, _oracle(s))


def test_duplicate_settlement_detected():
    s = copy.deepcopy(BASE)
    s["exch_settlements"].append({"market_id": "1.1", "settlement_id": "S2", "gross_pnl": 2.0, "net_pnl": 1.91})
    with pytest.raises(AssertionError, match="duplicati"):
        O.check_single_settlement_per_market(s)


def test_duplicate_intent_detected():
    s = copy.deepcopy(BASE)
    s["orders"].append(dict(s["orders"][0], customer_ref="b"))
    with pytest.raises(AssertionError, match="doppio invio"):
        O.check_no_duplicate_intents(s, ["1.1"])


def test_prematurely_freed_table_detected():
    s = copy.deepcopy(BASE)
    o = _oracle(s, outcomes={})               # mercato NON regolato: il tavolo deve essere vivo
    with pytest.raises(AssertionError, match="tavoli"):
        O.check_tables_match_open_positions(s, o)


def test_exchange_leg_missing_from_ledger_detected():
    s = copy.deepcopy(BASE)
    s["exch_orders"].append({"market_id": "1.1", "side": "LAY", "matched": 1.98, "price": 2.02, "ref": "h"})
    missing = O.ledger_missing_exchange_legs(s)
    assert missing and missing[0]["side"] == "LAY"
    o = O.Oracle(s, {"1.1": True}, start=D("100"), commission=COMM, extra_legs=O.exchange_legs(s))
    assert O.q(o.realized()) < D("1.910"), "l'hedge deve ridurre il netto del mercato"
