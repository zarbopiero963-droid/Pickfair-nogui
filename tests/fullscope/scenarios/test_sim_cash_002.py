"""SIM-CASH-002 — cashout (CSH-09 e' PARTIAL in R1, non nel catalogo UNVERIFIED: questa e' una sonda positiva, non una chiusura): un solo cashout per posizione (CSH-09, #516), hedge sul lato opposto, P&L BACK/LAY
dall'oracolo; secondo comando sulla stessa posizione => nessun secondo hedge."""
import tempfile
from decimal import Decimal as D

import pytest

from tests.fullscope.harness import oracle as O
from tests.fullscope.harness import runner as H

pytestmark = [pytest.mark.fullscope, pytest.mark.e2e]

START = D("100")
COMM = D("0.045")


@pytest.fixture(scope="module")
def cash002():
    home = tempfile.mkdtemp(prefix="pf_cash002_")
    return H.run([{"op": "signal", "m": "1.851", "price": 2.0, "stake": 3.0},
                  {"op": "signal", "m": "1.852", "price": 2.0, "side": "LAY", "stake": 2.0},
                  {"op": "cashout", "m": "1.851"},
                  {"op": "cashout", "m": "1.851"},            # secondo comando sulla STESSA posizione
                  {"op": "wait", "s": 2},
                  {"op": "settle", "m": "1.851", "win": True},
                  {"op": "settle", "m": "1.852", "win": True},  # la selezione layata VINCE => il LAY perde la liability
                  {"op": "wait", "s": 1}], home=home, scenario="sim_cash_002")


def test_hedge_is_opposite_side_and_pnl_matches_oracle(cash002):
    r = cash002
    pre = next(s for s in r["snaps"] if s["tag"].endswith("cashout:1.851"))
    legs = [leg for leg in O.exchange_legs(pre) if leg["market_id"] == "1.851"]
    assert {leg["side"] for leg in legs} == {"BACK", "LAY"}, "hedge non sul lato opposto: " + O.describe(pre)
    s = r["snaps"][-1]
    o = O.Oracle(s, {"1.851": True, "1.852": True}, start=START, commission=COMM, extra_legs=O.exchange_legs(pre))
    O.check_realized(s, o)
    O.check_conservation(s, o)
    O.check_single_settlement_per_market(s)
    assert o.pnl_market("1.852") == D("-2"), "LAY perdente: liability = stake*(quota-1) = 2.0"


def test_second_cashout_command_on_same_position_places_no_second_hedge(cash002):
    r = cash002
    pre = next(s for s in r["snaps"] if s["tag"].startswith("3:cashout"))
    lays = [leg for leg in O.exchange_legs(pre) if leg["market_id"] == "1.851" and leg["side"] == "LAY"]
    successes = len(H.events(r, "CASHOUT_SUCCESS", "1.851"))
    assert len(lays) == 1 and successes == 1, \
        f"secondo cashout eseguito: hedge LAY={len(lays)} success={successes}: " + O.describe(pre)
