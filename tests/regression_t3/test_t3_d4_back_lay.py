"""Decisione owner D4 (PF-OWNER-GATEA-FIVE-DECISIONS-20261111): contratto Betfair e F6 / DEC-426 P25. Riga PF-I04B-02
rettificata: si distinguono (1) lato dell'ordine del cliente, (2) lato della controparte, (3) campo API della
liquidita' disponibile, (4) riduzione della liquidita' dopo il matching.

Semantica Betfair: un BACK del cliente si abbina alla liquidita' esposta in `availableToBack` (offerte dei layer
controparte) ed e' e resta un ordine BACK; un LAY del cliente usa `availableToLay` e resta LAY. Fixture della riga:
best BACK 1.90 size 100, best LAY 2.00 size 50."""
import pytest

from simulation_broker import SimulationBroker

M, S = "1.500", 77


def _book(back=1.90, lay=2.00, sb=100.0, sl=50.0):
    return {"marketId": M, "status": "OPEN", "runners": [{"selectionId": S, "status": "ACTIVE", "ex": {
        "availableToBack": [{"price": back, "size": sb}], "availableToLay": [{"price": lay, "size": sl}]}}]}


def _ex(b):
    return b.get_market_book(M)["runners"][0]["ex"]


@pytest.mark.parametrize("side,price,field,other,start", [("BACK", 1.90, "availableToBack", "availableToLay", 100.0),
                                                          ("LAY", 2.00, "availableToLay", "availableToBack", 50.0)])
def test_pf_i04b_02_order_side_kept_and_liquidity_reduced_once(side, price, field, other, start):
    """ID: PF-I04B-02 — l'ordine resta del suo lato; consuma una sola volta il campo giusto; l'altro resta intatto."""
    b = SimulationBroker(starting_balance=1000.0)
    b.update_market_book(_book())
    other_start = _ex(b)[other][0]["size"]
    r = b.place_bet(market_id=M, selection_id=S, side=side, price=price, size=10.0)
    rep = r["instructionReports"][0]
    assert float(rep["sizeMatched"]) == pytest.approx(10.0)
    orders = b.list_current_orders(market_ids=[M]) if True else []
    sides = {str(o.get("side")).upper() for o in (orders.get("currentOrders", orders) if isinstance(orders, dict) else orders)}
    assert sides <= {side}, f"ordine {side} riportato come {sides}"
    for _ in range(3):                                   # letture e poll ripetuti non consumano di nuovo
        ex = _ex(b)
        b.list_current_orders(market_ids=[M])
    assert ex[field][0]["size"] == pytest.approx(start - 10.0)
    assert ex[other][0]["size"] == pytest.approx(other_start)


def test_pf_i04b_02_client_wire_side_is_never_inverted():
    """ID: PF-I04B-02 — sul filo verso Betfair il lato inviato e' quello del cliente."""
    from betfair_client import BetfairClient

    class _S:
        def __init__(self):
            self.bodies = []

        def post(self, *a, **k):
            self.bodies.append(str(k.get("json") or k.get("data")))

            class _R:
                status_code = 200

                def json(self_inner):
                    return {"jsonrpc": "2.0", "result": {"status": "SUCCESS", "instructionReports": [
                        {"status": "SUCCESS", "betId": "1", "sizeMatched": 0.0}]}, "id": 1}
            return _R()
    for side in ("BACK", "LAY"):
        s = _S()
        c = BetfairClient(username="u", app_key="a", cert_pem="c.pem", key_pem="k.pem", session=s)
        c.session_token = "TOK"
        try:
            c.place_bet(market_id="1.5", selection_id=7, side=side, price=2.0, size=5.0)
        except Exception:
            pass
        assert len(s.bodies) == 1
        wrong = "LAY" if side == "BACK" else "BACK"
        assert f"'side': '{side}'" in s.bodies[0] or f'"side": "{side}"' in s.bodies[0], s.bodies[0]
        assert f"'side': '{wrong}'" not in s.bodies[0] and f'"side": "{wrong}"' not in s.bodies[0]
