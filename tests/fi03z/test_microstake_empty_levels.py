"""CB-586-FI-03Z (#604, richiesta owner): microstake da 0,10 EUR sul broker SIM reale con livelli vuoti nel book.

Saga completa (quota SIM -> stub passivo da 1 EUR -> riduzione -> riprezzamento al target) con il primo livello del
book a size 0/negativa: l'esposizione abbinata non supera MAI lo stake richiesto, l'abbinamento parziale e il residuo
sono esatti, nessun ordine in piu'. Broker reale, nessun mock del broker.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from core.microstake_safe import MicrostakeCoordinator, MicrostakePolicy, MicrostakeRequest, Phase, Side
from core.microstake_sim_port import DbSagaStore, SimulationExchangePort, SimulationQuoteProvider
from simulation_broker import SimulationBroker

D = Decimal
MKT, SEL = "1.950", 9


class _Db:
    def __init__(self):
        self.settings = {}

    def save_simulation_bet(self, payload):
        pass

    def save_simulation_state(self, key, state):
        pass

    def save_settings(self, data):
        self.settings.update({str(k): v for k, v in data.items()})

    def get_settings(self):
        return dict(self.settings)


def _run(back, lay, side, target, stake="0.10"):
    db = _Db()
    broker = SimulationBroker(starting_balance=100.0, db=db)
    broker.update_market_book(MKT, {"marketId": MKT, "status": "OPEN", "runners": [{"selectionId": SEL, "ex": {
        "availableToBack": [{"price": p, "size": s} for p, s in back],
        "availableToLay": [{"price": p, "size": s} for p, s in lay]}}]})
    port = SimulationExchangePort(broker)
    quote = SimulationQuoteProvider(broker)({"bet_type": side.value, "price": target, "market_id": MKT,
                                             "selection_id": SEL})
    coordinator = MicrostakeCoordinator(port, DbSagaStore(db), MicrostakePolicy(max_transient_exposure=D("10")))
    res = coordinator.execute(MicrostakeRequest(market_id=MKT, selection_id=SEL, side=side, economic_stake=D(stake),
                                                target_price=D(str(target)), customer_ref=f"fi03z-{side.value}"),
                              stub_price=D(str(quote["passive_stub_price"])))
    orders = broker.list_current_orders()["currentOrders"]
    return res, quote, orders, port


CENT = D("0.01")


def _matched(orders):
    """Abbinato totale su TUTTI gli ordini (anche lo stub sostituito): misura l'esposizione davvero abbinata."""
    return sum(D(str(o.get("sizeMatched") or 0)) for o in orders).quantize(CENT)


def _residual(orders):
    """Residuo vivo: solo gli ordini ancora EXECUTABLE (lo stub sostituito e' CANCELLED, non e' esposizione).
    Quantizzato al centesimo: il SIM porta ancora rumore float sulle size (FI-08B)."""
    return sum(D(str(o.get("sizeRemaining") or 0)) for o in orders if o.get("status") == "EXECUTABLE").quantize(CENT)


@pytest.mark.parametrize("empty", [0.0, -1.0])
def test_back_microstake_with_an_empty_first_level_matches_exactly_the_requested_stake(empty):
    res, quote, orders, port = _run([(2.0, empty), (1.9, 5.0)], [(2.1, 5.0)], Side.BACK, 1.5)
    assert D(str(quote["best_counterparty_price"])) == D("1.9")            # la quota vede il livello vero
    assert _matched(orders) == D("0.10")                                   # mai lo stub da 1 EUR abbinato
    assert res.phase is Phase.ACTIVE and res.matched_size == D("0.10") and res.final_unmatched_size == 0
    assert [op for op, *_ in port.operation_log].count("PLACE") == 1      # un solo stub, mai ripiazzato


def test_back_microstake_partial_match_leaves_the_exact_residual():
    res, _quote, orders, _port = _run([(2.0, -1.0), (1.9, 0.04)], [(2.1, 5.0)], Side.BACK, 1.5)
    matched, residual = _matched(orders), _residual(orders)
    assert matched == D("0.04") and residual == D("0.06") and matched + residual == D("0.10")
    assert res.phase is Phase.ACTIVE and res.matched_size == D("0.04")
    assert len([o for o in orders if o.get("status") == "EXECUTABLE"]) == 1     # un solo ordine vivo


def test_lay_microstake_with_an_empty_first_level_matches_exactly_the_requested_stake():
    res, quote, orders, _port = _run([(1.9, 5.0)], [(2.02, 0.0), (2.1, 5.0)], Side.LAY, 3.0)
    assert D(str(quote["best_counterparty_price"])) == D("2.1")
    assert _matched(orders) == D("0.10")
    assert res.phase is Phase.ACTIVE and res.final_unmatched_size == 0


def test_microstake_against_only_empty_levels_rests_unmatched_at_target():
    res, quote, orders, _port = _run([(2.0, 0.0)], [(2.02, 0.0)], Side.BACK, 1.5)
    assert quote["best_counterparty_price"] is None
    assert _matched(orders) == 0 and _residual(orders) == D("0.10") and res.final_unmatched_size == D("0.10")
