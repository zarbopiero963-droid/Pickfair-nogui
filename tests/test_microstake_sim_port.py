"""PF-MICROSTAKE-NATIVE-INTEGRATION-02, lotto M2 — adapter autoritativo del SimulationBroker.

Prova (RED prima dell'adapter) che il broker di simulazione offre le operazioni che la saga Microstake richiede
con la semantica Betfair: cancellazione parziale `sizeReduction`, `replaceOrders` con NUOVO betId, identita'
persistita, risposta persa => AMBIGUOUS mai ritentata. Broker reale, nessun mock del broker.
"""
from decimal import Decimal

import pytest

from simulation_broker import SimulationBroker

D = Decimal


class _Db:
    """Registra le scritture durevoli del broker (save_simulation_bet / save_simulation_state)."""

    def __init__(self):
        self.bets = []
        self.states = []
        self.settings = {}

    def save_simulation_bet(self, payload):
        self.bets.append(dict(payload))

    def save_simulation_state(self, key, state):
        self.states.append((key, state))

    def save_settings(self, data):
        self.settings.update({str(k): v for k, v in data.items()})

    def get_settings(self):
        return dict(self.settings)


def _broker(db=None):
    return SimulationBroker(starting_balance=100.0, db=db)


def _book(broker, market_id, selection_id, *, back=None, lay=None):
    runner = {"selectionId": selection_id, "ex": {
        "availableToBack": [{"price": back[0], "size": back[1]}] if back else [],
        "availableToLay": [{"price": lay[0], "size": lay[1]}] if lay else [],
    }}
    broker.update_market_book(market_id, {"marketId": market_id, "status": "OPEN", "runners": [runner]})


def _order(broker, bet_id):
    rows = broker.list_current_orders([None] and None)["currentOrders"]
    return next(r for r in rows if r["betId"] == bet_id)


# ------------------------------------------------------------------ cancel_orders sizeReduction
def test_red_size_reduction_leaves_exactly_the_desired_unmatched_size():
    db = _Db()
    b = _broker(db)
    rep = b.place_bet(market_id="1.1", selection_id=7, side="BACK", price=3.0, size=2.0, customer_ref="ms-1")
    bet_id = rep["instructionReports"][0]["betId"]
    res = b.cancel_orders(market_id="1.1", instructions=[{"betId": bet_id, "sizeReduction": 0.09}])
    r = res["instructionReports"][0]
    assert r["status"] == "SUCCESS" and r["sizeCancelled"] == pytest.approx(0.09)
    o = _order(b, bet_id)
    assert o["status"] == "EXECUTABLE", "una riduzione parziale non cancella l'ordine"
    assert o["sizeRemaining"] == pytest.approx(1.91) and o["priceSize"]["size"] == pytest.approx(1.91)
    assert any(x["bet_id"] == bet_id and x["size"] == pytest.approx(1.91) for x in db.bets), "riduzione non persistita"


def test_red_size_reduction_keeps_partial_match_visible():
    b = _broker()
    _book(b, "1.2", 7, back=(3.0, 0.5))                                   # il BACK a 3.0 si abbina per 0.5
    rep = b.place_bet(market_id="1.2", selection_id=7, side="BACK", price=3.0, size=2.0, customer_ref="ms-2")
    bet_id = rep["instructionReports"][0]["betId"]
    assert rep["instructionReports"][0]["sizeMatched"] == pytest.approx(0.5)
    res = b.cancel_orders(market_id="1.2", instructions=[{"betId": bet_id, "sizeReduction": 1.0}])
    assert res["instructionReports"][0]["status"] == "SUCCESS"
    o = _order(b, bet_id)
    assert o["sizeMatched"] == pytest.approx(0.5) and o["sizeRemaining"] == pytest.approx(0.5)
    assert o["priceSize"]["size"] == pytest.approx(1.0)


@pytest.mark.parametrize("bad", [0, -0.5, float("nan"), float("inf"), "abc", True])
def test_red_invalid_size_reduction_is_rejected_and_leaves_state_intact(bad):
    b = _broker()
    rep = b.place_bet(market_id="1.3", selection_id=7, side="BACK", price=3.0, size=2.0, customer_ref="ms-3")
    bet_id = rep["instructionReports"][0]["betId"]
    res = b.cancel_orders(market_id="1.3", instructions=[{"betId": bet_id, "sizeReduction": bad}])
    assert res["instructionReports"][0]["status"] == "FAILURE"
    o = _order(b, bet_id)
    assert o["status"] == "EXECUTABLE" and o["sizeRemaining"] == pytest.approx(2.0)


def test_red_size_reduction_beyond_remaining_cancels_the_whole_remaining_like_betfair():
    b = _broker()
    rep = b.place_bet(market_id="1.4", selection_id=7, side="BACK", price=3.0, size=2.0, customer_ref="ms-4")
    bet_id = rep["instructionReports"][0]["betId"]
    res = b.cancel_orders(market_id="1.4", instructions=[{"betId": bet_id, "sizeReduction": 5.0}])
    r = res["instructionReports"][0]
    assert r["status"] == "SUCCESS" and r["sizeCancelled"] == pytest.approx(2.0)
    assert _order(b, bet_id)["status"] == "CANCELLED"


def test_red_full_cancel_without_size_reduction_is_unchanged():
    b = _broker()
    rep = b.place_bet(market_id="1.5", selection_id=7, side="LAY", price=3.0, size=2.0, customer_ref="ms-5")
    bet_id = rep["instructionReports"][0]["betId"]
    res = b.cancel_orders(market_id="1.5", instructions=[{"betId": bet_id}])
    assert res["instructionReports"][0]["sizeCancelled"] == pytest.approx(2.0)
    assert _order(b, bet_id)["status"] == "CANCELLED"


# ------------------------------------------------------------------ porta exchange per la saga
def _port(db=None):
    from core.microstake_sim_port import SimulationExchangePort
    b = _broker(db)
    return b, SimulationExchangePort(b)


def test_red_port_place_reduce_replace_tracks_the_new_bet_id_identity():
    from core.microstake_safe import Side
    b, port = _port()
    _book(b, "1.6", 7, back=(2.0, 50.0))                                   # target 2.0 si abbinerebbe: stub passivo 2.02
    o = port.place(market_id="1.6", selection_id=7, side=Side.BACK, size=D("2.00"), price=D("2.02"), customer_ref="ms-6")
    assert o.matched == 0 and o.remaining == D("2.00")
    o = port.cancel_reduction(o.bet_id, D("1.90"))
    assert o.remaining == D("0.10") and o.size == D("0.10")
    old_id = o.bet_id
    o2 = port.replace(old_id, D("2.00"))
    assert o2.bet_id != old_id, "replaceOrders su Betfair crea un nuovo betId"
    assert o2.price == D("2.00") and o2.size == D("0.10")
    assert o2.matched == D("0.10"), "al prezzo target l'ordine economico si abbina sul book"
    assert _order(b, old_id)["status"] == "CANCELLED"
    found = port.find_by_ref("ms-6")
    assert found is not None and found.bet_id == o2.bet_id, "la riconciliazione per customer_ref segue la nuova identita'"


def test_red_port_replace_rejected_leaves_old_order_and_raises():
    from core.microstake_safe import Side
    b, port = _port()
    o = port.place(market_id="1.7", selection_id=7, side=Side.BACK, size=D("2.00"), price=D("3.00"), customer_ref="ms-7")
    with pytest.raises(RuntimeError):
        port.replace(o.bet_id, D("1.00"))                                  # prezzo non valido: FAILURE del broker
    assert _order(b, o.bet_id)["status"] == "EXECUTABLE"
    assert port.cancel_all_remaining(o.bet_id).remaining == 0
    assert _order(b, o.bet_id)["status"] == "CANCELLED"


def test_red_port_find_by_ref_is_none_when_absent_and_raises_on_duplicates():
    from core.microstake_safe import Side
    b, port = _port()
    assert port.find_by_ref("nessuno") is None
    port.place(market_id="1.8", selection_id=7, side=Side.BACK, size=D("2.00"), price=D("3.00"), customer_ref="dup")
    b.place_bet(market_id="1.8", selection_id=8, side="BACK", price=3.0, size=2.0, customer_ref="dup")
    with pytest.raises(RuntimeError):
        port.find_by_ref("dup")


def test_red_port_reservation_is_capped_across_concurrent_refs():
    b, port = _port()
    assert port.reserve("a", D("2"), D("3")) is True
    assert port.reserve("b", D("2"), D("3")) is False, "la seconda riserva supera il cap complessivo"
    assert port.reserve("a", D("2"), D("3")) is True, "idempotente sullo stesso ref"
    port.release_reservation("a")
    assert port.reserve("b", D("2"), D("3")) is True


# ------------------------------------------------------------------ saga sul broker reale, con guasti
def _coordinator(port, db, cap="10"):
    from core.microstake_safe import MicrostakeCoordinator, MicrostakePolicy
    from core.microstake_sim_port import DbSagaStore
    return MicrostakeCoordinator(port, DbSagaStore(db), MicrostakePolicy(max_transient_exposure=D(cap)))


def _req(ref, stake="0.10", price="2.00", side=None):
    from core.microstake_safe import MicrostakeRequest, Side
    return MicrostakeRequest(market_id="1.9", selection_id=7, side=side or Side.BACK, economic_stake=D(stake),
                             target_price=D(price), customer_ref=ref)


def test_red_saga_on_real_broker_without_book_rests_at_target_without_replace():
    """Senza controparte lo stub al prezzo target non puo' abbinarsi: riduzione in loco, stessa identita'."""
    from core.microstake_safe import Phase
    db = _Db()
    b, port = _port(db)
    res = _coordinator(port, db).execute(_req("saga-1"), stub_price=D("2.00"))
    assert res.phase is Phase.ACTIVE and res.final_unmatched_size == D("0.10")
    orders = b.list_current_orders()["currentOrders"]
    assert len(orders) == 1 and orders[0]["betId"] == res.bet_id and orders[0]["sizeRemaining"] == pytest.approx(0.10)
    assert [op for op, *_ in port.operation_log] == ["PLACE", "CANCEL"]


def test_red_saga_lost_cancel_response_is_ambiguous_and_never_retried():
    from core.microstake_safe import LostResponse, Phase
    db = _Db()
    b, port = _port(db)
    port.inject_fault("LOST_CANCEL_RESPONSE")
    res = _coordinator(port, db).execute(_req("saga-2"), stub_price=D("2.00"))
    assert res.phase is Phase.AMBIGUOUS and res.reservation_held is True
    ops_before = list(port.operation_log)
    again = _coordinator(port, db).execute(_req("saga-2"), stub_price=D("2.00"))   # riavvio: stesso ref
    assert again.phase in (Phase.ACTIVE, Phase.AMBIGUOUS)
    assert [op for op, *_ in port.operation_log if op == "PLACE"] == ["PLACE"], "lo stub accettato non si ripiazza mai"
    assert len(b.list_current_orders()["currentOrders"]) == 1
    assert ops_before == port.operation_log[: len(ops_before)]


def test_red_saga_store_survives_restart_through_the_database():
    from core.microstake_sim_port import DbSagaStore
    db = _Db()
    DbSagaStore(db).save("r1", phase="ACTIVE", bet_id="SIMBET-x")
    assert DbSagaStore(db).load("r1") == {"phase": "ACTIVE", "bet_id": "SIMBET-x"}
    db.settings["microstake.sagas"] = "{rotto"
    with pytest.raises(RuntimeError):
        DbSagaStore(db).load("r1")
