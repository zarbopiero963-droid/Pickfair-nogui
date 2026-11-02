"""F-13a (#586, ID-01, D-582-01, D1) — identita' dell'ordine sul wire e attribuzione (RED prima della patch).

Prima:
- i place diretti (`OrderRouter.place`, `OrderManager._raw_place_bet`, `BetfairService.place_order`) passavano solo
  `customer_ref` (il `customerRef` di de-dup della richiesta, 60 s): sull'ordine LIVE non c'era nessun
  `customerOrderRef`, quindi dopo una risposta persa l'ordine non si ritrovava per identita' (solo il Microstake LIVE
  lo inviava);
- il SimulationBroker accettava due richieste con lo stesso `customer_ref` (Betfair: DUPLICATE_TRANSACTION);
- `ReconciliationEngine._lookup_remote_order` ripiegava su `market::selection` (indice last-wins): una gamba senza
  identita' veniva attribuita all'ordine di QUALCUN ALTRO sullo stesso runner (D-582-01).
Atteso: `customerOrderRef` = `customer_ref` conforme (<= 32, charset Betfair) su ogni place diretto, mai un rifiuto
dell'ordine per un ref non conforme (omesso); SIM: stessa richiesta entro 60 s => FAILURE DUPLICATE_TRANSACTION,
esito ignoto (AMBIGUOUS) come in LIVE; riconciliazione per `bet_id`, poi per ref, mai per runner; un ordine non
attribuibile sulla selezione non viene adottato e la gamba non viene chiusa come FAILED (resta in attesa).
"""
from __future__ import annotations

import time
import uuid

import pytest

from betfair_client import BetfairClient
from core.order_router import OrderRouter
from core.reconciliation_engine import ReconciliationEngine
from order_manager import OrderManager, OrderStatus
from simulation_broker import SimulationBroker

pytestmark = [pytest.mark.safety]

REF = uuid.uuid4().hex                     # il customer_ref del percorso ordini reale: 32 hex


# ---------------------------------------------------------------------------------------------------- wire del place
class _RecordingClient:
    """Client con la firma di `BetfairClient.place_bet` (core + customer_ref + customer_order_ref)."""

    def __init__(self):
        self.calls = []

    def place_bet(self, *, market_id, selection_id, side, price, size, customer_ref="", customer_order_ref=""):
        self.calls.append({"customer_ref": customer_ref, "customer_order_ref": customer_order_ref})
        return {"ok": True, "result": {"status": "SUCCESS", "instructionReports": [
            {"status": "SUCCESS", "betId": "B1", "sizeMatched": float(size)}]}}


class _Service:
    def __init__(self, client):
        self.client = client

    def get_client(self):
        return self.client


def _payload(ref=REF, stake=2.0):
    return {"market_id": "1.100", "selection_id": 7, "bet_type": "BACK", "price": 2.0, "stake": stake,
            "customer_ref": ref}


def test_direct_place_sends_customer_order_ref():
    client = _RecordingClient()
    OrderRouter(_Service(client)).place(_payload())
    assert client.calls == [{"customer_ref": REF, "customer_order_ref": REF}]


def test_order_manager_direct_place_sends_customer_order_ref():
    client = _RecordingClient()
    OrderManager._raw_place_bet(OrderManager.__new__(OrderManager), client, _payload())
    assert client.calls == [{"customer_ref": REF, "customer_order_ref": REF}]


def test_betfair_service_place_order_sends_customer_order_ref():
    from services.betfair_service import BetfairService
    client = _RecordingClient()
    svc = BetfairService.__new__(BetfairService)
    svc.client, svc._session_invalid, svc._session_invalid_reason = client, False, ""
    svc.place_order(_payload())
    assert client.calls == [{"customer_ref": REF, "customer_order_ref": REF}]


@pytest.mark.parametrize("ref", ["", None, "ref con spazi", "x" * 33, "ref/slash"])
def test_non_conforming_ref_is_omitted_never_rejects_the_order(ref):
    # BetfairClient rifiuta un customer_order_ref non conforme (INVALID_CUSTOMER_ORDER_REF): il percorso diretto non
    # deve mai trasformare un ref libero (es. la `source` di un comando) in un ordine rifiutato
    client = _RecordingClient()
    OrderRouter(_Service(client)).place(_payload(ref=ref))
    assert client.calls == [{"customer_ref": ref, "customer_order_ref": ""}]       # customerRef invariato


def test_customer_order_ref_reaches_the_betfair_wire():
    # il client vero, senza rete: il ref deve arrivare nell'istruzione di placeOrders
    client = BetfairClient(username="u", app_key="dummy", cert_pem="c", key_pem="k")
    sent = []

    def post(url, method, params, **kw):
        sent.append(params)
        return {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS", "betId": "B1", "sizeMatched": 2.0}]}
    client._post_jsonrpc = post
    res = OrderRouter(_Service(client)).place(_payload())
    assert res["placed"] is True
    assert sent[0]["customerRef"] == REF and sent[0]["instructions"][0]["customerOrderRef"] == REF


# ------------------------------------------------------------------------------------------------------- parita' SIM
def _sim():
    broker = SimulationBroker(starting_balance=100.0)
    broker.update_market_book("1.100", {"marketId": "1.100", "status": "OPEN", "runners": [
        {"selectionId": 7, "ex": {"availableToBack": [{"price": 2.0, "size": 50.0}],
                                  "availableToLay": [{"price": 2.02, "size": 50.0}]}}]})
    return broker


def _place(broker, ref=REF, **kw):
    return broker.place_bet(market_id="1.100", selection_id=7, side="BACK", price=2.0, size=2.0, customer_ref=ref, **kw)


def test_sim_rejects_duplicate_customer_ref():
    broker = _sim()
    assert _place(broker)["status"] == "SUCCESS"
    dup = _place(broker)
    assert dup["status"] == "FAILURE" and dup["errorCode"] == "DUPLICATE_TRANSACTION"
    assert len(broker.state.orders) == 1, "seconda richiesta con lo stesso customerRef piazzata in SIM"


def test_sim_duplicate_window_is_sixty_seconds():
    broker = _sim()
    now = [1000.0]
    broker._customer_ref_clock = lambda: now[0]
    _place(broker)
    now[0] += 59.0
    assert _place(broker)["errorCode"] == "DUPLICATE_TRANSACTION"
    now[0] += 2.0                                       # oltre i 60 s dalla prima richiesta
    assert _place(broker)["status"] == "SUCCESS"
    assert len(broker.state.orders) == 2


@pytest.mark.parametrize("ref", ["", "ref con spazi", "x" * 33])
def test_sim_never_dedups_a_ref_that_live_would_not_send(ref):
    # LIVE omette un customerRef assente o non conforme: nessun de-dup, il SIM uguale
    broker = _sim()
    assert _place(broker, ref=ref)["status"] == "SUCCESS" and _place(broker, ref=ref)["status"] == "SUCCESS"
    assert len(broker.state.orders) == 2


def test_sim_batch_shares_one_request_ref_and_a_repeated_batch_is_rejected():
    broker = _sim()
    legs = [{"selection_id": 7, "side": "BACK", "price": 2.0, "size": 1.0},
            {"selection_id": 7, "side": "BACK", "price": 2.0, "size": 1.5}]
    first = broker.place_orders(market_id="1.100", instructions=legs, customer_ref=REF)
    assert [r["status"] for r in first["instructionReports"]] == ["SUCCESS", "SUCCESS"]
    again = broker.place_orders(market_id="1.100", instructions=legs, customer_ref=REF)
    assert again["status"] == "FAILURE" and again["errorCode"] == "DUPLICATE_TRANSACTION"
    assert len(broker.state.orders) == 2


def test_sim_exposes_the_customer_order_ref_and_keeps_it_on_replace():
    broker = _sim()
    res = _place(broker, ref="txn-1", customer_order_ref=REF)
    bet_id = res["instructionReports"][0]["betId"]
    rows = broker.get_current_orders(["1.100"])
    assert [r["customerOrderRef"] for r in rows] == [REF]
    broker.update_market_book("1.100", {"marketId": "1.100", "status": "OPEN", "runners": [
        {"selectionId": 7, "ex": {"availableToBack": [], "availableToLay": []}}]})
    resting = _place(broker, ref="txn-2", customer_order_ref="ord-2")["instructionReports"][0]["betId"]
    broker.replace_orders(market_id="1.100", bet_id=resting, new_price=3.0)
    refs = {r["customerOrderRef"] for r in broker.get_current_orders(["1.100"]) if r["betId"] != bet_id}
    assert refs == {"ord-2"}, "il replace deve conservare il customerOrderRef dell'ordine sostituito"


def test_router_reports_a_sim_duplicate_as_unknown_outcome_like_live():
    # LIVE: DUPLICATE_TRANSACTION => order_unknown (la prima bet e' viva). Il SIM non deve dire FAILURE definitivo
    broker = _sim()
    router = OrderRouter(_Service(broker))
    assert router.place(_payload())["placed"] is True
    dup = router.place(_payload())
    assert dup["order_unknown"] is True and dup["status"] == "AMBIGUOUS" and dup["placed"] is False


class _SagaDB:
    def __init__(self):
        self.sagas, self.by_logical = {}, {}

    def create_order_saga(self, **kw):
        self.sagas[kw["customer_ref"]] = dict(kw)
        self.by_logical[kw["logical_key"]] = self.sagas[kw["customer_ref"]]

    def get_order_saga(self, ref):
        return self.sagas.get(ref)

    def get_order_saga_by_logical_key(self, key):
        return self.by_logical.get(key)

    def update_order_saga(self, **kw):
        self.sagas[kw["customer_ref"]].update(kw)


def test_order_manager_records_a_sim_duplicate_as_ambiguous():
    broker = _sim()
    _place(broker)                                      # stessa richiesta gia' passata (es. retry dopo un timeout)
    db = _SagaDB()
    manager = OrderManager(db=db, client_getter=lambda: broker, sleep_fn=lambda _: None)
    res = manager.place_order({"market_id": "1.100", "selection_id": 7, "bet_type": "BACK", "price": 2.0,
                               "stake": 2.0, "simulation_mode": True, "customer_ref": REF})
    assert res["ok"] is False
    assert db.sagas[REF]["status"] == OrderStatus.AMBIGUOUS.value, db.sagas[REF]
    assert len(broker.state.orders) == 1


# ------------------------------------------------------------------------------------- attribuzione in riconciliazione
class _DB:
    def __init__(self):
        self.markers, self.decisions = {}, []

    def persist_decision_log(self, batch_id, entries):
        self.decisions.extend(entries)

    def set_reconcile_marker(self, batch_id, value):
        self.markers[batch_id] = value

    def get_reconcile_marker(self, batch_id):
        return self.markers.get(batch_id)

    def get_pending_sagas(self):
        return []


class _Batches:
    def __init__(self, leg):
        self.batches = {"B1": {"batch_id": "B1", "status": "LIVE", "market_id": "1.100"}}
        self.legs = {"B1": [dict(leg)]}

    def get_batch(self, batch_id):
        return self.batches.get(batch_id)

    def get_batch_legs(self, batch_id):
        return self.legs.get(batch_id)

    def update_leg_status(self, batch_id, leg_index, status, **kw):
        self.legs[batch_id][leg_index]["status"] = status
        for k, v in kw.items():
            if v is not None:
                self.legs[batch_id][leg_index][k] = v

    def recompute_batch_status(self, batch_id):
        return self.batches[batch_id]

    def release_runtime_artifacts(self, **kw):
        pass

    def get_open_batches(self):
        return list(self.batches.values())

    def mark_batch_failed(self, *a, **kw):
        pass


class _Exchange:
    def __init__(self, orders):
        self.orders = orders

    def get_current_orders(self, **kw):
        return [dict(o) for o in self.orders]


def _leg(**over):
    leg = {"leg_index": 0, "status": "PLACED", "customer_ref": "REF1", "bet_id": None, "selection_id": "7",
           "market_id": "1.100", "created_at_ts": time.time() - 10_000}       # oltre ogni timeout di grazia
    leg.update(over)
    return leg


def _remote(bet_id, ref="", selection=7, matched=5.0):
    return {"betId": bet_id, "marketId": "1.100", "selectionId": selection, "customerOrderRef": ref,
            "status": "EXECUTION_COMPLETE", "sizeMatched": matched, "sizeRemaining": 0.0}


def _reconcile(leg, orders):
    bm = _Batches(leg)
    engine = ReconciliationEngine(db=_DB(), batch_manager=bm, client_getter=lambda: _Exchange(orders))
    engine.reconcile_batch("B1")
    return bm.legs["B1"][0], engine


def test_reconcile_never_matches_foreign_order_by_market_selection():
    # D-582-01: ordine diretto con risposta persa (nessun bet_id) e ordine ALTRUI abbinato sullo stesso runner
    leg, _ = _reconcile(_leg(), [_remote("FOREIGN", ref="other-bot")])
    assert leg.get("bet_id") in (None, ""), "ordine altrui adottato per market::selection"
    assert leg["status"] != "MATCHED"


def test_unattributable_order_on_the_selection_holds_the_leg_never_fails_it():
    # l'ordine sul runner potrebbe essere il nostro (piazzato senza ref prima di F-13): mai FAILED alla cieca
    leg, engine = _reconcile(_leg(), [_remote("FOREIGN", ref="")])
    assert leg["status"] == "PLACED", "gamba chiusa FAILED con un ordine non attribuibile sulla selezione"


def test_without_any_order_on_the_selection_the_timeout_still_applies():
    # comportamento invariato: nessun ordine sul runner => la gamba scaduta si chiude
    leg, _ = _reconcile(_leg(), [_remote("OTHER-RUNNER", selection=99)])
    assert leg["status"] == "FAILED"


def test_customer_order_ref_attributes_a_lost_response_order():
    leg, _ = _reconcile(_leg(), [_remote("B-REAL", ref="REF1")])
    assert leg["status"] == "MATCHED" and leg["bet_id"] == "B-REAL"


def test_bet_id_is_the_primary_identity_over_the_ref():
    engine = ReconciliationEngine(db=_DB(), batch_manager=_Batches(_leg()), client_getter=lambda: _Exchange([]))
    by_bet_order, by_ref_order = _remote("B1", ref="X"), _remote("B2", ref="REF1")
    found = engine._lookup_remote_order(_leg(bet_id="B1"), *engine._build_exchange_indices([by_ref_order,
                                                                                              by_bet_order]))
    assert found["betId"] == "B1"
