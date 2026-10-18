"""PR #512, ciclo 6 del fix loop (in attesa di deroga owner) — rilievi confermati sul head ad13383.

- Sol c.6242912622: retry dello stesso `customer_ref` con `store.load` fallito conserva la riserva preesistente
  (test in `tests/test_pr512_review_cycle4.py::test_store_load_failure_on_a_retry_keeps_the_pre_existing_reservation`);
- Codex r4304867227 (P1): costruzione pigra dell'esecutore Microstake serializzata: due chiamate concorrenti
  (Quick Bet + hedge del cashout) ottengono LO STESSO esecutore, una sola mappa di riserve, un solo store.
"""
import threading
from types import SimpleNamespace

import core.microstake_sim_port as sim_port
from test_microstake_engine_native import _engine


def test_concurrent_first_microstake_calls_build_one_single_executor(monkeypatch):
    engine, broker, _ = _engine()
    assert engine.microstake_executor is None
    real_build = sim_port.build_native_microstake_executor
    built = []
    gate = threading.Barrier(2, timeout=5)

    def slow_build(*args, **kwargs):
        # entrambi i thread sono "dentro" la costruzione solo se nessun lock li serializza
        try:
            gate.wait()
        except threading.BrokenBarrierError:
            pass
        ex = real_build(*args, **kwargs)
        built.append(ex)
        return ex

    monkeypatch.setattr(sim_port, "build_native_microstake_executor", slow_build)
    results = []

    def worker():
        results.append(engine.native_microstake_executor())

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert len(results) == 2 and all(r is not None for r in results)
    assert results[0] is results[1], "due esecutori Microstake distinti: riserve e store non condivisi"
    assert len(built) == 1, f"esecutore costruito {len(built)} volte"
    assert engine.microstake_executor is results[0]
    assert results[0].port.broker is broker


# ---------------------------------------------------------------- Codex r4304867229 (P1) + decisione owner D-C (C1):
# un payload `currentOrders` malformato non diventa mai "nessun ordine": errore esplicito (fail-closed), cosi'
# `_rebuild_open_positions` finisce in EXCHANGE_ORDERS_UNAVAILABLE invece di chiudere posizioni SUBMITTED vive.
import pytest

from tests.unit.test_betfair_client import DummySession, _RPCResp


def _client():
    from betfair_client import BetfairClient
    c = BetfairClient(username="user", app_key="app", cert_pem="cert.pem", key_pem="key.pem", session=DummySession())
    c.session_token = "TOK"
    return c


@pytest.mark.parametrize("payload", ["garbage", {"betId": "1"}, 42, [{"betId": "1"}, "not-a-row"], [None]])
def test_client_rejects_malformed_current_orders_payload(payload):
    client = _client()
    client.session.post = lambda *a, **k: _RPCResp([{"jsonrpc": "2.0", "id": 1, "result": {"currentOrders": payload, "moreAvailable": False}}])
    with pytest.raises(RuntimeError, match="CURRENT_ORDERS_MALFORMED"):
        client.get_current_orders()


def test_client_still_returns_well_formed_rows():
    client = _client()
    client.session.post = lambda *a, **k: _RPCResp([{"jsonrpc": "2.0", "id": 1, "result": {"currentOrders": [{"betId": "9", "marketId": "1.1"}], "moreAvailable": False}}])
    assert client.get_current_orders() == [{"betId": "9", "marketId": "1.1"}]


class _BrokerReturning:
    def __init__(self, value):
        self.value = value

    def get_current_orders(self, market_ids=None):
        return self.value


@pytest.mark.parametrize("sim", [True, False])
@pytest.mark.parametrize("value", ["garbage", {"currentOrders": []}, [{"betId": "1"}, "row"], [None]])
def test_service_propagates_malformed_current_orders_instead_of_filtering(sim, value):
    from services.betfair_service import BetfairService
    svc = BetfairService(SimpleNamespace())
    svc.simulation_mode = sim
    svc.connected = True
    if sim:
        svc.simulation_broker = _BrokerReturning(value)
    else:
        svc.client = _BrokerReturning(value)
    svc._session_invalid = False
    with pytest.raises(RuntimeError, match="CURRENT_ORDERS_MALFORMED"):
        svc.list_current_orders()


def test_runtime_rebuild_treats_malformed_exchange_payload_as_unavailable():
    """Percorso reale: il servizio solleva => _rebuild_open_positions non chiude nulla e resta ok=False."""
    from core.runtime_controller import RuntimeController
    rc = RuntimeController.__new__(RuntimeController)
    svc = SimpleNamespace(list_current_orders=lambda *a, **k: (_ for _ in ()).throw(RuntimeError("CURRENT_ORDERS_MALFORMED: x")))
    rc.betfair_service = svc
    calls = []
    rc.duplication_guard = SimpleNamespace(acquire=lambda k: calls.append(k))
    rc.table_manager = SimpleNamespace(snapshot=lambda: [{"table_id": 1, "current_event_key": ""}], activate=lambda **kw: calls.append(kw))
    rc._uncertain_reservations_map = lambda: {}
    rc._open_order_candidates = lambda: ([{"customer_ref": "r1", "status": "SUBMITTED", "bet_id": "", "market_id": "1.1", "selection_id": 7,
                                           "side": "BACK", "stake": 2.0, "price": 2.0, "event_key": "1.1|7|BACK", "table_id": 1, "meta": {}}], [])
    result = rc._rebuild_open_positions()
    assert result["ok"] is False and result["reason"].startswith("EXCHANGE_ORDERS_UNAVAILABLE")
    assert result["closed"] == []


# ---------------------------------------------------------------- riesame indipendente ciclo 6 (GUARDRAIL_GAP):
# un adapter exchange SENZA `has_reservation` non deve ereditare il difetto Sol: su store illeggibile la riserva
# si conserva (fail-closed), si scioglie solo quando l'adapter sa dire che la riserva e' davvero nuova.
def test_adapter_without_has_reservation_keeps_the_reservation_on_unreadable_store():
    from decimal import Decimal as D
    from core.microstake_safe import MicrostakeCoordinator, MicrostakeRequest, Side, StatefulExchangeSimulator
    from core.microstake_sim_port import SagaStoreError

    class _NoHasReservation(StatefulExchangeSimulator):
        has_reservation = None          # metodo assente (adapter minimale)

    class _BrokenStore:
        def load(self, ref):
            raise SagaStoreError("unreadable")

        def save(self, *a, **k):
            raise AssertionError("mai chiamato")

    exchange = _NoHasReservation()
    coordinator = MicrostakeCoordinator(exchange=exchange, store=_BrokenStore())
    req = MicrostakeRequest(market_id="1.1", selection_id=7, side=Side.BACK, economic_stake=D("0.10"),
                            target_price=D("2.0"), customer_ref="ref-1")
    with pytest.raises(SagaStoreError):
        coordinator.execute(req, stub_price=D("2.02"), transient_cap=D("10"))
    assert exchange._state["reservations"].get("ref-1") is not None, "riserva sciolta da un adapter senza has_reservation"
