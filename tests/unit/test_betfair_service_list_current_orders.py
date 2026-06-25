"""
Tests for BetfairService.list_current_orders (B3 / Fase 1.3).

Verifies the ghost-order detection facade:
- simulation mode delegates to the simulation broker and returns a dict list
- LIVE is fail-closed: a known-invalid session or a missing live client RAISES
  (never a silent empty list that ghost detection would read as "no orders")
- a SESSION_EXPIRED during the live fetch routes bounded recovery and re-raises
- a successful live fetch returns the order dicts (non-dicts filtered out)
"""

import pytest

from services.betfair_service import BetfairService
from simulation_broker import SimulationBroker


class _Settings:
    def load_betfair_config(self):
        class Cfg:
            username = "user"
            app_key = "key"
            certificate = "cert"
            private_key = "pk"
        return Cfg()

    def load_password(self):
        return "pw"


class _OrdersBroker:
    def __init__(self, orders=None, raises=None):
        self._orders = orders if orders is not None else []
        self._raises = raises
        self.calls = []

    def get_current_orders(self, market_ids=None):
        self.calls.append(market_ids)
        if self._raises:
            raise RuntimeError(self._raises)
        return self._orders


def _make_service():
    return BetfairService(_Settings())


@pytest.mark.unit
def test_simulation_delegates_to_broker_and_returns_list():
    svc = _make_service()
    svc.simulation_mode = True
    orders = [{"betId": "1", "marketId": "1.100"}]
    svc.simulation_broker = _OrdersBroker(orders=orders)

    out = svc.list_current_orders(["1.100"])

    # Original keys preserved; snake_case bet_id/order_id added for the startup
    # recovery store (state_recovery._is_missing_in_db keys on those).
    assert len(out) == 1
    assert out[0]["betId"] == "1"
    assert out[0]["marketId"] == "1.100"
    assert out[0]["bet_id"] == "1"
    assert out[0]["order_id"] == "1"
    assert svc.simulation_broker.calls == [["1.100"]]


@pytest.mark.unit
def test_simulation_without_broker_returns_empty_list():
    svc = _make_service()
    svc.simulation_mode = True
    svc.simulation_broker = None

    assert svc.list_current_orders() == []


@pytest.mark.unit
def test_simulation_filters_non_dict_entries():
    svc = _make_service()
    svc.simulation_mode = True
    svc.simulation_broker = _OrdersBroker(orders=[{"betId": "1"}, None, "x"])

    out = svc.list_current_orders()
    assert len(out) == 1
    assert out[0]["betId"] == "1" and out[0]["bet_id"] == "1"


@pytest.mark.unit
def test_live_session_invalid_raises_fail_closed():
    svc = _make_service()
    svc.simulation_mode = False
    svc._session_invalid = True
    svc._session_invalid_reason = "BOOM"
    # Even with a usable client, an invalid session must fail closed.
    svc.client = _OrdersBroker(orders=[{"betId": "1"}])

    with pytest.raises(RuntimeError, match="LIVE_BLOCKED_SESSION_INVALID"):
        svc.list_current_orders()


@pytest.mark.unit
def test_live_without_client_raises_fail_closed():
    svc = _make_service()
    svc.simulation_mode = False
    svc._session_invalid = False
    svc.client = None

    with pytest.raises(RuntimeError, match="NO_LIVE_CLIENT"):
        svc.list_current_orders()


@pytest.mark.unit
def test_live_success_returns_order_dicts():
    svc = _make_service()
    svc.simulation_mode = False
    svc._session_invalid = False
    orders = [{"betId": "1", "marketId": "1.100"}, None]
    svc.client = _OrdersBroker(orders=orders)

    out = svc.list_current_orders()

    assert len(out) == 1
    assert out[0]["betId"] == "1"
    assert out[0]["marketId"] == "1.100"
    assert out[0]["bet_id"] == "1" and out[0]["order_id"] == "1"


@pytest.mark.unit
def test_live_session_expired_routes_recovery_and_reraises():
    svc = _make_service()
    svc.simulation_mode = False
    svc._session_invalid = False
    svc.client = _OrdersBroker(raises="SESSION_EXPIRED")

    recovery = {"called": False, "reason": ""}

    def _fake_expiry(reason="", abort_if=None):
        recovery["called"] = True
        recovery["reason"] = reason
        return {"ok": True}

    svc.handle_session_expiry = _fake_expiry

    with pytest.raises(RuntimeError, match="SESSION_EXPIRED"):
        svc.list_current_orders()

    assert recovery["called"] is True
    assert "SESSION_EXPIRED" in recovery["reason"]


@pytest.mark.unit
def test_simulation_broker_get_current_orders_is_a_filtered_dict_list():
    # The real SimulationBroker must honour the same list-of-dicts contract as
    # BetfairClient.get_current_orders (the reconciliation engine calls the
    # active broker uniformly via get_client()).
    broker = SimulationBroker()
    broker.place_bet(market_id="1.100", selection_id=7, side="BACK", price=2.0, size=5.0)

    orders = broker.get_current_orders(["1.100"])

    assert isinstance(orders, list)
    assert orders and all(isinstance(o, dict) for o in orders)
    assert orders[0]["marketId"] == "1.100"
    # Market filter excludes other markets.
    assert broker.get_current_orders(["9.999"]) == []


@pytest.mark.unit
def test_simulation_broker_get_current_orders_excludes_cancelled():
    # Live listCurrentOrders never returns cancelled orders; the sim wrapper
    # must match, or a cancelled order would be mis-detected as a remote ghost.
    broker = SimulationBroker()
    broker.place_bet(market_id="1.100", selection_id=7, side="BACK", price=2.0, size=5.0)
    broker.place_bet(market_id="1.100", selection_id=8, side="BACK", price=3.0, size=5.0)

    # list_current_orders still exposes every order (cancelled included)...
    broker.cancel_orders(market_id="1.100")
    assert broker.list_current_orders(["1.100"])["currentOrders"]  # not empty

    # ...but get_current_orders drops the now-terminal (cancelled) orders.
    current = broker.get_current_orders(["1.100"])
    assert all(o.get("status") != "CANCELLED" for o in current)


@pytest.mark.unit
def test_simulation_broker_rows_carry_customer_ref():
    # The reconciliation engine matches remote orders by customer ref first;
    # the sim rows must expose it (customerOrderRef), like the live payload.
    broker = SimulationBroker()
    broker.place_bet(
        market_id="1.100", selection_id=7, side="BACK", price=2.0, size=5.0,
        customer_ref="REF-123",
    )

    row = broker.get_current_orders(["1.100"])[0]
    assert row["customerOrderRef"] == "REF-123"


@pytest.mark.unit
def test_simulation_broker_cancel_order_cancels_single_bet():
    broker = SimulationBroker()
    res = broker.place_bet(
        market_id="1.100", selection_id=7, side="BACK", price=2.0, size=5.0
    )
    bet_id = res.get("betId") or res.get("bet_id") or res["instructionReports"][0]["betId"]

    broker.cancel_order(bet_id=bet_id)

    # Cancelled order is no longer a current order.
    assert broker.get_current_orders(["1.100"]) == []


@pytest.mark.unit
def test_service_normalizes_customer_ref_for_startup_recovery():
    # Live rows use camelCase customerOrderRef; the startup store keys on
    # snake_case customer_ref — the service must mirror it.
    svc = _make_service()
    svc.simulation_mode = False
    svc._session_invalid = False
    svc.client = _OrdersBroker(orders=[
        {"betId": "B1", "marketId": "1.1", "customerOrderRef": "REF-9"},
    ])

    row = svc.list_current_orders()[0]
    assert row["bet_id"] == "B1" and row["order_id"] == "B1"
    assert row["customer_ref"] == "REF-9"
    assert row["customerOrderRef"] == "REF-9"  # original preserved


@pytest.mark.unit
def test_live_generic_error_reraises_without_recovery():
    svc = _make_service()
    svc.simulation_mode = False
    svc._session_invalid = False
    svc.client = _OrdersBroker(raises="TIMEOUT")

    recovery = {"called": False}

    def _fake_expiry(reason="", abort_if=None):
        recovery["called"] = True
        return {"ok": True}

    svc.handle_session_expiry = _fake_expiry

    with pytest.raises(RuntimeError, match="TIMEOUT"):
        svc.list_current_orders()

    assert recovery["called"] is False
