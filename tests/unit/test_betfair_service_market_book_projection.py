"""Test threading di ``include_prices`` in BetfairService.get_market_book_snapshot.

Attivazione LIVE best-price (B6.2): il flag ``include_prices`` deve arrivare al
client LIVE (per chiedere le ladder EX_BEST_OFFERS) ed essere ininfluente in
SIMULATION (il broker simulato restituisce gia' un book completo).
"""
import pytest

from services.betfair_service import BetfairService

pytestmark = pytest.mark.unit


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


class _Client:
    def __init__(self):
        self.calls = []

    def get_market_book(self, market_id, *, include_prices=False):
        self.calls.append((market_id, include_prices))
        return {"status": "OPEN", "runners": []}


class _SimBroker:
    def __init__(self):
        self.calls = []

    def get_market_book(self, market_id):
        self.calls.append(market_id)
        return {"status": "OPEN", "runners": []}


def _make_service():
    return BetfairService(_Settings())


def test_live_threads_include_prices_true_to_client():
    svc = _make_service()
    svc.simulation_mode = False
    svc.client = _Client()
    svc.get_market_book_snapshot("1.234", include_prices=True)
    assert svc.client.calls == [("1.234", True)]


def test_live_default_does_not_request_prices():
    svc = _make_service()
    svc.simulation_mode = False
    svc.client = _Client()
    svc.get_market_book_snapshot("1.234")
    assert svc.client.calls == [("1.234", False)]


def test_simulation_ignores_include_prices():
    # In SIMULATION il flag non viene propagato (il broker non lo accetta):
    # il book completo arriva comunque, nessun TypeError.
    svc = _make_service()
    svc.simulation_mode = True
    svc.simulation_broker = _SimBroker()
    book = svc.get_market_book_snapshot("1.234", include_prices=True)
    assert book == {"status": "OPEN", "runners": []}
    assert svc.simulation_broker.calls == ["1.234"]
