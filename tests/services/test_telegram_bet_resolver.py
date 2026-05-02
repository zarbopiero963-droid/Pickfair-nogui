import pytest

from services.telegram_bet_resolver import TelegramBetResolver


class _Client:
    def __init__(self, *, market_status="OPEN", include_prices=True):
        self.market_status = market_status
        self.include_prices = include_prices

    def list_soccer_events(self, live_only=True):
        _ = live_only
        return [{"event_id": "E1", "event_name": "Roma v Milan"}]

    def list_event_markets(self, event_id=None, **_kwargs):
        assert event_id == "E1"
        return [{"market_id": "1.100", "market_name": "Over/Under 2.5 Goals"}]

    def get_market_book(self, market_id):
        assert market_id == "1.100"
        runner = {"selectionId": 99, "runnerName": "Over 2.5", "ex": {}}
        if self.include_prices:
            runner["ex"] = {"availableToBack": [{"price": 2.02, "size": 100}]}
        return {"status": self.market_status, "runners": [runner]}


def test_resolve_deterministic_payload_and_semantics_preserved():
    resolver = TelegramBetResolver(client_getter=lambda: _Client())
    signal = {
        "event_name": "Roma v Milan",
        "home_score": 1,
        "away_score": 1,
        "minute": 55,
        "signal_type": "OVER_SUCCESSIVO",
    }

    resolved = resolver.resolve(signal)
    assert resolved is not None
    assert resolved.market_id == "1.100"
    assert resolved.selection_id == 99
    assert resolved.bet_type == "BACK"
    assert resolved.target_line == 2.5

    payload = resolved.to_order_payload(3.0, simulation_mode=False)
    assert payload["market_id"] == "1.100"
    assert payload["selection_id"] == 99
    assert payload["bet_type"] == "BACK"
    assert payload["price"] == pytest.approx(2.02)
    assert payload["stake"] == pytest.approx(3.0)
    assert payload["source"] == "TELEGRAM"


def test_resolve_invalid_or_incomplete_signal_fails_closed_no_order_payload():
    resolver = TelegramBetResolver(client_getter=lambda: _Client())

    assert resolver.resolve({"message": "COPY TRADE ONLY"}) is None
    assert resolver.resolve({"event_name": "Roma v Milan", "signal_type": "UNKNOWN"}) is None


@pytest.mark.parametrize("market_status", ["SUSPENDED", "CLOSED", "HALTED"])
def test_resolve_rejects_non_tradable_market_in_both_live_and_sim(market_status):
    resolver = TelegramBetResolver(client_getter=lambda: _Client(market_status=market_status))
    base = {"event_name": "Roma v Milan", "home_score": 1, "away_score": 1, "minute": 55, "signal_type": "OVER_SUCCESSIVO"}

    assert resolver.resolve({**base, "simulation_mode": False}) is None
    assert resolver.resolve({**base, "simulation_mode": True}) is None


def test_resolve_missing_prices_fails_closed():
    resolver = TelegramBetResolver(client_getter=lambda: _Client(include_prices=False))
    signal = {"event_name": "Roma v Milan", "home_score": 1, "away_score": 1, "minute": 55, "signal_type": "OVER_SUCCESSIVO"}
    assert resolver.resolve(signal) is None
