import pytest

from services.telegram_bet_resolver import TelegramBetResolver

EVENT_ID = "E1"
MARKET_ID = "1.100"
SELECTION_ID = 99
EVENT_NAME = "Roma v Milan"
MARKET_NAME = "Over/Under 2.5 Goals"
RUNNER_NAME = "Over 2.5"
SIGNAL_TYPE = "OVER_SUCCESSIVO"


def _signal(**overrides):
    base = {"event_name": EVENT_NAME, "home_score": 1, "away_score": 1, "minute": 55, "signal_type": SIGNAL_TYPE}
    base.update(overrides)
    return base


class _Client:
    def __init__(self, *, market_status="OPEN", include_prices=True):
        self.market_status = market_status
        self.include_prices = include_prices

    def list_soccer_events(self, live_only=True):
        _ = live_only
        return [{"event_id": EVENT_ID, "event_name": EVENT_NAME}]

    def list_event_markets(self, event_id=None, **_kwargs):
        assert event_id == EVENT_ID
        return [{"market_id": MARKET_ID, "market_name": MARKET_NAME}]

    def get_market_book(self, market_id):
        assert market_id == MARKET_ID
        runner = {"selectionId": SELECTION_ID, "runnerName": RUNNER_NAME, "ex": {}}
        if self.include_prices:
            runner["ex"] = {"availableToBack": [{"price": 2.02, "size": 100}]}
        return {"status": self.market_status, "runners": [runner]}


def test_resolve_deterministic_payload_and_semantics_preserved():
    resolver = TelegramBetResolver(client_getter=lambda: _Client())
    resolved = resolver.resolve(_signal())
    assert resolved is not None
    assert resolved.market_id == MARKET_ID
    assert resolved.selection_id == SELECTION_ID
    assert resolved.bet_type == "BACK"
    assert resolved.target_line == 2.5


def test_to_order_payload_live_and_simulation_have_same_invariants_and_real_mode_difference():
    resolver = TelegramBetResolver(client_getter=lambda: _Client())
    resolved = resolver.resolve(_signal())
    assert resolved is not None

    live_payload = resolved.to_order_payload(3.0, simulation_mode=False)
    sim_payload = resolved.to_order_payload(3.0, simulation_mode=True)

    for key in ["market_id", "selection_id", "bet_type", "price", "stake", "source"]:
        assert live_payload[key] == sim_payload[key]

    assert live_payload["market_id"] == MARKET_ID
    assert live_payload["selection_id"] == SELECTION_ID
    assert live_payload["bet_type"] == "BACK"
    assert live_payload["price"] == pytest.approx(2.02)
    assert live_payload["stake"] == pytest.approx(3.0)
    assert live_payload["source"] == "TELEGRAM"

    # Current production mode-specific difference is only simulation_mode flag.
    assert live_payload["simulation_mode"] is False
    assert sim_payload["simulation_mode"] is True


def test_resolve_invalid_or_incomplete_signal_fails_closed_no_order_payload():
    resolver = TelegramBetResolver(client_getter=lambda: _Client())
    assert resolver.resolve({"message": "COPY TRADE ONLY"}) is None
    assert resolver.resolve({"event_name": EVENT_NAME, "signal_type": "UNKNOWN"}) is None


@pytest.mark.parametrize("market_status", ["SUSPENDED", "CLOSED", "HALTED"])
def test_resolve_rejects_non_tradable_market_in_both_live_and_sim(market_status):
    resolver = TelegramBetResolver(client_getter=lambda: _Client(market_status=market_status))
    assert resolver.resolve({**_signal(), "simulation_mode": False}) is None
    assert resolver.resolve({**_signal(), "simulation_mode": True}) is None


def test_resolve_missing_prices_fails_closed():
    resolver = TelegramBetResolver(client_getter=lambda: _Client(include_prices=False))
    assert resolver.resolve(_signal()) is None
