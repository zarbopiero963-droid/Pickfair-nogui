import pytest

from services.telegram_bet_resolver import TelegramBetResolver


class _Client:
    def list_soccer_events(self, live_only=True):
        _ = live_only
        return [{"event_id": "E1", "event_name": "Roma v Milan"}]

    def list_event_markets(self, event_id=None, **_kwargs):
        assert event_id == "E1"
        return [{"market_id": "1.100", "market_name": "Over/Under 2.5 Goals"}]

    def get_market_book(self, market_id):
        assert market_id == "1.100"
        return {
            "runners": [
                {"selectionId": 99, "runnerName": "Over 2.5", "ex": {"availableToBack": [{"price": 2.02, "size": 100}]}}
            ]
        }


@pytest.mark.integration
def test_bet_resolver_live_sim_input_output_parity():
    resolver = TelegramBetResolver(client_getter=lambda: _Client())
    base = {"event_name": "Roma v Milan", "home_score": 1, "away_score": 1, "minute": 55, "signal_type": "OVER_SUCCESSIVO"}

    live = resolver.resolve({**base, "simulation_mode": False})
    sim = resolver.resolve({**base, "simulation_mode": True})

    assert live is not None and sim is not None
    assert live.market_id == sim.market_id
    assert live.selection_id == sim.selection_id
    assert live.signal_type == sim.signal_type
    assert live.to_order_payload(3.0, simulation_mode=False).keys() == sim.to_order_payload(3.0, simulation_mode=True).keys()


@pytest.mark.integration
def test_bet_resolver_copy_messages_skipped_in_both_modes():
    resolver = TelegramBetResolver(client_getter=lambda: _Client())
    # copy-like payload with no resolvable teams should be rejected deterministically in both modes
    signal = {"message": "COPY TRADE REF#1", "simulation_mode": False}
    assert resolver.resolve(signal) is None
    assert resolver.resolve({**signal, "simulation_mode": True}) is None
