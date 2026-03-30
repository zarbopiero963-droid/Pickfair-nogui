import pytest


@pytest.mark.guardrail
def test_betfair_client_public_contract():
    from betfair_client import BetfairClient

    required_methods = [
        "login",
        "logout",
        "get_account_funds",
        "calculate_cashout",
        "get_market_book",
        "place_bet",
        "status",
    ]

    for name in required_methods:
        assert hasattr(BetfairClient, name), f"Metodo mancante: {name}"