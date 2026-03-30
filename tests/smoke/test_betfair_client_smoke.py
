import pytest


@pytest.mark.smoke
def test_import_betfair_client_module():
    import betfair_client  # noqa: F401


@pytest.mark.smoke
def test_betfair_client_has_required_public_methods():
    from betfair_client import BetfairClient

    for name in [
        "login",
        "logout",
        "calculate_cashout",
        "get_market_book",
        "place_bet",
        "status",
    ]:
        assert hasattr(BetfairClient, name), f"Metodo pubblico mancante: {name}"