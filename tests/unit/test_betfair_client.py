import pytest


class DummySession:
    pass


@pytest.fixture
def client():
    from betfair_client import BetfairClient

    return BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=DummySession(),
    )


@pytest.mark.unit
def test_logout_is_idempotent_and_clears_state(client):
    client.session_token = "TOKEN"
    client.session_expiry = "EXPIRY"
    client.connected = True

    r1 = client.logout()
    r2 = client.logout()

    assert r1 == {"ok": True, "logged_out": True}
    assert r2 == {"ok": True, "logged_out": True}
    assert client.session_token == ""
    assert client.session_expiry == ""
    assert client.connected is False


@pytest.mark.unit
def test_safe_side_normalizes_values(client):
    assert client._safe_side("back") == "BACK"
    assert client._safe_side("lay") == "LAY"
    assert client._safe_side("weird") == "BACK"
    assert client._safe_side(None) == "BACK"


@pytest.mark.unit
def test_safe_float_and_safe_int(client):
    assert client._safe_float("2.5") == 2.5
    assert client._safe_float("x", 7.0) == 7.0
    assert client._safe_int("8") == 8
    assert client._safe_int("bad", 3) == 3


@pytest.mark.unit
def test_classify_error_transient(client):
    assert client._classify_error("TIMEOUT") == "TRANSIENT"
    assert client._classify_error("NETWORK_ERROR: boom") == "TRANSIENT"
    assert client._classify_error("HTTP_500") == "TRANSIENT"


@pytest.mark.unit
def test_classify_error_permanent(client):
    assert client._classify_error("SESSION_EXPIRED") == "PERMANENT"
    assert client._classify_error("INVALID_JSON") == "PERMANENT"
    assert client._classify_error("API_ERROR: INVALID_MARKET_ID") == "PERMANENT"


@pytest.mark.unit
def test_classify_error_unknown(client):
    assert client._classify_error("something_else") == "UNKNOWN"


@pytest.mark.unit
def test_status_depends_on_session_token(client):
    client.session_token = ""
    client.session_expiry = ""
    assert client.status() == {"connected": False, "expiry": ""}

    client.session_token = "TOK"
    client.session_expiry = "EXP"
    assert client.status() == {"connected": True, "expiry": "EXP"}


@pytest.mark.unit
def test_calculate_cashout_back_path(client):
    out = client.calculate_cashout(10, 2.0, 1.5, "BACK")

    assert out["cashout_stake"] > 0
    assert out["side_to_place"] == "LAY"
    assert "profit_if_win" in out
    assert "profit_if_lose" in out


@pytest.mark.unit
def test_calculate_cashout_lay_path(client):
    out = client.calculate_cashout(10, 3.0, 2.0, "LAY")

    assert out["cashout_stake"] > 0
    assert out["side_to_place"] == "BACK"
    assert "profit_if_win" in out
    assert "profit_if_lose" in out


@pytest.mark.unit
def test_calculate_cashout_invalid_inputs_returns_safe_zero(client):
    out = client.calculate_cashout(0, 1.0, 0.0, "BACK")

    assert out["cashout_stake"] == 0.0
    assert out["profit_if_win"] == 0.0
    assert out["profit_if_lose"] == 0.0


@pytest.mark.unit
def test_place_bet_invalid_market_id_fails_fast(client):
    with pytest.raises(RuntimeError, match="INVALID_MARKET_ID"):
        client.place_bet(
            market_id="",
            selection_id=1,
            side="BACK",
            price=2.0,
            size=2.0,
        )


@pytest.mark.unit
def test_place_bet_invalid_selection_id_fails_fast(client):
    with pytest.raises(RuntimeError, match="INVALID_SELECTION_ID"):
        client.place_bet(
            market_id="1.100",
            selection_id=0,
            side="BACK",
            price=2.0,
            size=2.0,
        )


@pytest.mark.unit
def test_place_bet_invalid_price_fails_fast(client):
    with pytest.raises(RuntimeError, match="INVALID_PRICE"):
        client.place_bet(
            market_id="1.100",
            selection_id=1,
            side="BACK",
            price=1.0,
            size=2.0,
        )


@pytest.mark.unit
def test_place_bet_invalid_size_fails_fast(client):
    with pytest.raises(RuntimeError, match="INVALID_SIZE"):
        client.place_bet(
            market_id="1.100",
            selection_id=1,
            side="BACK",
            price=2.0,
            size=0.0,
        )