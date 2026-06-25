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

class _KAResp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


@pytest.mark.unit
def test_keep_alive_hits_keepalive_endpoint_with_token(client):
    captured = {}

    def _post(url, headers=None, timeout=None, **kw):
        captured["url"] = url
        captured["auth"] = (headers or {}).get("X-Authentication")
        return _KAResp({"status": "SUCCESS", "token": "x"})

    client.session.post = _post
    client.session_token = "TOK"

    out = client.keep_alive()

    assert out == {"ok": True, "kept_alive": True}
    assert captured["url"] == client.KEEPALIVE_URL
    assert captured["url"].endswith("/api/keepAlive")
    assert captured["auth"] == "TOK"  # estende la sessione, non getAccountFunds


@pytest.mark.unit
def test_keep_alive_without_token_raises_session_expired(client):
    client.session_token = ""
    with pytest.raises(RuntimeError, match="SESSION_EXPIRED"):
        client.keep_alive()


@pytest.mark.unit
def test_keep_alive_non_success_status_raises_with_error_code(client):
    client.session_token = "TOK"
    client.session.post = lambda *a, **k: _KAResp(
        {"status": "FAIL", "error": "INVALID_SESSION_INFORMATION"}
    )
    with pytest.raises(RuntimeError, match="INVALID_SESSION_INFORMATION"):
        client.keep_alive()


class _RPCResp:
    """Minimal JSON-RPC response stub for _post_jsonrpc (list payload)."""

    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


@pytest.mark.unit
def test_get_current_orders_returns_list_and_sends_market_filter(client):
    import json as _json

    captured = {}

    def _post(url, headers=None, data=None, timeout=None, **kw):
        captured["url"] = url
        captured["body"] = _json.loads(data) if data else None
        return _RPCResp(
            [{"jsonrpc": "2.0", "id": 1, "result": {
                "currentOrders": [
                    {"betId": "1", "marketId": "1.100", "selectionId": 7,
                     "side": "BACK", "sizeRemaining": 2.0, "status": "EXECUTABLE"},
                ],
                "moreAvailable": False,
            }}]
        )

    client.session.post = _post
    client.session_token = "TOK"

    out = client.get_current_orders(market_ids=["1.100"])

    assert isinstance(out, list)
    assert out[0]["betId"] == "1"
    assert captured["url"] == client.BETTING_URL
    params = captured["body"][0]["params"]
    assert captured["body"][0]["method"] == "SportsAPING/v1.0/listCurrentOrders"
    assert params["marketIds"] == ["1.100"]
    # Pagination params are always sent.
    assert params["fromRecord"] == 0
    assert params["recordCount"] == client.CURRENT_ORDERS_PAGE_SIZE


@pytest.mark.unit
def test_get_current_orders_walks_all_pages_when_more_available(client):
    # moreAvailable=True must drive a follow-up page; both pages concatenate and
    # fromRecord advances. A truncated single page would fail-open ghost detect.
    import json as _json

    pages = [
        {"currentOrders": [{"betId": "1"}], "moreAvailable": True},
        {"currentOrders": [{"betId": "2"}], "moreAvailable": False},
    ]
    seen_from = []

    def _post(url, headers=None, data=None, timeout=None, **kw):
        body = _json.loads(data)
        seen_from.append(body[0]["params"]["fromRecord"])
        page = pages[len(seen_from) - 1]
        return _RPCResp([{"jsonrpc": "2.0", "id": 1, "result": page}])

    client.session.post = _post
    client.session_token = "TOK"

    out = client.get_current_orders()

    assert [o["betId"] for o in out] == ["1", "2"]
    assert seen_from == [0, 1]


@pytest.mark.unit
def test_get_current_orders_raises_when_pagination_never_terminates(client):
    # Fail-closed: a response that keeps signalling moreAvailable must raise once
    # the page cap is hit, never return a partial (silently truncated) set.
    client.session_token = "TOK"
    client.session.post = lambda *a, **k: _RPCResp(
        [{"jsonrpc": "2.0", "id": 1, "result": {
            "currentOrders": [{"betId": "x"}], "moreAvailable": True}}]
    )

    with pytest.raises(RuntimeError, match="CURRENT_ORDERS_TRUNCATED"):
        client.get_current_orders()


@pytest.mark.unit
def test_get_current_orders_empty_result_returns_empty_list(client):
    client.session_token = "TOK"
    client.session.post = lambda *a, **k: _RPCResp(
        [{"jsonrpc": "2.0", "id": 1, "result": {"moreAvailable": False}}]
    )

    out = client.get_current_orders()

    assert out == []


@pytest.mark.unit
def test_get_current_orders_propagates_session_expired(client):
    # Fail-closed: a session error MUST propagate (no silent empty list that
    # would be mistaken for "no remote orders" by ghost-order detection).
    client.session_token = "TOK"
    client.session.post = lambda *a, **k: _RPCResp(
        [{"jsonrpc": "2.0", "id": 1, "error": {"code": -32099,
          "message": "INVALID_SESSION_INFORMATION"}}]
    )

    with pytest.raises(RuntimeError, match="SESSION_EXPIRED"):
        client.get_current_orders(market_ids=["1.100"])


@pytest.mark.unit
def test_get_current_orders_raises_on_more_available_empty_page(client):
    # moreAvailable=True with no rows is an inconsistent/stale snapshot: fail
    # closed rather than return a partial set treated as complete.
    client.session_token = "TOK"
    client.session.post = lambda *a, **k: _RPCResp(
        [{"jsonrpc": "2.0", "id": 1, "result": {
            "currentOrders": [], "moreAvailable": True}}]
    )

    with pytest.raises(RuntimeError, match="CURRENT_ORDERS_TRUNCATED"):
        client.get_current_orders()


@pytest.mark.unit
def test_cancel_order_resolves_market_then_delegates(client):
    import json as _json

    posts = []

    def _post(url, headers=None, data=None, timeout=None, **kw):
        body = _json.loads(data)
        method = body[0]["method"]
        posts.append(method)
        if method.endswith("listCurrentOrders"):
            return _RPCResp([{"jsonrpc": "2.0", "id": 1, "result": {
                "currentOrders": [{"betId": "B9", "marketId": "1.222"}],
                "moreAvailable": False}}])
        # cancelOrders
        assert body[0]["params"]["marketId"] == "1.222"
        assert body[0]["params"]["instructions"] == [{"betId": "B9"}]
        return _RPCResp([{"jsonrpc": "2.0", "id": 1, "result": {
            "status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}]}}])

    client.session.post = _post
    client.session_token = "TOK"

    out = client.cancel_order(bet_id="B9")

    assert out["ok"] is True
    assert any(m.endswith("listCurrentOrders") for m in posts)
    assert any(m.endswith("cancelOrders") for m in posts)


@pytest.mark.unit
def test_cancel_order_noop_when_bet_not_current(client):
    # Bet id not among current orders → nothing to cancel (no cancelOrders call).
    client.session_token = "TOK"
    client.session.post = lambda *a, **k: _RPCResp(
        [{"jsonrpc": "2.0", "id": 1, "result": {
            "currentOrders": [{"betId": "OTHER", "marketId": "1.1"}],
            "moreAvailable": False}}]
    )

    out = client.cancel_order(bet_id="MISSING")

    assert out["ok"] is True
    assert out["status"] == "NOT_CURRENT"
    assert out["cancelled_count"] == 0


@pytest.mark.unit
def test_cancel_order_uses_given_market_without_lookup(client):
    import json as _json

    posts = []

    def _post(url, headers=None, data=None, timeout=None, **kw):
        body = _json.loads(data)
        posts.append(body[0]["method"])
        return _RPCResp([{"jsonrpc": "2.0", "id": 1, "result": {
            "status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}]}}])

    client.session.post = _post
    client.session_token = "TOK"

    out = client.cancel_order(bet_id="B1", market_id="1.5")

    assert out["ok"] is True
    # No listCurrentOrders lookup when market is supplied.
    assert all(m.endswith("cancelOrders") for m in posts)


@pytest.mark.unit
def test_cancel_order_empty_bet_id_raises(client):
    with pytest.raises(RuntimeError, match="INVALID_BET_ID"):
        client.cancel_order(bet_id="")


@pytest.mark.unit
def test_cancel_order_raises_when_cancel_fails(client):
    # cancel_orders returns ok=False on a FAILURE; the shim must RAISE so the
    # ghost-cancel path records a real failure instead of a false "cancelled"
    # (the live ghost would otherwise stay active on the exchange).
    def _post(url, headers=None, data=None, timeout=None, **kw):
        import json as _json
        method = _json.loads(data)[0]["method"]
        if method.endswith("listCurrentOrders"):
            return _RPCResp([{"jsonrpc": "2.0", "id": 1, "result": {
                "currentOrders": [{"betId": "B1", "marketId": "1.9"}],
                "moreAvailable": False}}])
        # cancelOrders → FAILURE status (cancel_orders returns ok=False)
        return _RPCResp([{"jsonrpc": "2.0", "id": 1, "result": {
            "status": "FAILURE", "errorCode": "BET_ACTION_ERROR",
            "instructionReports": []}}])

    client.session.post = _post
    client.session_token = "TOK"

    with pytest.raises(RuntimeError, match="CANCEL_ORDER_FAILED"):
        client.cancel_order(bet_id="B1")
