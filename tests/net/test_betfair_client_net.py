import json

import pytest
import requests
from requests.exceptions import Timeout


class FakeResponse:
    def __init__(self, *, status_code=200, json_data=None, text="", raise_http=False):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text
        self._raise_http = raise_http

    def raise_for_status(self):
        if self._raise_http:
            raise requests.exceptions.HTTPError(response=self)

    def json(self):
        if isinstance(self._json_data, Exception):
            raise self._json_data
        return self._json_data


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)

    def post(self, url, **kwargs):
        _ = url, kwargs
        if not self.responses:
            raise RuntimeError("no more fake responses")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class RecordingSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self.responses:
            raise RuntimeError("no more fake responses")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


@pytest.mark.integration
@pytest.mark.net
def test_network_error_then_success_recovers_for_market_book():
    from betfair_client import BetfairClient

    session = FakeSession([
        requests.exceptions.ConnectionError("net down"),
        FakeResponse(
            json_data=[{
                "result": [
                    {"marketId": "1.100", "runners": []}
                ]
            }]
        ),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=1,
    )
    client.session_token = "TOK"

    out = client.get_market_book("1.100")
    assert out["marketId"] == "1.100"


@pytest.mark.integration
@pytest.mark.net
def test_network_error_then_success_recovers_for_place_bet():
    from betfair_client import BetfairClient

    session = FakeSession([
        requests.exceptions.ConnectionError("net down"),
        FakeResponse(
            json_data=[{
                "result": {
                    "status": "SUCCESS",
                    "instructionReports": [
                        {"status": "SUCCESS", "betId": "BET1"}
                    ],
                }
            }]
        ),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=1,
    )
    client.session_token = "TOK"

    out = client.place_bet(
        market_id="1.100",
        selection_id=11,
        side="BACK",
        price=2.0,
        size=5.0,
    )
    assert out["ok"] is True


@pytest.mark.integration
@pytest.mark.net
def test_market_book_retry_multiple_timeouts_then_success():
    from betfair_client import BetfairClient

    session = FakeSession([
        Timeout(),
        Timeout(),
        FakeResponse(
            json_data=[{
                "result": [
                    {"marketId": "1.300", "runners": []}
                ]
            }]
        ),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=2,
    )
    client.session_token = "TOK"

    out = client.get_market_book("1.300")
    assert out["marketId"] == "1.300"


@pytest.mark.integration
@pytest.mark.net
def test_market_book_timeout_exhausts_retries_fails():
    from betfair_client import BetfairClient

    session = FakeSession([
        Timeout(),
        Timeout(),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=1,
    )
    client.session_token = "TOK"

    with pytest.raises(RuntimeError, match="REQUEST_FAILED"):
        client.get_market_book("1.300")


@pytest.mark.integration
@pytest.mark.net
def test_market_book_http_502_then_success_recovers():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(status_code=502, raise_http=True),
        FakeResponse(
            json_data=[{
                "result": [
                    {"marketId": "1.700", "runners": []}
                ]
            }]
        ),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=1,
    )
    client.session_token = "TOK"

    out = client.get_market_book("1.700")
    assert out["marketId"] == "1.700"


@pytest.mark.integration
@pytest.mark.net
def test_market_book_http_503_persistent_failure():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(status_code=503, raise_http=True),
        FakeResponse(status_code=503, raise_http=True),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=1,
    )
    client.session_token = "TOK"

    with pytest.raises(RuntimeError, match="REQUEST_FAILED"):
        client.get_market_book("1.701")


@pytest.mark.integration
@pytest.mark.net
def test_market_book_connection_reset_then_success():
    from betfair_client import BetfairClient

    session = FakeSession([
        requests.exceptions.ConnectionError("connection reset by peer"),
        FakeResponse(
            json_data=[{
                "result": [
                    {"marketId": "1.702", "runners": []}
                ]
            }]
        ),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=1,
    )
    client.session_token = "TOK"

    out = client.get_market_book("1.702")
    assert out["marketId"] == "1.702"


@pytest.mark.integration
@pytest.mark.net
def test_market_book_invalid_json_after_retry_fails_as_permanent():
    from betfair_client import BetfairClient

    session = FakeSession([
        requests.exceptions.ConnectionError("net down"),
        FakeResponse(json_data=ValueError("broken json")),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=1,
    )
    client.session_token = "TOK"

    with pytest.raises(RuntimeError, match="INVALID_JSON"):
        client.get_market_book("1.703")


@pytest.mark.integration
@pytest.mark.net
def test_market_book_retry_does_not_mutate_request_payload():
    from betfair_client import BetfairClient

    session = RecordingSession([
        Timeout(),
        FakeResponse(
            json_data=[{
                "result": [
                    {"marketId": "1.709", "runners": []}
                ]
            }]
        ),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=1,
    )
    client.session_token = "TOK"

    out = client.get_market_book("1.709")
    assert out["marketId"] == "1.709"
    assert len(session.calls) == 2

    first_url, first_kwargs = session.calls[0]
    second_url, second_kwargs = session.calls[1]

    assert first_url == second_url
    assert json.loads(first_kwargs["data"]) == json.loads(second_kwargs["data"])


@pytest.mark.integration
@pytest.mark.net
def test_http_500_then_success_recovers_for_place_bet():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(status_code=500, raise_http=True, text="server error"),
        FakeResponse(
            json_data=[{
                "result": {
                    "status": "SUCCESS",
                    "instructionReports": [
                        {"status": "SUCCESS", "betId": "BET500"}
                    ],
                }
            }]
        ),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=1,
    )
    client.session_token = "TOK"

    out = client.place_bet(
        market_id="1.200",
        selection_id=22,
        side="BACK",
        price=2.2,
        size=5.0,
    )

    assert out["ok"] is True


@pytest.mark.integration
@pytest.mark.net
def test_place_bet_http_500_then_success_preserves_result_shape():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(status_code=500, raise_http=True),
        FakeResponse(
            json_data=[{
                "result": {
                    "status": "SUCCESS",
                    "marketId": "1.704",
                    "instructionReports": [
                        {"status": "SUCCESS", "betId": "BET704"}
                    ],
                }
            }]
        ),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=1,
    )
    client.session_token = "TOK"

    out = client.place_bet(
        market_id="1.704",
        selection_id=70,
        side="BACK",
        price=2.5,
        size=3.0,
    )

    assert out["ok"] is True
    assert out["result"]["status"] == "SUCCESS"
    assert out["result"]["instructionReports"][0]["betId"] == "BET704"


@pytest.mark.integration
@pytest.mark.net
def test_place_bet_timeout_sets_order_unknown_flag():
    from betfair_client import BetfairClient

    session = FakeSession([
        Timeout(),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=0,
    )
    client.session_token = "TOK"

    out = client.place_bet(
        market_id="1.400",
        selection_id=33,
        side="BACK",
        price=2.0,
        size=10.0,
    )

    assert out["ok"] is False
    assert out["classification"] == "TRANSIENT"
    assert out["order_unknown"] is True


@pytest.mark.integration
@pytest.mark.net
def test_place_bet_http_503_persistent_failure():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(status_code=503, raise_http=True),
        FakeResponse(status_code=503, raise_http=True),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=1,
    )
    client.session_token = "TOK"

    out = client.place_bet(
        market_id="1.500",
        selection_id=44,
        side="BACK",
        price=2.0,
        size=5.0,
    )

    assert out["ok"] is False
    assert out["classification"] == "TRANSIENT"


@pytest.mark.integration
@pytest.mark.net
def test_place_bet_connection_error_persistent_failure_classified_transient():
    from betfair_client import BetfairClient

    session = FakeSession([
        requests.exceptions.ConnectionError("net down"),
        requests.exceptions.ConnectionError("still down"),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=1,
    )
    client.session_token = "TOK"

    out = client.place_bet(
        market_id="1.705",
        selection_id=71,
        side="BACK",
        price=2.0,
        size=2.0,
    )

    assert out["ok"] is False
    assert out["classification"] == "TRANSIENT"
    assert out["order_unknown"] is False


@pytest.mark.integration
@pytest.mark.net
def test_place_bet_invalid_json_rpc_response_is_permanent_failure():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(json_data={"not": "a list"}),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=0,
    )
    client.session_token = "TOK"

    out = client.place_bet(
        market_id="1.706",
        selection_id=72,
        side="BACK",
        price=2.0,
        size=2.0,
    )

    assert out["ok"] is False
    assert out["classification"] == "PERMANENT"
    assert "INVALID_JSON_RPC" in out["error"]


@pytest.mark.integration
@pytest.mark.net
def test_place_bet_session_expired_returns_permanent_failure():
    from betfair_client import BetfairClient

    session = FakeSession([
        FakeResponse(
            json_data=[{
                "error": {
                    "code": -32099,
                    "message": "INVALID_SESSION",
                }
            }]
        ),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=0,
    )
    client.session_token = "TOK"
    client.connected = True

    out = client.place_bet(
        market_id="1.707",
        selection_id=73,
        side="BACK",
        price=2.0,
        size=2.0,
    )

    assert out["ok"] is False
    assert out["classification"] == "PERMANENT"
    assert "SESSION_EXPIRED" in out["error"]
    assert client.session_token == ""
    assert client.connected is False


@pytest.mark.integration
@pytest.mark.net
def test_place_bet_timeout_exhausted_single_attempt_marks_order_unknown():
    from betfair_client import BetfairClient

    session = FakeSession([
        Timeout(),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=0,
    )
    client.session_token = "TOK"

    out = client.place_bet(
        market_id="1.708",
        selection_id=74,
        side="LAY",
        price=3.2,
        size=4.0,
    )

    assert out["ok"] is False
    assert out["classification"] == "TRANSIENT"
    assert out["order_unknown"] is True


@pytest.mark.integration
@pytest.mark.net
def test_place_bet_timeout_then_retry_submits_same_payload():
    from betfair_client import BetfairClient

    session = RecordingSession([
        Timeout(),
        FakeResponse(
            json_data=[{
                "result": {
                    "status": "SUCCESS",
                    "instructionReports": [
                        {"status": "SUCCESS", "betId": "BET-ON-RETRY"}
                    ],
                }
            }]
        ),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=1,
    )
    client.session_token = "TOK"

    out = client.place_bet(
        market_id="1.600",
        selection_id=101,
        side="BACK",
        price=2.0,
        size=5.0,
    )

    assert out["ok"] is True
    assert len(session.calls) == 2

    first_url, first_kwargs = session.calls[0]
    second_url, second_kwargs = session.calls[1]

    assert first_url == second_url
    assert json.loads(first_kwargs["data"]) == json.loads(second_kwargs["data"])


@pytest.mark.integration
@pytest.mark.net
def test_place_bet_timeout_ambiguous_requires_reconcile_signal():
    from betfair_client import BetfairClient

    session = RecordingSession([
        Timeout(),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=0,
    )
    client.session_token = "TOK"

    out = client.place_bet(
        market_id="1.601",
        selection_id=202,
        side="LAY",
        price=3.0,
        size=4.0,
    )

    assert out["ok"] is False
    assert out["classification"] == "TRANSIENT"
    assert out["order_unknown"] is True
    assert "TIMEOUT" in out["error"].upper()