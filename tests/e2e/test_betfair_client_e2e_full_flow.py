import pytest
import requests
import shutil
import subprocess


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
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self.responses:
            raise RuntimeError("no more fake responses")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _write_self_signed_cert(tmp_path):
    cert = tmp_path / "client.crt"
    key = tmp_path / "client.key"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-sha256",
            "-days",
            "365",
            "-nodes",
            "-subj",
            "/CN=localhost",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    cert.chmod(0o600)
    key.chmod(0o600)
    return cert, key


@pytest.mark.e2e
@pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl required to generate local test cert")
def test_full_login_market_book_place_bet_logout_flow(tmp_path):
    from betfair_client import BetfairClient

    cert, key = _write_self_signed_cert(tmp_path)

    session = FakeSession([
        FakeResponse(
            json_data={
                "loginStatus": "SUCCESS",
                "sessionToken": "TOK123",
                "sessionExpiryTime": "2030-01-01T00:00:00Z",
            }
        ),
        FakeResponse(
            json_data=[{
                "result": [{
                    "marketId": "1.600",
                    "runners": [
                        {
                            "selectionId": 11,
                            "ex": {
                                "availableToBack": [{"price": 2.0, "size": 100.0}],
                                "availableToLay": [{"price": 2.02, "size": 50.0}],
                            },
                        }
                    ],
                }]
            }]
        ),
        FakeResponse(
            json_data=[{
                "result": {
                    "status": "SUCCESS",
                    "instructionReports": [
                        {"status": "SUCCESS", "betId": "BET600"}
                    ],
                }
            }]
        ),
    ])

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem=str(cert),
        key_pem=str(key),
        session=session,
    )

    login_out = client.login("pw")
    assert login_out["connected"] is True

    market_book = client.get_market_book("1.600")
    assert market_book["marketId"] == "1.600"

    bet_out = client.place_bet(
        market_id="1.600",
        selection_id=11,
        side="BACK",
        price=2.0,
        size=5.0,
    )
    assert bet_out["ok"] is True

    logout_out = client.logout()
    assert logout_out == {"ok": True, "logged_out": True}
    assert client.status()["connected"] is False
