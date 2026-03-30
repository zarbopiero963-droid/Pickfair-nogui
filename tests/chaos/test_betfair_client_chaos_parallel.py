import threading

import pytest
import requests


class FakeResponse:
    def __init__(self, *, json_data=None, raise_http=False, status_code=200):
        self._json_data = json_data
        self._raise_http = raise_http
        self.status_code = status_code
        self.text = ""

    def raise_for_status(self):
        if self._raise_http:
            raise requests.exceptions.HTTPError(response=self)

    def json(self):
        return self._json_data


class ThreadSafeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.lock = threading.Lock()

    def post(self, url, **kwargs):
        _ = url, kwargs
        with self.lock:
            if not self.responses:
                raise RuntimeError("no more fake responses")
            item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


@pytest.mark.chaos
@pytest.mark.concurrency
def test_parallel_place_bet_requests_do_not_crash():
    from betfair_client import BetfairClient

    responses = []
    for _ in range(20):
        responses.append(
            FakeResponse(
                json_data=[{
                    "result": {
                        "status": "SUCCESS",
                        "instructionReports": [{"status": "SUCCESS", "betId": "B"}],
                    }
                }]
            )
        )

    session = ThreadSafeSession(responses)

    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=0,
    )
    client.session_token = "TOK"

    results = []

    def worker(i):
        out = client.place_bet(
            market_id=f"1.{500+i}",
            selection_id=i + 1,
            side="BACK",
            price=2.0,
            size=2.0,
        )
        results.append(out)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 20
    assert all(r["ok"] is True for r in results)