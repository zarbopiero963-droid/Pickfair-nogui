from betfair_client import BetfairClient


class FlakySession:
    def __init__(self):
        self.calls = 0

    def post(self, *a, **k):
        self.calls += 1

        if self.calls < 2:
            raise Exception("network down")

        class R:
            def raise_for_status(self): pass
            def json(self): return [{"result": {}}]

        return R()


def test_retry_network():
    s = FlakySession()

    c = BetfairClient(
        username="u",
        app_key="k",
        cert_pem="c",
        key_pem="k",
        session=s,
        max_retries=2
    )
    c.session_token = "X"

    r = c._post_jsonrpc("url", "m", {})

    assert r == {}
