from betfair_client import BetfairClient


class FakeSession:
    def post(self, *a, **k):
        class R:
            def raise_for_status(self): pass
            def json(self):
                return [{
                    "result": {
                        "status": "SUCCESS",
                        "instructionReports": [{"status": "SUCCESS"}]
                    }
                }]
        return R()


def test_full_flow():
    c = BetfairClient(
        username="u",
        app_key="k",
        cert_pem="c",
        key_pem="k",
        session=FakeSession()
    )

    c.session_token = "X"

    result = c.place_bet(
        market_id="1",
        selection_id=1,
        side="BACK",
        price=2,
        size=10
    )

    assert result["status"] == "SUCCESS"