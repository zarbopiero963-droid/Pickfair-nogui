import pytest

from core.order_router import OrderRouter


class _FakeClient:
    def __init__(self):
        self.calls = []

    def place_bet(self, **kwargs):
        self.calls.append(kwargs)
        return {"ok": True, "payload": kwargs}


class _FakeService:
    def __init__(self, client):
        self._client = client

    def get_client(self):
        return self._client


def _payload():
    return {
        "market_id": "1.333",
        "selection_id": 77,
        "bet_type": "BACK",
        "price": 2.4,
        "stake": 11.5,
        "customer_ref": "cust-1",
        "event_key": "evt-1",
        "table_id": "tab-9",
        "batch_id": "batch-2",
    }


def test_place_forwards_expected_payload_to_client_without_mutation():
    client = _FakeClient()
    router = OrderRouter(_FakeService(client))
    payload = _payload()

    out = router.place(payload)

    assert out["ok"] is True
    assert client.calls == [
        {
            "market_id": payload["market_id"],
            "selection_id": payload["selection_id"],
            "side": payload["bet_type"],
            "price": payload["price"],
            "size": payload["stake"],
            "customer_ref": payload["customer_ref"],
            "event_key": payload["event_key"],
            "table_id": payload["table_id"],
            "batch_id": payload["batch_id"],
        }
    ]
    assert payload == _payload()


def test_place_surfaces_downstream_errors():
    class _ErrClient:
        def place_bet(self, **_kwargs):
            raise RuntimeError("downstream failure")

    router = OrderRouter(_FakeService(_ErrClient()))

    with pytest.raises(RuntimeError, match="downstream failure"):
        router.place(_payload())


def test_place_missing_required_field_raises_current_key_error():
    client = _FakeClient()
    router = OrderRouter(_FakeService(client))

    bad_payload = _payload()
    bad_payload.pop("market_id")

    with pytest.raises(KeyError, match="market_id"):
        router.place(bad_payload)
