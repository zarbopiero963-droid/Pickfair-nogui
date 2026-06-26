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


# ---------------------------------------------------------------------------
# Normalizzazione live/sim + parità firma del broker
# ---------------------------------------------------------------------------


class _StrictLiveClient:
    """Mima BetfairClient.place_bet: firma keyword-only senza kwargs extra.

    Inoltrare customer_ref/event_key/table_id/batch_id solleverebbe
    TypeError; il router deve filtrarli.
    """

    def __init__(self, result):
        self.calls = []
        self._result = result

    def place_bet(self, *, market_id, selection_id, side, price, size):
        self.calls.append(
            {
                "market_id": market_id,
                "selection_id": selection_id,
                "side": side,
                "price": price,
                "size": size,
            }
        )
        return self._result


class _SimLikeClient:
    """Mima SimulationBroker.place_bet: accetta i kwargs extra e ritorna
    la forma Betfair raw {"status", "instructionReports"}."""

    def __init__(self, result):
        self.calls = []
        self._result = result

    def place_bet(self, *, market_id, selection_id, side, price, size,
                  customer_ref="", event_key="", table_id=None, batch_id=""):
        self.calls.append({"market_id": market_id, "customer_ref": customer_ref})
        return self._result


def test_place_filters_extra_kwargs_for_strict_live_client():
    # Il client live rifiuta i kwargs extra: il router li deve filtrare
    # (nessun TypeError) e inoltrare solo i 5 parametri della firma.
    client = _StrictLiveClient(
        {"ok": True, "result": {"status": "SUCCESS",
                                "instructionReports": [
                                    {"status": "SUCCESS", "betId": "B1", "sizeMatched": 5.0}]}}
    )
    router = OrderRouter(_FakeService(client))

    out = router.place(_payload())

    assert client.calls == [
        {"market_id": "1.333", "selection_id": 77, "side": "BACK", "price": 2.4, "size": 11.5}
    ]
    assert out["ok"] is True
    assert out["status"] == "SUCCESS"
    assert out["matched"] == 5.0
    assert out["bet_id"] == "B1"
    assert out["error"] is None


def test_place_normalizes_live_failure_shape():
    client = _StrictLiveClient({"ok": False, "error": "BET_FAILED: EXECUTABLE"})
    router = OrderRouter(_FakeService(client))

    out = router.place(_payload())

    assert out["ok"] is False
    assert out["status"] == "FAILURE"
    assert out["matched"] == 0.0
    assert out["error"] == "BET_FAILED: EXECUTABLE"


def test_place_normalizes_sim_betfair_raw_shape():
    client = _SimLikeClient(
        {"status": "SUCCESS", "simulated": True,
         "instructionReports": [{"status": "SUCCESS", "betId": "SIM1", "sizeMatched": 3.0}]}
    )
    router = OrderRouter(_FakeService(client))

    out = router.place(_payload())

    # I kwargs extra arrivano al broker sim (la sua firma li dichiara).
    assert client.calls == [{"market_id": "1.333", "customer_ref": "cust-1"}]
    assert out["ok"] is True
    assert out["status"] == "SUCCESS"
    assert out["matched"] == 3.0
    assert out["bet_id"] == "SIM1"


def test_place_sim_unmatched_is_fail_closed():
    # Ordine sim non abbinato (report FAILURE): il router NON lo riporta
    # come successo (fail-closed), anche se lo status top-level è SUCCESS.
    client = _SimLikeClient(
        {"status": "SUCCESS",
         "instructionReports": [{"status": "FAILURE", "betId": "SIM2", "sizeMatched": 0.0}]}
    )
    router = OrderRouter(_FakeService(client))

    out = router.place(_payload())

    assert out["ok"] is False
    assert out["matched"] == 0.0
    assert out["error"] == "BET_NOT_FULLY_PLACED"


def test_place_non_dict_response_is_fail_closed():
    class _WeirdClient:
        def place_bet(self, **_kwargs):
            return None

    router = OrderRouter(_FakeService(_WeirdClient()))

    out = router.place(_payload())

    assert out["ok"] is False
    assert out["status"] == "UNKNOWN"
    assert out["error"] == "NON_DICT_RESPONSE"
