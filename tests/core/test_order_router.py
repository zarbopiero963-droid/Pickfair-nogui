import pytest

from core.order_router import OrderRouter


class _FakeClient:
    def __init__(self):
        self.calls = []

    def place_bet(self, **kwargs):
        self.calls.append(kwargs)
        # Risposta live-shape abbinata: ok=True a livello router.
        return {
            "ok": True,
            "result": {
                "status": "SUCCESS",
                "instructionReports": [
                    {"status": "SUCCESS", "betId": "FB1", "sizeMatched": kwargs.get("size", 0.0)}
                ],
            },
        }


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
            # Metadata di audit forwardati (default "" se assenti nel payload).
            "event_name": "",
            "market_name": "",
            "runner_name": "",
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

    Inoltrare customer_ref/event_key/table_id/batch_id/event_name/...
    solleverebbe TypeError; il router deve filtrarli.
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
    """Mima SimulationBroker.place_bet: accetta i kwargs extra (inclusi i
    metadata di audit) e ritorna la forma Betfair raw {"status", "instructionReports"}."""

    def __init__(self, result):
        self.calls = []
        self._result = result

    def place_bet(self, *, market_id, selection_id, side, price, size,
                  customer_ref="", event_key="", table_id=None, batch_id="",
                  event_name="", market_name="", runner_name=""):
        self.calls.append(
            {"market_id": market_id, "customer_ref": customer_ref,
             "event_name": event_name, "market_name": market_name, "runner_name": runner_name}
        )
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
    assert out["placed"] is True
    assert out["status"] == "SUCCESS"
    assert out["matched"] == 5.0
    assert out["bet_id"] == "B1"
    assert out["error"] is None


def test_place_forwards_audit_metadata_to_sim_broker():
    # I metadata di audit (event_name/market_name/runner_name) NON devono
    # essere persi sul percorso sim (Greptile P1 / CodeRabbit).
    client = _SimLikeClient(
        {"status": "SUCCESS",
         "instructionReports": [{"status": "SUCCESS", "betId": "SIM1", "sizeMatched": 3.0}]}
    )
    router = OrderRouter(_FakeService(client))
    payload = _payload()
    payload.update(event_name="Team A vs Team B", market_name="Match Odds", runner_name="Team A")

    out = router.place(payload)

    assert client.calls == [
        {"market_id": "1.333", "customer_ref": "cust-1",
         "event_name": "Team A vs Team B", "market_name": "Match Odds", "runner_name": "Team A"}
    ]
    assert out["ok"] is True
    assert out["matched"] == 3.0
    assert out["bet_id"] == "SIM1"


def test_place_live_failure_shape_is_failed_not_ambiguous():
    client = _StrictLiveClient({"ok": False, "error": "BET_FAILED: INSUFFICIENT_FUNDS"})
    router = OrderRouter(_FakeService(client))

    out = router.place(_payload())

    assert out["ok"] is False
    assert out["placed"] is False
    assert out["order_unknown"] is False
    assert out["status"] == "FAILURE"
    assert out["matched"] == 0.0
    assert out["error"] == "BET_FAILED: INSUFFICIENT_FUNDS"


def test_place_live_order_unknown_is_ambiguous_not_failed():
    # Timeout/rete dopo l'invio: l'ordine PUO' esistere => AMBIGUOUS, mai
    # un fallimento da ritentare (eviterebbe un duplicato reale). Codex P2.
    client = _StrictLiveClient(
        {"ok": False, "order_unknown": True, "error": "TIMEOUT", "classification": "TRANSIENT"}
    )
    router = OrderRouter(_FakeService(client))

    out = router.place(_payload())

    assert out["ok"] is False
    assert out["order_unknown"] is True
    assert out["status"] == "AMBIGUOUS"
    assert out["error"] == "TIMEOUT"


def test_place_live_unmatched_is_placed_but_not_ok():
    # Parità SIM/LIVE: un LIMIT accettato ma non abbinato è placed=True ma
    # ok=False (matched=0), su entrambi i broker. Codex P2 / Greptile P2.
    client = _StrictLiveClient(
        {"ok": True, "result": {"status": "SUCCESS",
                                "instructionReports": [
                                    {"status": "SUCCESS", "betId": "L1", "sizeMatched": 0.0}]}}
    )
    router = OrderRouter(_FakeService(client))

    out = router.place(_payload())

    assert out["placed"] is True
    assert out["ok"] is False
    assert out["matched"] == 0.0
    assert out["bet_id"] == "L1"  # bet_id restituito: il chiamante lo registra.


def test_place_sim_unmatched_is_placed_but_not_ok():
    # Ordine sim non abbinato: l'ordine ESISTE (placed=True, bet_id valorizzato
    # => niente orphan), ma ok=False perché non abbinato (parità col live).
    client = _SimLikeClient(
        {"status": "SUCCESS",
         "instructionReports": [{"status": "FAILURE", "betId": "SIM2", "sizeMatched": 0.0}]}
    )
    router = OrderRouter(_FakeService(client))

    out = router.place(_payload())

    assert out["placed"] is True
    assert out["ok"] is False
    assert out["matched"] == 0.0
    assert out["bet_id"] == "SIM2"


def test_place_non_dict_response_is_fail_closed():
    class _WeirdClient:
        def place_bet(self, **_kwargs):
            return None

    router = OrderRouter(_FakeService(_WeirdClient()))

    out = router.place(_payload())

    assert out["ok"] is False
    assert out["placed"] is False
    assert out["status"] == "UNKNOWN"
    assert out["error"] == "NON_DICT_RESPONSE"
