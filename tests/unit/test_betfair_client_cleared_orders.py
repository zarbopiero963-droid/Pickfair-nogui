"""list_cleared_orders — la sorgente del settlement reale (PR 2 del piano di
cablaggio, 2026-08-23).

Il ciclo di chiusura posizioni (PR 3) ricavera' il PnL realizzato da questo
metodo: un difetto qui non e' un bug di lettura, e' un daily-loss cieco.
Quindi i test BLOCK contano piu' dei PASS (hard_verify_spec §6): fetch
fallito che sembra «niente da regolare», report paginato parziale spacciato
per completo, filtro sbagliato degradato in report vuoto — ognuno di questi
deve SOLLEVARE, mai restituire.

Harness identico a tests/unit/test_betfair_client.py (fake session.post che
risponde JSON-RPC batch): stesso confine di mock — la rete — e client reale
sotto test, breaker e redazione inclusi via _post_jsonrpc.
"""
import json as _json

import pytest


class DummySession:
    pass


class _RPCResp:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


@pytest.fixture
def client():
    from betfair_client import BetfairClient

    c = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=DummySession(),
    )
    c.session_token = "TOK"
    return c


def _rpc_result(result):
    return _RPCResp([{"jsonrpc": "2.0", "id": 1, "result": result}])


# =========================================================================
# PASS
# =========================================================================
@pytest.mark.unit
def test_default_e_settled_e_manda_paginazione(client):
    captured = {}

    def _post(url, headers=None, data=None, timeout=None, **kw):
        captured["url"] = url
        captured["body"] = _json.loads(data) if data else None
        return _rpc_result({
            "clearedOrders": [
                {"betId": "1", "marketId": "1.100", "profit": 4.5,
                 "betOutcome": "WON"},
            ],
            "moreAvailable": False,
        })

    client.session.post = _post

    out = client.list_cleared_orders()

    assert isinstance(out, list) and out[0]["betId"] == "1"
    assert captured["url"] == client.BETTING_URL
    call = captured["body"][0]
    assert call["method"] == "SportsAPING/v1.0/listClearedOrders"
    params = call["params"]
    assert params["betStatus"] == "SETTLED"
    assert params["fromRecord"] == 0
    assert params["recordCount"] == client.CLEARED_ORDERS_PAGE_SIZE
    # Nessun filtro non richiesto: un filtro fantasma restringerebbe il
    # settlement in silenzio.
    assert "marketIds" not in params
    assert "betIds" not in params
    assert "settledDateRange" not in params
    assert "groupBy" not in params


@pytest.mark.unit
def test_filtri_e_group_by_arrivano_al_wire(client):
    captured = {}

    def _post(url, headers=None, data=None, timeout=None, **kw):
        captured["body"] = _json.loads(data) if data else None
        return _rpc_result({"clearedOrders": [], "moreAvailable": False})

    client.session.post = _post

    out = client.list_cleared_orders(
        bet_status="voided",
        market_ids=["1.100", " ", "1.200"],
        bet_ids=["77"],
        settled_after="2026-08-23T00:00:00Z",
        settled_before="2026-08-24T00:00:00Z",
        group_by="market",
    )

    assert out == []
    params = captured["body"][0]["params"]
    assert params["betStatus"] == "VOIDED"
    assert params["marketIds"] == ["1.100", "1.200"]
    assert params["betIds"] == ["77"]
    assert params["settledDateRange"] == {
        "from": "2026-08-23T00:00:00Z",
        "to": "2026-08-24T00:00:00Z",
    }
    assert params["groupBy"] == "MARKET"


@pytest.mark.unit
def test_pagine_multiple_concatenate_e_from_record_avanza(client):
    pages = [
        {"clearedOrders": [{"betId": "1"}, {"betId": "2"}], "moreAvailable": True},
        {"clearedOrders": [{"betId": "3"}], "moreAvailable": False},
    ]
    seen_from = []

    def _post(url, headers=None, data=None, timeout=None, **kw):
        params = _json.loads(data)[0]["params"]
        seen_from.append(params["fromRecord"])
        return _rpc_result(pages[len(seen_from) - 1])

    client.session.post = _post

    out = client.list_cleared_orders()

    assert [o["betId"] for o in out] == ["1", "2", "3"]
    assert seen_from == [0, 2]


# =========================================================================
# BLOCK — fail-closed
# =========================================================================
@pytest.mark.unit
def test_bet_status_invalido_solleva_senza_toccare_la_rete(client):
    def _post(*a, **kw):
        raise AssertionError("nessuna chiamata di rete attesa")

    client.session.post = _post

    with pytest.raises(RuntimeError, match="INVALID_BET_STATUS"):
        client.list_cleared_orders(bet_status="MATCHED")


@pytest.mark.unit
def test_group_by_invalido_solleva_senza_toccare_la_rete(client):
    def _post(*a, **kw):
        raise AssertionError("nessuna chiamata di rete attesa")

    client.session.post = _post

    with pytest.raises(RuntimeError, match="INVALID_GROUP_BY"):
        client.list_cleared_orders(group_by="RUNNER")


@pytest.mark.unit
def test_range_settled_fornito_ma_vuoto_solleva(client):
    def _post(*a, **kw):
        raise AssertionError("nessuna chiamata di rete attesa")

    client.session.post = _post

    with pytest.raises(RuntimeError, match="INVALID_SETTLED_RANGE"):
        client.list_cleared_orders(settled_after="   ")


@pytest.mark.unit
def test_more_available_con_pagina_vuota_solleva(client):
    def _post(url, headers=None, data=None, timeout=None, **kw):
        return _rpc_result({"clearedOrders": [], "moreAvailable": True})

    client.session.post = _post

    with pytest.raises(RuntimeError, match="CLEARED_ORDERS_TRUNCATED"):
        client.list_cleared_orders()


@pytest.mark.unit
def test_paginazione_che_non_termina_solleva_al_cap(client):
    def _post(url, headers=None, data=None, timeout=None, **kw):
        return _rpc_result({
            "clearedOrders": [{"betId": "x"}],
            "moreAvailable": True,
        })

    client.session.post = _post

    with pytest.raises(RuntimeError, match="pagination cap exceeded"):
        client.list_cleared_orders()


@pytest.mark.unit
def test_session_expired_propaga_e_azzera_la_sessione(client):
    def _post(url, headers=None, data=None, timeout=None, **kw):
        return _RPCResp([{
            "jsonrpc": "2.0", "id": 1,
            "error": {"data": {"APINGException": {
                "errorCode": "INVALID_SESSION_INFORMATION"}}},
        }])

    client.session.post = _post

    with pytest.raises(RuntimeError, match="SESSION_EXPIRED"):
        client.list_cleared_orders()
    assert client.session_token == ""


@pytest.mark.unit
def test_senza_autenticazione_solleva(client):
    client.session_token = ""

    with pytest.raises(RuntimeError, match="NOT_AUTHENTICATED"):
        client.list_cleared_orders()


@pytest.mark.unit
def test_breaker_aperto_rifiuta_subito(client):
    for _ in range(5):
        client._api_breaker.record_failure(RuntimeError("x"))

    with pytest.raises(RuntimeError, match="CIRCUIT_BREAKER_OPEN"):
        client.list_cleared_orders()


@pytest.mark.unit
def test_errore_api_propaga_mai_lista_vuota(client):
    """Il contratto che protegge il daily-loss: un errore API deve SOLLEVARE.
    Un ramo che degradasse in `return []` farebbe sembrare «niente da
    regolare» un fetch fallito."""
    def _post(url, headers=None, data=None, timeout=None, **kw):
        return _RPCResp([{
            "jsonrpc": "2.0", "id": 1,
            "error": {"code": -32099, "message": "ANGX-0002"},
        }])

    client.session.post = _post

    with pytest.raises(RuntimeError, match="API_ERROR"):
        client.list_cleared_orders()
