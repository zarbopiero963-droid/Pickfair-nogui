"""Gate A — batch A, componente betfair (BetfairClient._post_jsonrpc).

BetfairClient reale con sessione HTTP finta: nessuna rete, nessuna credenziale.
"""
from __future__ import annotations

from typing import Any, List

import pytest

from betfair_client import BetfairClient


class _Risposta:
    status_code = 200

    def __init__(self, corpo: Any) -> None:
        self._corpo = corpo

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self._corpo


class _SessioneContata:
    def __init__(self, corpo: Any) -> None:
        self.corpo = corpo
        self.chiamate = 0

    def post(self, url, **kwargs):
        self.chiamate += 1
        return _Risposta(self.corpo)


@pytest.mark.parametrize("marker", ["INVALID_SESSION", "NO_SESSION"])
def test_trg_628_b_0806(marker: str):
    """ID: TRG-628-B-0806 / PF-B1-BF-TOKEN-EXPIRED

    Un errore JSON-RPC con INVALID_SESSION o NO_SESSION azzera token, scadenza e
    connected e solleva SESSION_EXPIRED (non API_ERROR), senza ritentare.
    """
    sessione = _SessioneContata([{"jsonrpc": "2.0", "id": 1,
                                  "error": {"code": -32099, "message": f"ANGX-0003 {marker}"}}])
    client = BetfairClient(username="u", app_key="a", cert_pem="c.pem", key_pem="k.pem",
                           session=sessione, max_retries=2)
    client.session_token = "TOK-0806"
    client.session_expiry = "2099-01-01T00:00:00Z"
    client.connected = True

    with pytest.raises(RuntimeError) as exc:
        client._post_jsonrpc("http://betfair.invalid/rpc", "SportsAPING/v1.0/listMarketBook", {})

    assert str(exc.value) == "SESSION_EXPIRED"
    assert client.session_token == ""
    assert client.session_expiry == ""
    assert client.connected is False
    assert sessione.chiamate == 1, "una sessione scaduta non va ritentata"
