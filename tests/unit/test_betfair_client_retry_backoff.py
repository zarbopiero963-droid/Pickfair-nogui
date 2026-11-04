"""BetfairClient: attesa limitata fra i tentativi delle letture, Retry-After del 429, scritture mai ritentate.

Su `main` `ada4076` `_post_jsonrpc` ritentava letture e cancel completo fino a 3 volte SENZA attesa, anche su HTTP 429
(troppe richieste): i tentativi si sommavano al carico che Betfair stava rifiutando. Le scritture (placeOrders,
replaceOrders, riduzione) restano single_shot: un solo invio, mai un retry (esito ignoto => riconciliazione).
"""
from __future__ import annotations

import json
from typing import Any, List

import pytest
from requests.exceptions import HTTPError, Timeout

import betfair_client


class _Resp:
    def __init__(self, status: int, body: Any = None, headers=None) -> None:
        self.status_code = status
        self._body = body
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise HTTPError(response=self)

    def json(self) -> Any:
        return self._body


class _Rete:
    def __init__(self, risposte: List[Any]) -> None:
        self.risposte = list(risposte)
        self.invii: List[str] = []

    def post(self, _url, **kw) -> Any:
        self.invii.append(json.loads(kw["data"])[0]["method"])
        r = self.risposte.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


OK = _Resp(200, [{"jsonrpc": "2.0", "id": 1, "result": {"availableToBetBalance": 10.0, "exposure": 0.0}}])


def _client(rete: _Rete, **kw) -> tuple:
    attese: List[float] = []
    c = betfair_client.BetfairClient(username="u", app_key="a", cert_pem="c", key_pem="k", session=rete,
                                     max_retries=2, sleep_fn=attese.append, **kw)
    c.session_token = "TOK"
    return c, attese


def test_block_429_ritentato_con_attesa_e_retry_after() -> None:
    rete = _Rete([_Resp(429, headers={"Retry-After": "2"}), _Resp(429), OK])
    c, attese = _client(rete)

    c.get_account_funds()

    assert len(rete.invii) == 3
    assert attese == [2.0, 1.0], attese          # Retry-After 2 s, poi base 0,5 x 2^1


def test_block_timeout_ritentato_con_attesa_esponenziale() -> None:
    rete = _Rete([Timeout(), Timeout(), OK])
    c, attese = _client(rete)

    c.get_account_funds()

    assert attese == [0.5, 1.0]


def test_attesa_mai_oltre_il_massimo() -> None:
    rete = _Rete([_Resp(429, headers={"Retry-After": "3600"}), OK])
    c, attese = _client(rete)

    c.get_account_funds()

    assert attese == [5.0], "Retry-After enorme non limitato"


@pytest.mark.parametrize("valore", ["Wed, 21 Oct 2026 07:28:00 GMT", "abc", "-3", "nan"])
def test_retry_after_non_numerico_o_negativo_ignorato(valore: str) -> None:
    rete = _Rete([_Resp(429, headers={"Retry-After": valore}), OK])
    c, attese = _client(rete)

    c.get_account_funds()

    assert attese == [0.5]


def test_nessuna_attesa_dopo_l_ultimo_tentativo() -> None:
    rete = _Rete([Timeout(), Timeout(), Timeout()])
    c, attese = _client(rete)

    with pytest.raises(RuntimeError, match="REQUEST_FAILED"):
        c._post_jsonrpc(c.ACCOUNT_URL, "AccountAPING/v1.0/getAccountFunds", {})

    assert len(rete.invii) == 3 and attese == [0.5, 1.0]


def test_block_scrittura_mai_ritentata_ne_attesa() -> None:
    """placeOrders single_shot: un solo invio anche su 429/timeout, nessuna attesa, esito ignoto => riconciliazione."""
    rete = _Rete([_Resp(429, headers={"Retry-After": "1"}), OK, OK])
    c, attese = _client(rete)

    esito = c.place_bet(market_id="1.2", selection_id=5, side="BACK", price=2.0, size=2.0)

    assert rete.invii == ["SportsAPING/v1.0/placeOrders"] and attese == []
    assert esito["ok"] is False


@pytest.mark.parametrize("base,cap", [(-1.0, 5.0), (1.0, 0.5), (float("nan"), 5.0), (0.5, float("inf"))])
def test_parametri_di_attesa_non_validi_rifiutati(base: float, cap: float) -> None:
    with pytest.raises(ValueError, match="INVALID_RETRY_BACKOFF"):
        betfair_client.BetfairClient(username="u", app_key="a", cert_pem="c", key_pem="k",
                                     retry_backoff_base_s=base, retry_backoff_max_s=cap)


def test_attesa_esponenziale_con_tre_ritentativi() -> None:
    rete = _Rete([Timeout(), Timeout(), Timeout(), OK])
    attese: List[float] = []
    c = betfair_client.BetfairClient(username="u", app_key="a", cert_pem="c", key_pem="k", session=rete,
                                     max_retries=3, sleep_fn=attese.append)
    c.session_token = "TOK"

    c.get_account_funds()

    assert attese == [0.5, 1.0, 2.0]


def test_retry_after_vale_solo_per_il_tentativo_successivo() -> None:
    rete = _Rete([_Resp(429, headers={"Retry-After": "4"}), Timeout(), OK])
    c, attese = _client(rete)

    c.get_account_funds()

    assert attese == [4.0, 1.0], "Retry-After di un 429 precedente applicato a un timeout successivo"
