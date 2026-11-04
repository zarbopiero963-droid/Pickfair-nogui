"""PF-BETFAIR-E2E-01 livello B — guardia READ-ONLY e runner della campagna, provati OFFLINE.

La guardia sta sul trasporto del `BetfairClient` di produzione: qualunque metodo che altera ordini o fondi viene
rifiutato PRIMA della rete, anche se il chiamante e' il codice di trading (place_bet, cancel_orders, replace_orders,
reduce_order). Questi test usano una sessione finta: dimostrano la guardia e la forma del report, NON la compatibilita'
con le API reali (quella e' il livello B, da eseguire sul canale autorizzato: mode=REAL_READ_ONLY).
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest

import betfair_client
from tools.bf_readonly.campaign import run_campaign
from tools.bf_readonly.guard import ALLOWED_METHODS, MUTATING_METHODS, ReadOnlyTransport, ReadOnlyViolation

TOKEN = "TOKEN-SEGRETO-0123456789"
APP_KEY = "APPKEY-SEGRETA-9876"


class _Resp:
    status_code = 200

    def __init__(self, body: Any) -> None:
        self._body = body

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self._body


class _Rete:
    """Sessione finta: registra ogni richiesta che RAGGIUNGE la rete e risponde come Betfair (fixture sintetiche)."""

    def __init__(self, risposte: Dict[str, Any] | None = None) -> None:
        self.inviate: List[str] = []
        self.risposte = risposte or {}

    def post(self, url: str, **kw: Any) -> Any:
        if "identitysso" in url:
            self.inviate.append(url)
            return _Resp({"sessionToken": TOKEN, "loginStatus": "SUCCESS"})
        body = json.loads(kw["data"])
        method = body[0]["method"]
        self.inviate.append(method)
        return _Resp([{"jsonrpc": "2.0", "id": 1, "result": self.risposte.get(method, {})}])


def _client(rete: _Rete) -> tuple:
    guardia = ReadOnlyTransport(rete)
    c = betfair_client.BetfairClient(username="u", app_key=APP_KEY, cert_pem="c.pem", key_pem="k.pem",
                                     session=guardia)
    c.session_token = TOKEN
    return c, guardia


# ------------------------------------------------------------------ guardia: nessuna mutazione raggiunge la rete
@pytest.mark.parametrize("chiamata", [
    lambda c: c.place_bet(market_id="1.2", selection_id=5, side="BACK", price=2.0, size=2.0),
    lambda c: c.cancel_orders(market_id="1.2", bet_ids=["9"]),
    lambda c: c.cancel_orders(market_id="1.2"),
    lambda c: c.replace_orders(market_id="1.2", bet_id="9", new_price=3.0),
    lambda c: c.reduce_order(market_id="1.2", bet_id="9", size_reduction=1.0),
], ids=["place_bet", "cancel_orders", "cancel_all", "replace_orders", "reduce_order"])
def test_block_nessuna_operazione_mutante_raggiunge_la_rete(chiamata) -> None:
    rete = _Rete()
    c, guardia = _client(rete)

    try:
        chiamata(c)                                   # il client puo' inghiottire l'eccezione in un esito ok=False
    except ReadOnlyViolation:
        pass

    assert rete.inviate == [], f"richiesta mutante arrivata alla rete: {rete.inviate}"
    assert guardia.violations and guardia.violations[0].startswith("METHOD_NOT_READ_ONLY:"), guardia.violations


def test_i_metodi_mutanti_non_sono_mai_nell_allowlist() -> None:
    assert not (MUTATING_METHODS & ALLOWED_METHODS)
    assert all(m.split("/")[-1].startswith(("list", "get")) for m in ALLOWED_METHODS)


@pytest.mark.parametrize("url,data", [
    ("https://api.betfair.com/exchange/betting/rest/v1.0/placeOrders/", "{}"),       # REST, non JSON-RPC
    ("https://evil.example/exchange/betting/json-rpc/v1",
     json.dumps([{"method": "SportsAPING/v1.0/listMarketBook"}])),
    ("http://api.betfair.com/exchange/betting/json-rpc/v1",
     json.dumps([{"method": "SportsAPING/v1.0/listMarketBook"}])),                    # non https
    ("https://api.betfair.com/exchange/betting/json-rpc/v1", "non json"),
    ("https://api.betfair.com/exchange/betting/json-rpc/v1", "[]"),
    ("https://api.betfair.com/exchange/betting/json-rpc/v1",
     json.dumps([{"method": "SportsAPING/v1.0/listMarketBook"}, {"method": "SportsAPING/v1.0/placeOrders"}])),
    ("https://api.betfair.com/exchange/betting/json-rpc/v1", json.dumps([{"method": None}])),
])
def test_block_url_o_corpo_non_ammessi_rifiutati_prima_della_rete(url: str, data: str) -> None:
    rete = _Rete()
    guardia = ReadOnlyTransport(rete)

    with pytest.raises(ReadOnlyViolation):
        guardia.post(url, data=data)
    assert rete.inviate == []


@pytest.mark.parametrize("metodo", ["get", "put", "delete", "request"])
def test_block_altri_verbi_http_rifiutati(metodo: str) -> None:
    guardia = ReadOnlyTransport(_Rete())
    with pytest.raises(ReadOnlyViolation):
        getattr(guardia, metodo)("https://api.betfair.com/exchange/betting/json-rpc/v1")


def test_le_letture_e_il_login_passano() -> None:
    rete = _Rete({"SportsAPING/v1.0/listMarketBook": [{"marketId": "1.2", "status": "OPEN", "runners": []}]})
    c, guardia = _client(rete)

    assert c.get_market_book("1.2", include_prices=True)["status"] == "OPEN"
    guardia.post(betfair_client.BetfairClient.IDENTITY_URL, data={"username": "u", "password": "p"})
    assert rete.inviate == ["SportsAPING/v1.0/listMarketBook", betfair_client.BetfairClient.IDENTITY_URL]
    assert guardia.violations == []


def test_il_proxy_configurato_va_sulla_sessione_reale() -> None:
    rete = _Rete()
    guardia = ReadOnlyTransport(rete)
    guardia.proxies = {"https": "socks5://h:1"}
    assert rete.proxies == {"https": "socks5://h:1"}


# ------------------------------------------------------------------ campagna: report pubblicabile, nessun segreto
_RISPOSTE = {
    "AccountAPING/v1.0/getAccountFunds": {"availableToBetBalance": 123.45, "exposure": -6.0, "retainedCommission": 0,
                                          "exposureLimit": -10000, "wallet": "UK"},
    "SportsAPING/v1.0/listEvents": [{"event": {"id": "31", "name": "A v B"}, "marketCount": 3}],
    "SportsAPING/v1.0/listMarketCatalogue": [{"marketId": "1.111", "runners": [{"selectionId": 1}]},
                                             {"marketId": "1.222", "runners": [{"selectionId": 2}]}],
    "SportsAPING/v1.0/listMarketBook": [{"marketId": "1.111", "status": "SUSPENDED", "inplay": True, "runners": [
        {"selectionId": 1, "status": "ACTIVE",
         "ex": {"availableToBack": [{"price": 2.0, "size": 10}], "availableToLay": [{"price": 2.02, "size": 5}]}}]}],
    "SportsAPING/v1.0/listCurrentOrders": {"currentOrders": [], "moreAvailable": False},
    "SportsAPING/v1.0/listClearedOrders": {"clearedOrders": [], "moreAvailable": False},
}


def test_campagna_offline_produce_un_report_senza_segreti_ne_importi() -> None:
    rete = _Rete(_RISPOSTE)
    c, guardia = _client(rete)

    report = run_campaign(c, guardia, mode="OFFLINE_FIXTURE", max_markets=2, commit_sha="abc123")
    testo = json.dumps(report)

    assert report["mode"] == "OFFLINE_FIXTURE" and report["read_only_violations"] == []
    assert [s["op"] for s in report["steps"]][:3] == ["getAccountFunds", "listEvents", "listMarketCatalogue"]
    assert TOKEN not in testo and APP_KEY not in testo and "123.45" not in testo, "segreti o importi nel report"
    assert "1.111" not in testo, "id di mercato in chiaro nel report"
    books = [s for s in report["steps"] if s["op"] == "listMarketBook"]
    assert len(books) == 2 and books[0].get("status") == "SUSPENDED" and books[0]["ladders_present"] is True
    assert set(rete.inviate) <= ALLOWED_METHODS | {betfair_client.BetfairClient.IDENTITY_URL}


def test_campagna_e_fail_se_la_guardia_ha_bloccato_qualcosa() -> None:
    rete = _Rete(_RISPOSTE)
    c, guardia = _client(rete)
    try:
        c.cancel_orders(market_id="1.2", bet_ids=["9"])
    except ReadOnlyViolation:
        pass

    report = run_campaign(c, guardia, mode="OFFLINE_FIXTURE", max_markets=1)

    assert report["verdict"] == "FAIL" and report["read_only_violations"], report


# ------------------------------------------------------------------ estratto conto per la ricostruzione F-11
def test_export_estratto_conto_pagina_tutto_in_sola_lettura() -> None:
    from tools.bf_readonly.campaign import export_account_statement

    pagine = [{"accountStatement": [{"refId": str(i), "balance": i} for i in range(100)], "moreAvailable": True},
              {"accountStatement": [{"refId": "x", "balance": 1}], "moreAvailable": False}]

    class _ReteStatement(_Rete):
        def post(self, url, **kw):
            body = json.loads(kw["data"])
            self.inviate.append(body[0]["method"])
            return _Resp([{"jsonrpc": "2.0", "id": 1, "result": pagine[body[0]["params"]["fromRecord"] // 100]}])

    rete = _ReteStatement()
    c, guardia = _client(rete)
    out = export_account_statement(c, from_iso="2026-01-01T00:00:00Z", to_iso="2026-11-01T00:00:00Z")

    assert len(out["accountStatement"]) == 101 and guardia.violations == []
    assert rete.inviate == ["AccountAPING/v1.0/getAccountStatement"] * 2


def test_export_estratto_conto_malformato_o_troncato_e_errore() -> None:
    from tools.bf_readonly import campaign

    class _ReteRotta(_Rete):
        def post(self, url, **kw):
            return _Resp([{"jsonrpc": "2.0", "id": 1, "result": {"moreAvailable": True, "accountStatement": []}}])

    c, _g = _client(_ReteRotta())
    with pytest.raises(RuntimeError, match="STATEMENT_TRUNCATED"):
        campaign.export_account_statement(c, from_iso="a", to_iso="b")
    c2, _g2 = _client(_Rete({"AccountAPING/v1.0/getAccountStatement": {"x": 1}}))
    with pytest.raises(RuntimeError, match="STATEMENT_MALFORMED"):
        campaign.export_account_statement(c2, from_iso="a", to_iso="b")
