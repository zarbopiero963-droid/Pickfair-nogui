"""Gate A — T2b batch b2, componente betfair (BetfairClient).

Verdetti individuali C-1 sul comportamento gia' ratificato. BetfairClient reale con sessione HTTP finta: nessuna
rete, nessuna credenziale; la scatola nera e' quella finta installata da tests/conftest.py.
"""
from __future__ import annotations

import json
from typing import Any, List

import pytest
from requests.exceptions import ConnectionError as ReqConnectionError
from requests.exceptions import Timeout

from betfair_client import BetfairClient
from cashout_cancel_adapter import CashoutCancelAdapter

_PIAZZATO = {"status": "SUCCESS", "instructionReports": [
    {"status": "SUCCESS", "betId": "B-1", "sizeMatched": 0.0, "orderStatus": "EXECUTABLE"}]}


class _Risposta:
    status_code = 200

    def __init__(self, corpo: Any) -> None:
        self._corpo = corpo

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self._corpo


class _SessioneRegistrata:
    """Sessione HTTP finta: registra ogni POST; `esiti` e' una coda di corpi o eccezioni."""

    def __init__(self, *esiti: Any) -> None:
        self.esiti = list(esiti)
        self.chiamate: List[dict] = []

    def post(self, url, **kwargs):
        self.chiamate.append({"url": url, **kwargs})
        if not self.esiti:
            raise AssertionError("POST inattesa")
        esito = self.esiti.pop(0)
        if isinstance(esito, Exception):
            raise esito
        return _Risposta(esito)

    def params(self, i: int = 0) -> dict:
        return json.loads(self.chiamate[i]["data"])[0]["params"]


def _client(sessione: Any, token: str = "TOK") -> BetfairClient:
    client = BetfairClient(username="u", app_key="a", cert_pem="c.pem", key_pem="k.pem",
                           session=sessione, max_retries=2)
    client.session_token = token
    return client


def _ok_rpc(result: Any) -> list:
    return [{"jsonrpc": "2.0", "id": 1, "result": result}]


_VALIDI = dict(market_id="1.234", selection_id=5678, side="BACK", price=2.0, size=5.0)


# ---------------------------------------------------------------------------
def test_trg_628_b_0808():
    """ID: TRG-628-B-0808 / PF-B1-BF-LOGOUT-LOCAL

    logout() azzera token, scadenza e connected e dichiara logged_out, senza alcuna chiamata di rete."""
    sessione = _SessioneRegistrata()
    client = _client(sessione, token="TOK-0808")
    client.session_expiry = "2099-01-01T00:00:00Z"
    client.connected = True

    esito = client.logout()

    assert esito == {"ok": True, "logged_out": True}
    assert client.session_token == ""
    assert client.session_expiry == ""
    assert client.connected is False
    assert sessione.chiamate == [], "il logout locale non deve parlare con Betfair"


# ---------------------------------------------------------------------------
def test_trg_628_b_0809():
    """ID: TRG-628-B-0809 / PF-B1-BF-KEEPALIVE

    keep_alive() invia una POST dedicata all'endpoint keepAlive con il token corrente; distingue SUCCESS, rifiuto
    (KEEPALIVE_FAILED con il codice), timeout (KEEPALIVE_TIMEOUT), rete (KEEPALIVE_NETWORK_ERROR); senza token
    non invia nulla (SESSION_EXPIRED)."""
    sessione = _SessioneRegistrata({"status": "SUCCESS", "token": "TOK"})
    client = _client(sessione)
    assert client.keep_alive() == {"ok": True, "kept_alive": True}
    assert sessione.chiamate[0]["url"] == BetfairClient.KEEPALIVE_URL
    assert sessione.chiamate[0]["headers"]["X-Authentication"] == "TOK"

    rifiuto = _SessioneRegistrata({"status": "FAIL", "error": "NO_SESSION"})
    with pytest.raises(RuntimeError) as exc:
        _client(rifiuto).keep_alive()
    assert str(exc.value) == "KEEPALIVE_FAILED: NO_SESSION"

    with pytest.raises(RuntimeError) as exc:
        _client(_SessioneRegistrata(Timeout("lento"))).keep_alive()
    assert str(exc.value) == "KEEPALIVE_TIMEOUT"

    with pytest.raises(RuntimeError) as exc:
        _client(_SessioneRegistrata(ReqConnectionError("giu"))).keep_alive()
    assert str(exc.value).startswith("KEEPALIVE_NETWORK_ERROR")

    senza_token = _SessioneRegistrata()
    with pytest.raises(RuntimeError) as exc:
        _client(senza_token, token="").keep_alive()
    assert str(exc.value) == "SESSION_EXPIRED"
    assert senza_token.chiamate == []


# ---------------------------------------------------------------------------
_INVALIDI = [
    ("market_id", "", "INVALID_MARKET_ID"),
    ("market_id", None, "INVALID_MARKET_ID"),
    ("selection_id", "abc", "INVALID_SELECTION_ID"),
    ("selection_id", 0, "INVALID_SELECTION_ID"),
    ("price", float("nan"), "INVALID_PRICE"),
    ("price", 1.0, "INVALID_PRICE"),
    ("size", float("inf"), "INVALID_SIZE"),
    ("size", 0.0, "INVALID_SIZE"),
    ("side", "BAK", "INVALID_SIDE"),
    ("side", None, "INVALID_SIDE"),
]


def _rifiuto_pre_invio(campo: str, valore: Any, codice: str) -> None:
    sessione = _SessioneRegistrata()
    with pytest.raises(RuntimeError) as exc:
        _client(sessione).place_bet(**{**_VALIDI, campo: valore})
    assert str(exc.value) == codice
    assert sessione.chiamate == [], f"{campo}={valore!r} ha raggiunto il POST"


@pytest.mark.parametrize("campo,valore,codice", _INVALIDI)
def test_trg_628_b_0810(campo, valore, codice):
    """ID: TRG-628-B-0810 / PF-B1-BF-PLACE-VALIDATE

    place_bet valida mercato, selezione, prezzo, size e lato PRIMA del POST: input invalido => codice dedicato e
    nessuna richiesta HTTP. Controprova: input valido => una sola POST placeOrders con i valori normalizzati."""
    _rifiuto_pre_invio(campo, valore, codice)


def test_trg_628_b_0810_valid_sends_once():
    """ID: TRG-628-B-0810 / PF-B1-BF-PLACE-VALIDATE (controprova PASS)"""
    sessione = _SessioneRegistrata(_ok_rpc(_PIAZZATO))
    esito = _client(sessione).place_bet(market_id=" 1.234 ", selection_id="5678", side=" lay ", price="2.5",
                                        size="4")
    assert esito["ok"] is True
    assert len(sessione.chiamate) == 1
    istr = sessione.params()["instructions"][0]
    assert sessione.params()["marketId"] == "1.234"
    assert istr["selectionId"] == 5678 and istr["side"] == "LAY"
    assert istr["limitOrder"]["price"] == 2.5 and istr["limitOrder"]["size"] == 4.0


@pytest.mark.parametrize("valore", ["", None, "   "])
def test_trg_628_b_0906(valore):
    """ID: TRG-628-B-0906 / PF-B1-BF-PLACE-VALIDATE-MARKET

    market_id vuoto/assente/di soli spazi => INVALID_MARKET_ID, nessun POST."""
    _rifiuto_pre_invio("market_id", valore, "INVALID_MARKET_ID")


@pytest.mark.parametrize("valore", ["abc", None, 0, -5, "1.5x"])
def test_trg_628_b_0907(valore):
    """ID: TRG-628-B-0907 / PF-B1-BF-PLACE-VALIDATE-SELECTION

    selection_id non intero o <= 0 => INVALID_SELECTION_ID, nessun POST."""
    _rifiuto_pre_invio("selection_id", valore, "INVALID_SELECTION_ID")


@pytest.mark.parametrize("valore", ["x", None, float("nan"), float("inf"), 1.0, 0.5])
def test_trg_628_b_0908(valore):
    """ID: TRG-628-B-0908 / PF-B1-BF-PLACE-VALIDATE-PRICE

    price non numerico, non finito o <= 1.0 => INVALID_PRICE, nessun POST."""
    _rifiuto_pre_invio("price", valore, "INVALID_PRICE")


@pytest.mark.parametrize("valore", ["x", None, float("nan"), float("inf"), 0.0, -1.0])
def test_trg_628_b_0909(valore):
    """ID: TRG-628-B-0909 / PF-B1-BF-PLACE-VALIDATE-SIZE

    size non numerica, non finita o <= 0 => INVALID_SIZE, nessun POST."""
    _rifiuto_pre_invio("size", valore, "INVALID_SIZE")


@pytest.mark.parametrize("valore", ["BAK", "", None, 1, "BACKLAY", b"BACK"])
def test_trg_628_b_0910(valore):
    """ID: TRG-628-B-0910 / PF-B1-BF-PLACE-VALIDATE-SIDE

    lato diverso da BACK/LAY (anche None, vuoto, non stringa) => INVALID_SIDE, mai un BACK di ripiego; nessun POST
    (D4: BACK resta BACK)."""
    _rifiuto_pre_invio("side", valore, "INVALID_SIDE")


# ---------------------------------------------------------------------------
def test_trg_628_b_0811():
    """ID: TRG-628-B-0811 / PF-B1-BF-CUSTOMERREF

    customerRef inviato al livello superiore della richiesta solo se conforme (<=32, charset Betfair); un ref non
    conforme e' omesso (mai troncato). Nessun customerOrderRef implicito nell'istruzione se il chiamante non lo
    chiede esplicitamente; un customer_order_ref esplicito non conforme rifiuta prima del POST."""
    conforme = "a" * 32
    s1 = _SessioneRegistrata(_ok_rpc(_PIAZZATO))
    _client(s1).place_bet(**_VALIDI, customer_ref=conforme)
    p1 = s1.params()
    assert p1["customerRef"] == conforme
    assert "customerRef" not in p1["instructions"][0]
    assert "customerOrderRef" not in p1["instructions"][0], "customerOrderRef implicito"

    for non_conforme in ("a" * 33, "ref con spazi", "ref\n", "é"):
        s2 = _SessioneRegistrata(_ok_rpc(_PIAZZATO))
        _client(s2).place_bet(**_VALIDI, customer_ref=non_conforme)
        p2 = s2.params()
        assert "customerRef" not in p2, f"ref non conforme inviato: {non_conforme!r}"
        assert "customerOrderRef" not in p2["instructions"][0]

    s3 = _SessioneRegistrata(_ok_rpc(_PIAZZATO))
    _client(s3).place_bet(**_VALIDI, customer_ref="R1", customer_order_ref="ORD-1")
    assert s3.params()["instructions"][0]["customerOrderRef"] == "ORD-1"

    s4 = _SessioneRegistrata()
    with pytest.raises(RuntimeError, match="INVALID_CUSTOMER_ORDER_REF"):
        _client(s4).place_bet(**_VALIDI, customer_order_ref="x" * 40)
    assert s4.chiamate == []


# ---------------------------------------------------------------------------
@pytest.mark.parametrize("risposta,atteso_ignoto", [
    (Timeout("lento"), True),
    (ReqConnectionError("reset"), True),
    ("NON_JSON", True),
    (_ok_rpc({"status": "SUCCESS", "instructionReports": []}), True),
    (_ok_rpc({"status": "FAILURE", "errorCode": "DUPLICATE_TRANSACTION", "instructionReports": [
        {"status": "FAILURE", "errorCode": "DUPLICATE_TRANSACTION"}]}), True),
    (_ok_rpc({"status": "BOH"}), True),
    (_ok_rpc({"status": "FAILURE", "errorCode": "INSUFFICIENT_FUNDS", "instructionReports": [
        {"status": "FAILURE", "errorCode": "INSUFFICIENT_FUNDS"}]}), False),
])
def test_trg_628_b_0812(risposta, atteso_ignoto):
    """ID: TRG-628-B-0812 / PF-B1-BF-PLACE-UNKNOWN

    Dopo l'invio: timeout, errore di rete, risposta illeggibile (non JSON, SUCCESS senza report, status fuori
    contratto) e DUPLICATE_TRANSACTION => ok=False con order_unknown=True (riconciliazione, mai FAILED definitivo).
    Un rifiuto dichiarato (FAILURE INSUFFICIENT_FUNDS) resta order_unknown=False. Sempre un solo POST."""

    class _NonJson(_Risposta):
        def json(self):
            raise ValueError("not json")

    class _S(_SessioneRegistrata):
        def post(self, url, **kwargs):
            self.chiamate.append({"url": url, **kwargs})
            if risposta == "NON_JSON":
                return _NonJson(None)
            if isinstance(risposta, Exception):
                raise risposta
            return _Risposta(risposta)

    sessione = _S()
    esito = _client(sessione).place_bet(**_VALIDI, customer_ref="REF0812")
    assert esito["ok"] is False
    assert esito["order_unknown"] is atteso_ignoto, esito
    assert len(sessione.chiamate) == 1, "placeOrders deve restare single-shot"


# ---------------------------------------------------------------------------
def test_trg_628_b_0813():
    """ID: TRG-628-B-0813 / PF-B1-BF-CANCEL-SCOPE

    cancel_orders costruisce un'istruzione per ogni bet_id; con lista vuota/assente invia instructions=[] (per
    l'API Betfair: tutti gli unmatched del mercato). P38 (solo unmatched pertinenti) e' applicato dai chiamanti:
    l'adapter del cashout non inoltra mai una lista vuota (nessun invio). market_id vuoto => INVALID_MARKET_ID."""
    risposta_ok = _ok_rpc({"status": "SUCCESS", "instructionReports": [
        {"status": "SUCCESS", "instruction": {"betId": "11"}, "sizeCancelled": 1.0},
        {"status": "SUCCESS", "instruction": {"betId": "22"}, "sizeCancelled": 1.0}]})
    s1 = _SessioneRegistrata(risposta_ok)
    _client(s1).cancel_orders(market_id="1.9", bet_ids=["11", 22])
    assert s1.params() == {"marketId": "1.9", "instructions": [{"betId": "11"}, {"betId": "22"}]}

    for vuota in ([], None):
        s2 = _SessioneRegistrata(_ok_rpc({"status": "SUCCESS", "instructionReports": []}))
        _client(s2).cancel_orders(market_id="1.9", bet_ids=vuota)
        assert s2.params() == {"marketId": "1.9", "instructions": []}

    s3 = _SessioneRegistrata()
    with pytest.raises(RuntimeError, match="INVALID_MARKET_ID"):
        _client(s3).cancel_orders(market_id="  ", bet_ids=["1"])
    assert s3.chiamate == []

    # Guardia P38 al chiamante: lista vuota => nessun cancel dell'intero mercato.
    chiamate: List[dict] = []
    adapter = CashoutCancelAdapter(is_simulation=lambda: False,
                                   live_cancel=lambda **kw: chiamate.append(kw) or {"ok": True},
                                   sim_cancel=lambda **kw: chiamate.append(kw) or {"ok": True})
    assert adapter.cancel("1.9", []) is True
    assert adapter.cancel("1.9", ["", "  "]) is True
    assert chiamate == [], "lista vuota inoltrata al broker: cancel dell'intero mercato"


@pytest.mark.xfail(strict=True, reason="TRG-628-B-0813: bet_ids non vuota ma di soli id vuoti diventa "
                                       "instructions=[] (cancel dell'intero mercato) nel client; ramo latente, "
                                       "nessun chiamante di produzione lo raggiunge (P38)")
def test_trg_628_b_0813_blank_ids_never_market_wide():
    """ID: TRG-628-B-0813 / PF-B1-BF-CANCEL-SCOPE (rilievo latente)

    Una lista con soli bet_id vuoti esprime un cancel per-bet senza bersagli: non deve diventare il cancel di tutto
    il mercato."""
    sessione = _SessioneRegistrata(_ok_rpc({"status": "SUCCESS", "instructionReports": []}))
    try:
        _client(sessione).cancel_orders(market_id="1.9", bet_ids=["", None])
    except RuntimeError:
        pass
    assert all(c and json.loads(c["data"])[0]["params"]["instructions"] for c in sessione.chiamate)
