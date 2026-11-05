"""F-14 (#586, T10/BF-05/NF-05, residuo #495) — cancel single-shot, esito ignoto = AMBIGUOUS, report per istruzione.

Scheda (parte 4, c.6502715406, campo 7): `single_shot=True` sul cancel; TIMEOUT/NETWORK => AMBIGUOUS + riconciliazione
per bet_id; `status` assente => `ok=False`; `instructionReports` verificati per istruzione; `cancelled_count` solo
SUCCESS. Rete simulata (nessuna chiamata reale).
"""
from __future__ import annotations

import json
from typing import Any, List

import pytest
from requests.exceptions import ConnectionError as ReqConnectionError, HTTPError, Timeout

import betfair_client


class _Resp:
    def __init__(self, status: int, body: Any = None) -> None:
        self.status_code = status
        self._body = body
        self.headers = {}
        self.text = json.dumps(body)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise HTTPError(response=self)

    def json(self) -> Any:
        return self._body


class _Rete:
    def __init__(self, risposte: List[Any]) -> None:
        self.risposte = list(risposte)
        self.invii: List[dict] = []

    def post(self, _url, **kw) -> Any:
        self.invii.append(json.loads(kw["data"])[0])
        r = self.risposte.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def _ok(result: Any) -> _Resp:
    return _Resp(200, [{"jsonrpc": "2.0", "id": 1, "result": result}])


def _client(rete: _Rete) -> betfair_client.BetfairClient:
    c = betfair_client.BetfairClient(username="u", app_key="a", cert_pem="c", key_pem="k", session=rete, max_retries=2)
    c.session_token = "TOK"
    return c


def _report(bet: str, status: str = "SUCCESS", **kw) -> dict:
    return {"status": status, "instruction": {"betId": bet}, "sizeCancelled": 2.0 if status == "SUCCESS" else 0.0, **kw}


# ------------------------------------------------------------------ nomi dei test RED della scheda
@pytest.mark.parametrize("errore", [Timeout(), ReqConnectionError("reset"), _Resp(503)])
def test_cancel_orders_is_single_shot(errore) -> None:
    rete = _Rete([errore, _ok({"status": "SUCCESS", "instructionReports": [_report("B1")]})] * 2)

    out = _client(rete).cancel_orders(market_id="1.2", bet_ids=["B1"])

    assert [i["method"] for i in rete.invii] == ["SportsAPING/v1.0/cancelOrders"], "cancel re-inviato"
    assert out["ok"] is False


@pytest.mark.parametrize("risultato", [{}, {"instructionReports": [_report("B1")]}, {"status": ""}, None])
def test_cancel_without_status_is_not_ok(risultato) -> None:
    out = _client(_Rete([_ok(risultato)])).cancel_orders(market_id="1.2", bet_ids=["B1"])

    assert out["ok"] is False
    assert out["outcome"] == "AMBIGUOUS", out           # risposta arrivata ma illeggibile: esito ignoto


@pytest.mark.parametrize("errore", [Timeout(), ReqConnectionError("reset"), _Resp(502), _Resp(200, "non json")])
def test_cancel_timeout_is_ambiguous_not_failed(errore) -> None:
    out = _client(_Rete([errore])).cancel_orders(market_id="1.2", bet_ids=["B1"])

    assert out["ok"] is False and out["outcome"] == "AMBIGUOUS", out
    assert out["bet_ids"] == ["B1"], "senza bet_id non si riconcilia"


@pytest.mark.parametrize("risultato,contati", [
    ({"status": "PROCESSED_WITH_ERRORS", "instructionReports": [
        _report("B1"), _report("B2", "FAILURE", errorCode="BET_TAKEN_OR_LAPSED")]}, 1),
    ({"status": "SUCCESS", "instructionReports": [_report("B1"), _report("B2", "FAILURE")]}, 1),
    ({"status": "SUCCESS", "instructionReports": [_report("B1")]}, 1),       # report di B2 mancante
])
def test_cancel_report_failure_not_counted(risultato, contati) -> None:
    out = _client(_Rete([_ok(risultato)])).cancel_orders(market_id="1.2", bet_ids=["B1", "B2"])

    assert out["ok"] is False, out
    assert out["cancelled_count"] == contati
    assert out["cancelled_bet_ids"] == ["B1"]


def test_ambiguous_never_becomes_matched_without_evidence() -> None:
    """#495: un cancel con esito ignoto lascia la saga AMBIGUOUS; nessuna transizione successiva senza prova."""
    from order_manager import OrderStatus, VALID_TRANSITIONS, TERMINAL_STATES

    assert OrderStatus.AMBIGUOUS in VALID_TRANSITIONS[OrderStatus.CANCEL_PENDING]
    assert VALID_TRANSITIONS[OrderStatus.AMBIGUOUS] == frozenset()
    assert OrderStatus.AMBIGUOUS in TERMINAL_STATES


# ------------------------------------------------------------------ esiti certi invariati
def test_cancel_confermato_per_ogni_istruzione() -> None:
    rete = _Rete([_ok({"status": "SUCCESS", "instructionReports": [_report("B1"), _report("B2")]})])

    out = _client(rete).cancel_orders(market_id="1.2", bet_ids=["B1", "B2"])

    assert out["ok"] is True and out["outcome"] == "SUCCESS" and out["cancelled_count"] == 2
    assert rete.invii[0]["params"]["instructions"] == [{"betId": "B1"}, {"betId": "B2"}]


@pytest.mark.parametrize("risposta", [
    _ok({"status": "FAILURE", "errorCode": "MARKET_SUSPENDED", "instructionReports": []}),
    _Resp(200, [{"jsonrpc": "2.0", "id": 1, "error": {"code": -32099, "message": "ANGX-0003"}}]),
    _Resp(400),
])
def test_rifiuto_certo_e_failed(risposta) -> None:
    out = _client(_Rete([risposta])).cancel_orders(market_id="1.2", bet_ids=["B1"])

    assert out["ok"] is False and out["outcome"] == "FAILED", out


def test_cancel_di_tutto_il_mercato_confermato_senza_report() -> None:
    out = _client(_Rete([_ok({"status": "SUCCESS", "instructionReports": []})])).cancel_orders(market_id="1.2")

    assert out["ok"] is True and out["outcome"] == "SUCCESS"


# ------------------------------------------------------------------ OrderManager: saga del cancel
def _cancel_with(risposta):
    from unittest.mock import MagicMock

    from order_manager import OrderManager
    from tests.integration.test_order_manager_cancel_replace import FakeBus, FakeDB, _payload

    db, bus, client = FakeDB(), FakeBus(), MagicMock()
    client.place_bet = MagicMock(return_value={"status": "SUCCESS", "instructionReports": [
        {"status": "SUCCESS", "betId": "B1", "sizeMatched": 0.0}]})
    if isinstance(risposta, Exception):
        client.cancel_orders = MagicMock(side_effect=risposta)
    else:
        client.cancel_orders = MagicMock(return_value=risposta)
    om = OrderManager(db=db, bus=bus, client_getter=lambda: client, sleep_fn=lambda _: None)
    om.place_order(_payload(customer_ref="F14-1", stake=10.0))
    out = om.cancel_order(customer_ref="F14-1", bet_id="B1", market_id="1.234567890")
    return out, db.get_order_saga("F14-1")["status"], bus.names()


@pytest.mark.parametrize("risposta,stato", [
    ({"ok": False, "outcome": "AMBIGUOUS", "error": "REQUEST_FAILED: TIMEOUT", "bet_ids": ["B1"]}, "AMBIGUOUS"),
    (RuntimeError("REQUEST_FAILED: TIMEOUT"), "AMBIGUOUS"),
    (RuntimeError("REQUEST_FAILED: NETWORK_ERROR: reset"), "AMBIGUOUS"),
    ({"ok": False, "outcome": "FAILED", "error": "CANCEL_FAILED: MARKET_SUSPENDED"}, "FAILED"),
    (RuntimeError("INVALID_MARKET_ID"), "FAILED"),
    ({"ok": True, "result": {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS", "sizeCancelled": 10.0}]}},
     "CANCELLED"),
])
def test_order_manager_cancel_esito_ignoto_resta_ambiguous(risposta, stato) -> None:
    out, saga, eventi = _cancel_with(risposta)

    assert out["status"] == stato and saga == stato, (out, saga)
    assert out["ok"] is (stato == "CANCELLED")
    if stato == "AMBIGUOUS":
        assert "QUICK_BET_CANCEL_AMBIGUOUS" in eventi and "QUICK_BET_CANCELLED" not in eventi


def test_report_timeout_per_istruzione_e_ambiguous() -> None:
    risultato = {"status": "SUCCESS", "instructionReports": [{"status": "TIMEOUT", "errorCode": "ERROR_IN_ORDER"}]}
    out = _client(_Rete([_ok(risultato)])).cancel_orders(market_id="1.2", bet_ids=["B1"])

    assert out["ok"] is False and out["outcome"] == "AMBIGUOUS" and out["cancelled_count"] == 0


def test_report_senza_betid_attribuiti_per_posizione() -> None:
    risultato = {"status": "PROCESSED_WITH_ERRORS", "instructionReports": [{"status": "FAILURE"}, {"status": "SUCCESS"}]}
    out = _client(_Rete([_ok(risultato)])).cancel_orders(market_id="1.2", bet_ids=["B1", "B2"])

    assert out["cancelled_bet_ids"] == ["B2"] and out["failed_bet_ids"] == ["B1"] and out["outcome"] == "PARTIAL"


def test_report_senza_betid_e_conteggio_diverso_non_attribuiti() -> None:
    risultato = {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}]}
    out = _client(_Rete([_ok(risultato)])).cancel_orders(market_id="1.2", bet_ids=["B1", "B2"])

    assert out["ok"] is False and out["outcome"] == "AMBIGUOUS"


# ------------------------------------------------------------------ revisione Claude B su db11a86 (P2-1, M10, M11, M22)
@pytest.mark.parametrize("risultato,atteso", [
    ({"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}, {"status": "TIMEOUT"}]}, "AMBIGUOUS"),
    ({"status": "PROCESSED_WITH_ERRORS", "instructionReports": [{"status": "TIMEOUT"}]}, "AMBIGUOUS"),
    ({"status": "FAILURE", "instructionReports": [{"status": "TIMEOUT"}]}, "AMBIGUOUS"),
    ({"status": "PROCESSED_WITH_ERRORS", "instructionReports": [{"status": "FAILURE"}]}, "FAILED"),
    ({"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}]}, "SUCCESS"),
])
def test_cancel_di_mercato_report_timeout_e_ambiguous(risultato, atteso) -> None:
    """P2-1: cancel di tutto il mercato (nessun betId richiesto): un report TIMEOUT/ignoto non e' mai un successo
    (emergency stop) ne' un rifiuto certo."""
    out = _client(_Rete([_ok(risultato)])).cancel_orders(market_id="1.2")

    assert out["outcome"] == atteso, out
    assert out["ok"] is (atteso == "SUCCESS")


def test_status_sconosciuto_con_report_success_e_ambiguous() -> None:
    """M11: status complessivo fuori contratto, anche con report SUCCESS => AMBIGUOUS, mai PARTIAL/FAILED."""
    risultato = {"status": "PENDING", "instructionReports": [_report("B1"), _report("B2", "FAILURE")]}
    out = _client(_Rete([_ok(risultato)])).cancel_orders(market_id="1.2", bet_ids=["B1", "B2"])

    assert out["ok"] is False and out["outcome"] == "AMBIGUOUS"
    om_out, saga, _ev = _cancel_with({"ok": False, "outcome": out["outcome"], "error": out.get("error")})
    assert om_out["status"] == "AMBIGUOUS" and saga == "AMBIGUOUS"


def test_betid_esplicito_prevale_sulla_posizione() -> None:
    """M10: con betId espliciti nei report l'ordine dei report non conta."""
    risultato = {"status": "PROCESSED_WITH_ERRORS", "instructionReports": [_report("B2"), _report("B1", "FAILURE")]}
    out = _client(_Rete([_ok(risultato)])).cancel_orders(market_id="1.2", bet_ids=["B1", "B2"])

    assert out["cancelled_bet_ids"] == ["B2"] and out["failed_bet_ids"] == ["B1"] and out["outcome"] == "PARTIAL"


@pytest.mark.parametrize("errore", ["REQUEST_FAILED: HTTP_503", "REQUEST_FAILED: HTTP_502",
                                     "REQUEST_FAILED: INVALID_JSON", "REQUEST_FAILED: UNKNOWN_ERROR: x"])
def test_order_manager_eccezione_dopo_invio_e_ambiguous(errore) -> None:
    """M22: 5xx o risposta illeggibile dopo l'invio, sollevati dal broker => saga AMBIGUOUS."""
    out, saga, _ev = _cancel_with(RuntimeError(errore))
    assert out["status"] == "AMBIGUOUS" and saga == "AMBIGUOUS", (errore, out)



def test_report_senza_betid_piu_numerosi_delle_istruzioni_non_attribuiti() -> None:
    """M10 originale: piu' report che istruzioni, senza betId espliciti => nessuna attribuzione per posizione."""
    risultato = {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}, {"status": "FAILURE"}]}
    out = _client(_Rete([_ok(risultato)])).cancel_orders(market_id="1.2", bet_ids=["B1"])

    assert out["ok"] is False and out["outcome"] == "AMBIGUOUS" and out["cancelled_bet_ids"] == []


# ------------------------------------------------------------------ revisione Sol su #609 (911562a..a8659e4, VALID_BLOCKERS)
def _api_error(code: str) -> _Resp:
    return _Resp(200, [{"jsonrpc": "2.0", "id": 1, "error": {
        "code": -32099, "message": "ANGX-0007", "data": {"APINGException": {"errorCode": code}}}}])


@pytest.mark.parametrize("codice", ["TIMEOUT_ERROR", "UNEXPECTED_ERROR"])
def test_errore_api_ad_esito_ignoto_e_ambiguous(codice) -> None:
    """Betfair risponde con un'eccezione che NON prova il mancato cancel: AMBIGUOUS e riconciliazione, mai FAILED."""
    rete = _Rete([_api_error(codice)])
    out = _client(rete).cancel_orders(market_id="1.2", bet_ids=["B1"])

    assert out["ok"] is False and out["outcome"] == "AMBIGUOUS", out
    assert out["bet_ids"] == ["B1"] and len(rete.invii) == 1


@pytest.mark.parametrize("codice", ["INVALID_INPUT_DATA", "TOO_MANY_REQUESTS", "SERVICE_BUSY", "ACCESS_DENIED"])
def test_errore_api_di_rifiuto_resta_failed(codice) -> None:
    """Rifiuto dichiarato da Betfair prima di eseguire: FAILED invariato (il blocco non diventa permissivo)."""
    out = _client(_Rete([_api_error(codice)])).cancel_orders(market_id="1.2", bet_ids=["B1"])

    assert out["ok"] is False and out["outcome"] == "FAILED", out


@pytest.mark.parametrize("reports", [
    [None, {"status": "SUCCESS"}],
    ["x", {"status": "SUCCESS"}],
    [{"status": "SUCCESS"}, 7],
])
def test_report_non_dict_non_rende_attribuibile_il_report_superstite(reports) -> None:
    """Un report illeggibile conta nel confronto con le istruzioni: niente attribuzione per posizione, esito AMBIGUOUS."""
    out = _client(_Rete([_ok({"status": "SUCCESS", "instructionReports": reports})])).cancel_orders(
        market_id="1.2", bet_ids=["B1"])

    assert out["ok"] is False and out["outcome"] == "AMBIGUOUS", out
    assert out["cancelled_bet_ids"] == [], out


def test_report_non_dict_con_due_istruzioni_resta_ambiguous_e_attribuisce_solo_per_posizione_vera() -> None:
    reports = [None, {"status": "SUCCESS"}]
    out = _client(_Rete([_ok({"status": "SUCCESS", "instructionReports": reports})])).cancel_orders(
        market_id="1.2", bet_ids=["B1", "B2"])

    assert out["outcome"] == "AMBIGUOUS" and out["ok"] is False, out
    assert out["cancelled_bet_ids"] == ["B2"], out          # B2 e' alla sua posizione reale; B1 resta ignoto


# ------------------------------------------------------------------ revisione Sol su #609 (911562a..69765cf, VALID_BLOCKERS)
@pytest.mark.parametrize("errore", [
    TimeoutError(),                                                   # senza messaggio: il testo e' vuoto
    ConnectionError(),
    Timeout(),                                                        # requests, senza messaggio
    ReqConnectionError(),
    RuntimeError("CANCEL_ORDER_UNCONFIRMED: CANCEL_AMBIGUOUS"),       # wrapper del cancel singolo
    RuntimeError("CANCEL_ORDER_UNCONFIRMED: status=UNKNOWN reports=[]"),
])
def test_order_manager_eccezione_ad_esito_ignoto_per_tipo_o_codice_e_ambiguous(errore) -> None:
    """Il tipo dell'eccezione o il codice UNCONFIRMED provano un esito ignoto anche senza i marcatori nel testo."""
    out, saga, eventi = _cancel_with(errore)
    assert out["status"] == "AMBIGUOUS" and saga == "AMBIGUOUS", (repr(errore), out)
    assert "QUICK_BET_CANCEL_AMBIGUOUS" in eventi and "QUICK_BET_CANCEL_FAILED" not in eventi


@pytest.mark.parametrize("errore", [
    RuntimeError("API_ERROR: INVALID_INPUT_DATA"), RuntimeError("SESSION_EXPIRED"),
    RuntimeError("CANCEL_ORDER_FAILED: CANCEL_FAILED: BET_TAKEN_OR_LAPSED"), ValueError("bad"),
])
def test_order_manager_rifiuto_certo_resta_failed(errore) -> None:
    out, saga, _ev = _cancel_with(errore)
    assert out["status"] == "FAILED" and saga == "FAILED", (repr(errore), out)
