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
