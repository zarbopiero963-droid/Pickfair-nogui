"""P0 — placeOrders: risposta 2xx ARRIVATA ma senza un esito leggibile => esito IGNOTO, mai rifiuto definitivo.

Difetto riprodotto su `main` `ada4076` (audit Claude D, 04/11/2026). Il POST di `placeOrders` e' partito e
Betfair ha risposto con un corpo JSON-RPC valido ma **senza esito**:

- elemento JSON-RPC senza `result` e senza `error` (o `result: null` / `{}`);
- `result` senza `status` (anche con un instruction report SUCCESS e un `betId`!);
- `status: SUCCESS` con un instruction report senza `status` (ma con `betId`);
- `status` non riconosciuto dal contratto Betfair (ExecutionReportStatus).

`BetfairClient.place_bet` le restituiva come `{"ok": False, "order_unknown": False}` (`BET_FAILED:` o
`BET_REJECTED: {...}`): un rifiuto DEFINITIVO. Il TradingEngine le chiudeva `FAILED` con zero riconciliazioni e,
per il contratto `docs/trading_engine_executor_contract.md` §2, un esito certo libera tavolo, riserva e chiave
anti-duplicato. Un ordine forse VIVO sull'exchange (in un caso con `betId` gia' assegnato) restava fuori da ogni
contabilita' e un nuovo segnale sullo stesso evento poteva piazzarne un secondo.

Regola owner (CLAUDE.md, Microstake nativo): «esito ignoto dopo l'invio = AMBIGUOUS + riconciliazione, mai retry».
Il rifiuto ESPLICITO di Betfair (`status: FAILURE`, report `FAILURE`, elemento `error`) resta definitivo: i test
`test_rifiuto_esplicito_*` bloccano la correzione opposta (ogni rifiuto in riconciliazione).
"""
from __future__ import annotations

from typing import Any, Dict, List

import pytest

import betfair_client
from core.trading_constants import AMBIGUITY_SUBMIT_UNKNOWN
from core.trading_engine import TradingEngine
from tests.helpers.fake_consumed_intents import FakeConsumedIntentsMixin


class _RispostaHttp:
    """Risposta HTTP 200 arrivata: il POST e' gia' partito."""

    status_code = 200

    def __init__(self, corpo: Any) -> None:
        self._corpo = corpo

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self._corpo


class _SessioneContaInvii:
    """Contatore del TRASPORTO: ogni `post` e' un invio a Betfair."""

    def __init__(self, corpo: Any) -> None:
        self.corpo = corpo
        self.post_inviati = 0

    def post(self, _url: str, **_kw: Any) -> Any:
        self.post_inviati += 1
        return _RispostaHttp(self.corpo)


class _Riconciliazione:
    def __init__(self) -> None:
        self.accodati: List[Dict[str, Any]] = []

    def enqueue(self, **meta: Any) -> None:
        self.accodati.append(meta)


class _Nulla:
    def __getattr__(self, _name: str) -> Any:
        return lambda *a, **k: None


class _DbMinimo(FakeConsumedIntentsMixin):
    def insert_order(self, _payload: Dict[str, Any]) -> str:
        return "OID-P0-PLACE"

    def update_order(self, *_a: Any, **_k: Any) -> None:
        return None


class _RuntimeLive:
    """Runtime LIVE minimo: il percorso dell'engine arriva al client (stesso stub di test_issue437_pr02)."""

    def get_effective_execution_mode(self) -> str:
        return "LIVE"

    def is_live_allowed(self) -> bool:
        return True

    is_emergency_stopped = False
    betfair_service = None


def _client(sessione: _SessioneContaInvii) -> betfair_client.BetfairClient:
    reale = betfair_client.BetfairClient(
        username="u", app_key="a", cert_pem="c.pem", key_pem="k.pem", session=sessione, max_retries=2,
    )
    reale.session_token = "TOK"
    return reale


def _place(client: betfair_client.BetfairClient) -> Dict[str, Any]:
    return client.place_bet(market_id="1.234", selection_id=5678, side="BACK", price=2.0, size=5.0)


# Corpi JSON-RPC 2xx che NON dicono se l'ordine e' passato.
RISPOSTE_SENZA_ESITO = {
    "senza_result_ne_error": [{"jsonrpc": "2.0", "id": 1}],
    "result_null": [{"jsonrpc": "2.0", "id": 1, "result": None}],
    "result_vuoto": [{"jsonrpc": "2.0", "id": 1, "result": {}}],
    "status_assente_report_success_con_betid": [
        {"result": {"instructionReports": [{"status": "SUCCESS", "betId": "31415", "sizeMatched": 5.0}]}}
    ],
    "report_senza_status_con_betid": [
        {"result": {"status": "SUCCESS", "instructionReports": [{"betId": "31415"}]}}
    ],
    "status_non_riconosciuto": [
        {"result": {"status": "PENDING", "instructionReports": [{"status": "SUCCESS", "betId": "31415"}]}}
    ],
    "report_status_non_riconosciuto": [
        {"result": {"status": "SUCCESS", "instructionReports": [{"status": "PENDING", "betId": "31415"}]}}
    ],
}


@pytest.mark.parametrize("corpo", list(RISPOSTE_SENZA_ESITO.values()), ids=list(RISPOSTE_SENZA_ESITO))
def test_block_client_risposta_senza_esito_e_order_unknown(corpo: Any) -> None:
    sessione = _SessioneContaInvii(corpo)

    esito = _place(_client(sessione))

    assert sessione.post_inviati == 1, "placeOrders deve restare single-shot"
    assert esito["ok"] is False
    assert esito["order_unknown"] is True, (
        f"risposta arrivata senza esito trattata come rifiuto definitivo: {esito!r}"
    )


@pytest.mark.parametrize("corpo", list(RISPOSTE_SENZA_ESITO.values()), ids=list(RISPOSTE_SENZA_ESITO))
def test_block_engine_risposta_senza_esito_ambigua_riserva_trattenuta(corpo: Any) -> None:
    """Percorso pubblico `submit_quick_bet` col `BetfairClient` reale: AMBIGUOUS, riconciliazione, lock tenuto,
    nessun secondo POST."""
    sessione = _SessioneContaInvii(corpo)
    ric = _Riconciliazione()
    reale = _client(sessione)
    eng = TradingEngine(bus=_Nulla(), db=_DbMinimo(), client_getter=lambda: reale,
                        executor=None, reconciliation_engine=ric)
    eng._runtime_state = "READY"
    eng.runtime_controller = _RuntimeLive()
    richiesta = {"customer_ref": "PFP0PLACE1", "market_id": "1.23456789", "selection_id": 47999,
                 "bet_type": "BACK", "price": 2.5, "stake": 10.0}

    primo = eng.submit_quick_bet(dict(richiesta))

    assert sessione.post_inviati == 1
    assert primo["status"] == "AMBIGUOUS", f"esito ignoto chiuso {primo['status']!r}: {primo.get('error')}"
    assert primo["ambiguity_reason"] == AMBIGUITY_SUBMIT_UNKNOWN
    assert len(ric.accodati) == 1, "ordine dall'esito ignoto mai mandato in riconciliazione"
    assert "PFP0PLACE1" in eng._inflight_keys, "lock/riserva rilasciati su un esito ignoto"

    secondo = eng.submit_quick_bet(dict(richiesta))

    assert secondo["status"] == "DUPLICATE_BLOCKED"
    assert sessione.post_inviati == 1, "secondo POST su un ordine forse gia' vivo"


# --------------------------------------------------------------------------
# Il contrario: il rifiuto ESPLICITO resta definitivo (nessuna sovra-correzione)
# --------------------------------------------------------------------------
RIFIUTI_ESPLICITI = {
    "status_failure_con_errorcode": [{"result": {
        "status": "FAILURE", "errorCode": "BET_ACTION_ERROR",
        "instructionReports": [{"status": "FAILURE", "errorCode": "INVALID_BET_SIZE"}]}}],
    "status_failure_senza_report": [{"result": {"status": "FAILURE", "errorCode": "MARKET_SUSPENDED"}}],
    "report_failure": [{"result": {
        "status": "SUCCESS", "instructionReports": [{"status": "FAILURE", "errorCode": "INVALID_ODDS"}]}}],
    "processed_with_errors_report_failure": [{"result": {
        "status": "PROCESSED_WITH_ERRORS",
        "instructionReports": [{"status": "FAILURE", "errorCode": "INVALID_ODDS"}]}}],
    "errore_json_rpc": [{"error": {"code": -32602, "message": "DSC-0018"}}],
}


@pytest.mark.parametrize("corpo", list(RIFIUTI_ESPLICITI.values()), ids=list(RIFIUTI_ESPLICITI))
def test_rifiuto_esplicito_resta_definitivo(corpo: Any) -> None:
    sessione = _SessioneContaInvii(corpo)

    esito = _place(_client(sessione))

    assert sessione.post_inviati == 1
    assert esito["ok"] is False
    assert esito["order_unknown"] is False, f"rifiuto esplicito trattato come esito ignoto: {esito!r}"


def test_timeout_esplicito_di_betfair_resta_ignoto() -> None:
    """ExecutionReportStatus TIMEOUT: Betfair stesso dice che l'esito non e' noto."""
    sessione = _SessioneContaInvii([{"result": {"status": "TIMEOUT", "instructionReports": [
        {"status": "TIMEOUT"}]}}])

    esito = _place(_client(sessione))

    assert esito["ok"] is False
    assert esito["order_unknown"] is True


def test_successo_completo_resta_successo() -> None:
    sessione = _SessioneContaInvii([{"result": {"status": "SUCCESS", "instructionReports": [
        {"status": "SUCCESS", "betId": "271828", "sizeMatched": 5.0}]}}])

    esito = _place(_client(sessione))

    assert esito["ok"] is True
    assert esito["result"]["instructionReports"][0]["betId"] == "271828"
