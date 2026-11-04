"""OrderRouter (hedge del cashout): una sessione scaduta va recuperata come nel TradingEngine, senza mai re-inviare.

Su `main` `ada4076` il TradingEngine chiama `betfair_service.handle_session_expiry` quando `place_bet` risponde
SESSION_EXPIRED / INVALID_SESSION (trading_engine.py:2368, :2539); `OrderRouter.place`, usato dagli hedge del cashout
(cashout_wiring.py:89), no: dopo un hedge rifiutato per sessione scaduta la sessione restava giu' finche' un altro
percorso non se ne accorgeva. L'ordine resta FALLITO (Betfair lo ha rifiutato prima di piazzarlo): mai un secondo invio.
"""
from __future__ import annotations

from typing import Any, Dict, List

import pytest

from core.order_router import OrderRouter


class _Client:
    def __init__(self, esito: Any) -> None:
        self.esito = esito
        self.invii = 0

    def place_bet(self, *, market_id, selection_id, side, price, size, customer_ref="", customer_order_ref=""):
        self.invii += 1
        if isinstance(self.esito, Exception):
            raise self.esito
        return self.esito


class _Service:
    def __init__(self, client: _Client) -> None:
        self.client = client
        self.recuperi: List[str] = []

    def get_client(self) -> _Client:
        return self.client

    def handle_session_expiry(self, reason: str = "SESSION_EXPIRED", abort_if=None) -> Dict[str, Any]:
        self.recuperi.append(reason)
        return {"recovered": True}


def _payload() -> Dict[str, Any]:
    return {"market_id": "1.2", "selection_id": 5, "bet_type": "LAY", "price": 2.0, "stake": 4.0,
            "customer_ref": "hedge-1"}


@pytest.mark.parametrize("errore", ["SESSION_EXPIRED", "API_ERROR: {'code': 'INVALID_SESSION_INFORMATION'}",
                                    "NOT_AUTHENTICATED"])
def test_block_sessione_scaduta_recuperata_e_ordine_non_reinviato(errore: str) -> None:
    client = _Client({"ok": False, "error": errore, "order_unknown": False})
    svc = _Service(client)

    esito = OrderRouter(svc).place(_payload())

    assert esito["placed"] is False and esito["ok"] is False
    assert client.invii == 1, "ordine re-inviato dopo una sessione scaduta"
    assert svc.recuperi == [errore], "sessione scaduta non recuperata"


def test_altri_errori_non_toccano_la_sessione() -> None:
    client = _Client({"ok": False, "error": "BET_FAILED: FAILURE INSUFFICIENT_FUNDS", "order_unknown": False})
    svc = _Service(client)

    OrderRouter(svc).place(_payload())

    assert svc.recuperi == []


def test_successo_non_tocca_la_sessione() -> None:
    client = _Client({"ok": True, "result": {"status": "SUCCESS", "instructionReports": [
        {"status": "SUCCESS", "betId": "9", "sizeMatched": 4.0}]}})
    svc = _Service(client)

    esito = OrderRouter(svc).place(_payload())

    assert esito["placed"] is True and svc.recuperi == []


def test_recupero_che_solleva_non_cambia_l_esito() -> None:
    client = _Client({"ok": False, "error": "SESSION_EXPIRED", "order_unknown": False})
    svc = _Service(client)

    def _boom(**_k):
        raise RuntimeError("relogin down")

    svc.handle_session_expiry = _boom
    esito = OrderRouter(svc).place(_payload())

    assert esito["placed"] is False and client.invii == 1


def test_servizio_senza_recupero_non_rompe_il_router() -> None:
    client = _Client({"ok": False, "error": "SESSION_EXPIRED", "order_unknown": False})

    class _SvcMinimo:
        def get_client(self):
            return client

    assert OrderRouter(_SvcMinimo()).place(_payload())["placed"] is False
