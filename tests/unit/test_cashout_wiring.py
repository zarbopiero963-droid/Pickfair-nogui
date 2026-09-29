"""Unit test del cablaggio unico della catena cashout (``cashout_wiring``, #461 PR04).

Il costruttore e' usato da ``HeadlessApp`` e da ``MiniPickfairGUI``: qui si
fissa il suo contratto su un ``EventBus`` REALE. La prova sui due entrypoint
veri sta in ``tests/acceptance/test_issue437_pr04.py``.
"""
from __future__ import annotations

import threading

import pytest

import cashout_wiring
from cashout_executor import CashoutExecutor
from cashout_request_bridge import CashoutRequestBridge
from cashout_residual_handler import CashoutResidualHandler
from core.event_bus import EventBus
from core.order_router import OrderRouter
from core.safety_layer import SafetyLayer
from core.trading_constants import (
    CASHOUT_FAILED,
    CMD_EXECUTE_CASHOUT,
    REQ_EXECUTE_CASHOUT,
)


class _Service:
    """Service minimo: il cablaggio non deve toccarlo al build."""

    def __init__(self):
        self.chiamate = []

    def get_client(self):
        self.chiamate.append("get_client")
        return None

    def is_simulation_mode(self):
        self.chiamate.append("is_simulation_mode")
        return True

    def get_live_client(self):
        self.chiamate.append("get_live_client")
        return None

    def get_simulation_broker(self):
        self.chiamate.append("get_simulation_broker")
        return None


@pytest.fixture
def bus():
    istanza = EventBus()
    yield istanza
    istanza.stop()


def _cabla(bus, service=None, **kw):
    return cashout_wiring.cabla_catena_cashout(
        bus=bus,
        betfair_service=service if service is not None else _Service(),
        persist=kw.get("persist", lambda record: None),
        notify=kw.get("notify", lambda text, *, severity="HIGH": None),
    )


def test_catena_con_safety_layer_e_un_consumer_per_topic(bus):
    service = _Service()
    catena = _cabla(bus, service)

    assert isinstance(catena.order_router, OrderRouter)
    assert catena.order_router.service is service
    assert isinstance(catena.cashout_executor, CashoutExecutor)
    assert catena.cashout_executor.order_router is catena.order_router
    assert isinstance(catena.cashout_executor.safety_layer, SafetyLayer)
    assert isinstance(catena.cashout_request_bridge, CashoutRequestBridge)
    assert isinstance(catena.cashout_residual_handler, CashoutResidualHandler)

    subs = {k: list(v) for k, v in bus._subscribers.items() if v}
    assert [cb.__self__ for cb in subs[REQ_EXECUTE_CASHOUT]] == [catena.cashout_request_bridge]
    assert [cb.__self__ for cb in subs[CMD_EXECUTE_CASHOUT]] == [catena.cashout_executor]
    assert [cb.__self__ for cb in subs[CASHOUT_FAILED]] == [catena.cashout_residual_handler]
    assert set(subs) == {REQ_EXECUTE_CASHOUT, CMD_EXECUTE_CASHOUT, CASHOUT_FAILED}


def test_cablaggio_inerte_nessun_evento_nessun_accesso_ai_servizi(bus):
    service = _Service()
    pubblicati = []
    originale = bus.publish
    bus.publish = lambda topic, payload=None, *a, **k: (pubblicati.append(topic), originale(topic, payload, *a, **k))

    _cabla(bus, service)

    assert pubblicati == []
    assert service.chiamate == []


def test_la_catena_non_crea_thread(bus):
    prima = {t.ident for t in threading.enumerate()}
    _cabla(bus)
    dopo = {t.ident for t in threading.enumerate()}
    assert dopo == prima


@pytest.mark.parametrize("manca", ["bus", "betfair_service"])
def test_dipendenze_mancanti_errore_esplicito(bus, manca):
    argomenti = {
        "bus": bus,
        "betfair_service": _Service(),
        "persist": lambda record: None,
        "notify": lambda text, *, severity="HIGH": None,
    }
    argomenti[manca] = None
    with pytest.raises(RuntimeError, match="non inizializzati"):
        cashout_wiring.cabla_catena_cashout(**argomenti)
    assert not any(bus._subscribers.get(t) for t in (REQ_EXECUTE_CASHOUT, CMD_EXECUTE_CASHOUT, CASHOUT_FAILED))


def test_persist_e_notify_sono_quelli_dell_entrypoint(bus):
    """Il residuo va al persist e al notify passati, risolti all'uso."""
    registrati, notifiche = [], []
    catena = _cabla(
        bus,
        persist=registrati.append,
        notify=lambda text, *, severity="HIGH": notifiche.append((severity, text)),
    )
    catena.cashout_residual_handler.on_cashout_failed({
        "status": "AMBIGUOUS", "reason": "order_unknown", "bet_id": "B1",
        "market_id": "1.1", "selection_id": 11, "matched": 0.0,
    })
    assert registrati and registrati[0]["type"] == "CASHOUT_RECONCILIATION"
    assert notifiche and notifiche[0][0] == "CRITICAL"


# ---------------------------------------------------------------------------
# Notifica del residuo via Telegram (spostata da headless_main, condivisa)
# ---------------------------------------------------------------------------
class _SenderCoda:
    def __init__(self):
        self.messaggi = []

    def queue_default_message(self, text, message_type=None):
        self.messaggi.append((message_type, text))


class _SenderDiretto:
    def __init__(self):
        self.messaggi = []

    def send_message(self, text):
        self.messaggi.append(text)


class _Servizio:
    def __init__(self, sender):
        self._sender = sender

    def get_sender(self):
        return self._sender


def test_notifica_in_coda_col_tipo_dedicato():
    sender = _SenderCoda()
    cashout_wiring.notifica_residuo_cashout(_Servizio(sender), "residuo X", severity="CRITICAL")
    assert sender.messaggi == [("CASHOUT_RESIDUAL", "[CRITICAL] residuo X")]


def test_notifica_diretta_se_la_coda_non_c_e():
    sender = _SenderDiretto()
    cashout_wiring.notifica_residuo_cashout(_Servizio(sender), "residuo Y")
    assert sender.messaggi == ["[HIGH] residuo Y"]


def test_senza_sender_ripiega_sul_global_poi_sul_log(monkeypatch, caplog):
    globale = _SenderCoda()
    monkeypatch.setattr(cashout_wiring, "get_telegram_sender", lambda: globale)
    cashout_wiring.notifica_residuo_cashout(_Servizio(None), "residuo Z", severity="INFO")
    assert globale.messaggi == [("CASHOUT_RESIDUAL", "[INFO] residuo Z")]

    def _rotto():
        raise RuntimeError("sender globale non inizializzato")

    monkeypatch.setattr(cashout_wiring, "get_telegram_sender", _rotto)
    with caplog.at_level("WARNING", logger="cashout_wiring"):
        cashout_wiring.notifica_residuo_cashout(_Servizio(None), "residuo W", severity="HIGH")
    assert "[CASHOUT_RESIDUAL][HIGH] residuo W" in caplog.text


def test_invio_che_solleva_non_propaga(caplog):
    class _SenderRotto:
        def queue_default_message(self, text, message_type=None):
            raise RuntimeError("rete giu'")

    with caplog.at_level("WARNING", logger="cashout_wiring"):
        cashout_wiring.notifica_residuo_cashout(_Servizio(_SenderRotto()), "residuo V")
    assert "[CASHOUT_RESIDUAL][HIGH] residuo V" in caplog.text
