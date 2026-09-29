"""Cablaggio unico della catena di esecuzione del cashout (#461 PR04, H28 #453).

``HeadlessApp`` e ``MiniPickfairGUI`` costruiscono la catena con la STESSA
funzione, cosi' i due entrypoint non possono divergere::

    REQ_EXECUTE_CASHOUT -> CashoutRequestBridge (normalizza, dedup 2 s per posizione)
    CMD_EXECUTE_CASHOUT -> CashoutExecutor (invarianti hard + SafetyLayer) -> OrderRouter
    CASHOUT_FAILED      -> CashoutResidualHandler (UNMATCHED: un cancel; AMBIGUOUS: mai)

Fino alla PR04 la catena esisteva solo in ``headless_main``: nella GUI un
CASHOUT arrivato dalla chat veniva rifiutato (``cashout_chain_not_wired``) e
il REQ_EXECUTE_CASHOUT dell'auto-close cadeva su un bus senza ascoltatori.

Qui si costruiscono solo i CONSUMER. Il publisher di REQ_EXECUTE_CASHOUT resta
il ``RuntimeController`` (router e resolver costruiti per segnale, dopo i gate
di emergenza, sessione, deploy e runtime attivo). Nessun thread e nessuna I/O
al build: cancel del residuo, persistenza e notifica si risolvono al momento
dell'uso. Un solo consumer per topic: un secondo bridge o executor sullo
stesso bus vorrebbe dire due hedge reali per un comando.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict

from cashout_cancel_adapter import CashoutCancelAdapter
from cashout_executor import CashoutExecutor
from cashout_request_bridge import CashoutRequestBridge
from cashout_residual_handler import CashoutResidualHandler
from core.order_router import OrderRouter
from core.safety_layer import SafetyLayer
from telegram_sender import get_telegram_sender

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CatenaCashout:
    """I quattro componenti cablati; l'entrypoint li espone con questi nomi."""

    order_router: OrderRouter
    cashout_executor: CashoutExecutor
    cashout_request_bridge: CashoutRequestBridge
    cashout_residual_handler: CashoutResidualHandler


def cabla_catena_cashout(
    *,
    bus: Any,
    betfair_service: Any,
    persist: Callable[[Dict[str, Any]], None],
    notify: Callable[..., None],
) -> CatenaCashout:
    """Sottoscrive al ``bus`` la catena di esecuzione + residuo del cashout.

    - ``OrderRouter``: seam unico di piazzamento sim/live (sceglie il broker
      attivo via ``betfair_service.get_client()`` a ogni invio);
    - ``CashoutExecutor``: consuma ``CMD_EXECUTE_CASHOUT`` con un
      ``SafetyLayer`` REALE (#437): ``validate_cashout_request`` aggiunge
      schema e tipi agli invarianti hard. Il costruttore del layer e' inerte e
      il bridge normalizza gia' i tipi sul percorso reale, quindi il layer non
      puo' rigettare cashout legittimi;
    - ``CashoutRequestBridge``: consuma ``REQ_EXECUTE_CASHOUT`` (dedup,
      normalizzazione, ``CASHOUT_FAILED`` strutturato) e pubblica il CMD;
    - ``CashoutResidualHandler``: consuma ``CASHOUT_FAILED``; il cancel passa
      dal ``CashoutCancelAdapter`` (solo cashout), ``persist`` e ``notify``
      sono quelli dell'entrypoint.

    Gli accessori del service sono risolti lazy (lambda): l'adapter legge
    ``is_simulation_mode``/broker solo a cancel-time, mai qui, cosi' un service
    incompleto non fa fallire il build e la scelta sim/live resta quella del
    momento del cancel reale.
    """
    if bus is None or betfair_service is None:
        raise RuntimeError("Bus/BetfairService non inizializzati per il wiring cashout")

    order_router = OrderRouter(betfair_service)
    cashout_executor = CashoutExecutor(bus, order_router, safety_layer=SafetyLayer())
    cashout_executor.wire()

    cashout_request_bridge = CashoutRequestBridge(bus)
    cashout_request_bridge.wire()

    svc = betfair_service
    residual_cancel = CashoutCancelAdapter(
        is_simulation=lambda: svc.is_simulation_mode(),
        live_cancel=lambda **kw: (c.cancel_orders(**kw) if (c := svc.get_live_client()) is not None else False),
        sim_cancel=lambda **kw: (b.cancel_orders(**kw) if (b := svc.get_simulation_broker()) is not None else False),
    )
    cashout_residual_handler = CashoutResidualHandler(
        cancel=residual_cancel.cancel,
        persist=persist,
        notify=notify,
    )
    cashout_residual_handler.wire(bus)

    logger.info(
        "Catena cashout cablata: "
        "CashoutRequestBridge[REQ_EXECUTE_CASHOUT] -> "
        "CashoutExecutor[CMD_EXECUTE_CASHOUT] -> OrderRouter; "
        "CashoutResidualHandler[CASHOUT_FAILED]"
    )
    return CatenaCashout(
        order_router=order_router,
        cashout_executor=cashout_executor,
        cashout_request_bridge=cashout_request_bridge,
        cashout_residual_handler=cashout_residual_handler,
    )


def risolvi_sender_telegram(telegram_service: Any) -> Any:
    """Sender Telegram per le notifiche del residuo.

    Il global ``get_telegram_sender()`` e' inizializzato solo se costruito con
    un client (in headless e nella GUI non lo e'), quindi prima si usa il
    sender del ``telegram_service`` (lo stesso che consuma CASHOUT_SUCCESS),
    poi il global come ripiego.
    """
    sender = None
    try:
        if telegram_service is not None:
            getter = getattr(telegram_service, "get_sender", None)
            if callable(getter):
                sender = getter()
            if sender is None:
                sender = getattr(telegram_service, "sender", None)
    except Exception:
        sender = None
    if sender is None:
        try:
            sender = get_telegram_sender()
        except Exception:
            sender = None
    return sender


def notifica_residuo_cashout(telegram_service: Any, text: str, *, severity: str = "HIGH") -> None:
    """Notifica all'operatore il residuo di un cashout (best-effort, mai solleva).

    Prova i metodi del sender in ordine (``queue_default_message`` → invio
    diretto); se nessuno e' disponibile o l'invio fallisce, logga. Il
    ``message_type`` dedicato distingue queste notifiche dagli altri invii.
    """
    msg = f"[{severity}] {text}"
    sender = risolvi_sender_telegram(telegram_service)
    if sender is not None:
        q = getattr(sender, "queue_default_message", None)
        if callable(q):
            try:
                q(msg, message_type="CASHOUT_RESIDUAL")
                return
            except Exception:
                logger.exception("Notifica residuo via queue_default_message fallita")
        for name in ("send_message", "enqueue_message", "send"):
            fn = getattr(sender, name, None)
            if callable(fn):
                try:
                    fn(msg)
                    return
                except Exception:
                    logger.exception("Notifica residuo via %s fallita", name)
    logger.warning("[CASHOUT_RESIDUAL][%s] %s", severity, text)
