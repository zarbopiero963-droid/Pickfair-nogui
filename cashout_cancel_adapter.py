"""Adapter di cancellazione per il cashout (Fase 2.1-B2.1, design owner **C1**).

Il ``CashoutRouter`` (puro, 2.1-B1) inietta un callable
``cancel_orders(market_id, bet_ids)`` **posizionale** che deve ritornare un bool
"confermato / non confermato". I broker reali hanno però firme **keyword-only** e
shape di risposta diverse:

- **LIVE** ``BetfairClient.cancel_orders(*, market_id, bet_ids=None)`` →
  ``{"ok": bool, "status": ..., "result": {... instructionReports ...}}`` (il
  fallimento è nel dict, non come eccezione; i report grezzi sono **annidati**
  sotto ``result``);
- **SIM** ``SimulationBroker.cancel_orders(*, market_id, instructions=[...])`` →
  ``{"status": "SUCCESS", "instructionReports": [{"status": ...}], ...}`` (status
  top-level sempre SUCCESS, ma i report per-istruzione possono essere FAILURE).

Questo adapter incapsula in **un solo punto testato** (a) la selezione sim/live,
(b) l'adattamento della firma, (c) la normalizzazione della risposta a un bool —
così la conoscenza delle shape dei broker NON vive più nel router puro
(broker-agnostico). È iniettato a runtime (design C1: riceve i callable, zero
file forbidden in questa fase).

**Sicurezza**: un elenco di ``bet_ids`` vuoto NON viene inoltrato al broker — sia
``BetfairClient`` sia ``SimulationBroker`` interpretano "nessuna istruzione" come
"cancella **TUTTI** gli ordini unmatched del mercato"; l'adapter ritorna ``True``
(no-op) senza chiamare, per non flat-tare l'intero mercato per errore.

**Fail-closed**: qualsiasi eccezione o risposta ambigua → ``False`` (il router
salta la posizione invece di chiudere alla cieca lasciando il resting vivo).
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List

logger = logging.getLogger(__name__)

_OK_STATUSES = ("SUCCESS", "OK")


def response_confirms_cancel(result: Any) -> bool:
    """``True`` se la risposta del broker non porta ALCUN segnale di fallimento.

    Ispeziona ``ok``, ``status`` e ``instructionReports`` sia al livello top sia
    **annidati** sotto ``result`` (il wrapper live di ``BetfairClient`` ritorna
    ``ok``/``status`` SUCCESS al top e annida la risposta grezza — coi report
    per-istruzione — sotto ``result``). Un valore truthy non-dict (es. ``True`` o
    la lista dei bet_id cancellati) è considerato conferma.
    """
    if not result:
        return False
    if not isinstance(result, dict):
        return True
    if result.get("ok") is False:
        return False
    nested = result.get("result")
    for container in (result, nested if isinstance(nested, dict) else None):
        if not isinstance(container, dict):
            continue
        status = str(container.get("status") or "").strip().upper()
        if status and status not in _OK_STATUSES:
            return False
        for report in container.get("instructionReports") or []:
            if isinstance(report, dict):
                report_status = str(report.get("status") or "").strip().upper()
                if report_status and report_status not in _OK_STATUSES:
                    return False
    return True


class CashoutCancelAdapter:
    """Adatta il cancel dei broker al contratto bool del router (design C1)."""

    def __init__(
        self,
        *,
        is_simulation: Callable[[], bool],
        live_cancel: Callable[..., Any],
        sim_cancel: Callable[..., Any],
    ) -> None:
        self.is_simulation = is_simulation
        self.live_cancel = live_cancel
        self.sim_cancel = sim_cancel

    def cancel(self, market_id: str, bet_ids: List[str]) -> bool:
        """Cancella gli ordini resting indicati; ``True`` solo se CONFERMATO.

        Firma posizionale ``(market_id, bet_ids)`` = il contratto che il router
        si aspetta da ``cancel_orders``.
        """
        ids = [str(b).strip() for b in (bet_ids or []) if b and str(b).strip()]
        if not ids:
            # Niente da cancellare. NON inoltrare al broker: bet_ids vuoto =
            # "cancella TUTTI gli unmatched del mercato" lato Betfair/SIM.
            return True
        market = str(market_id or "").strip()
        if not market:
            logger.warning("[cashout_cancel] market_id vuoto: cancel non confermato")
            return False
        try:
            if self.is_simulation():
                instructions: List[Dict[str, Any]] = [{"betId": bid} for bid in ids]
                result = self.sim_cancel(market_id=market, instructions=instructions)
            else:
                result = self.live_cancel(market_id=market, bet_ids=ids)
        except Exception as exc:  # noqa: BLE001 - fail-closed: cancel non confermato
            logger.warning("[cashout_cancel] cancel fallito su %s: %s", market, exc)
            return False
        confirmed = response_confirms_cancel(result)
        if not confirmed:
            logger.warning("[cashout_cancel] cancel NON confermato su %s (%r)", market, result)
        return confirmed
