"""Consumer cashout-domain di ``CASHOUT_FAILED`` (Fase 2.1-B2.4b-2, B5).

Gestisce il **residuo** di un cashout fallito, split-per-status, **fail-closed**
e **senza retry automatico** (un retry creerebbe un secondo ordine reale: la
riconciliazione è manuale/operatore):

- ``status=UNMATCHED`` (hedge piazzato ma non abbinato, resta a riposo e potrebbe
  abbinarsi *dopo*, non tracciato): se ``bet_id`` e ``market_id`` sono presenti,
  esegue **un solo** cancel del resting tramite il cancel adapter (cashout-only);
  persiste **sempre** il record residuo con l'esito del cancel
  (``cancel_attempted``/``cancel_ok``/``cancel_error``); notifica l'operatore.
- ``status=AMBIGUOUS`` (esito ignoto da timeout/rete: l'ordine PUÒ esistere):
  **mai** cancel automatico (cancellerebbe un ordine forse abbinato, o fallirebbe
  su uno inesistente); persiste un record di **reconciliation**; notifica con
  **severità alta**.
- ``status ∈ {ERROR, REJECTED, FAILURE}``: notifica l'operatore; persiste un
  record diagnostico se c'è un ``bet_id`` o altro contesto utile.
- ``CASHOUT_SUCCESS`` con ``partial`` non falso (hedge abbinato in parte, verifica owner
  pre-merge #522): il residuo resta a riposo come per UNMATCHED ⇒ **un solo** cancel
  del residuo dello stesso ``bet_id``, record ``PARTIAL`` persistito, notifica.

Il cancel è limitato **esclusivamente** al residuo dell'hedge di cashout (UNMATCHED o
abbinato in parte): questo handler sottoscrive ``CASHOUT_FAILED`` e ``CASHOUT_SUCCESS`` e
chiama ``cancel`` solo in ``_cancel_residual`` — non tocca mai QUICK_BET / dutching /
cancel / replace.

Gli I/O (``cancel``/``persist``/``notify``) sono **iniettati** → testabile senza
broker, DB o Telegram. ``persist`` e ``notify`` non propagano mai eccezioni
all'EventBus (best-effort, loggate): l'handler non deve crashare il worker.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict

from core.trading_constants import CASHOUT_FAILED, CASHOUT_RESIDUAL_HANDLED, CASHOUT_SUCCESS

logger = logging.getLogger(__name__)

#: ``audit_events.event_json.type`` per i record di residuo e di reconciliation.
AUDIT_TYPE_RESIDUAL = "CASHOUT_RESIDUAL"
AUDIT_TYPE_RECONCILIATION = "CASHOUT_RECONCILIATION"


class CashoutResidualHandler:
    """Consuma ``CASHOUT_FAILED`` e gestisce il residuo (cancel/persist/notify)."""

    def __init__(
        self,
        *,
        cancel: Callable[[str, list], bool],
        persist: Callable[[Dict[str, Any]], None],
        notify: Callable[..., None],
    ) -> None:
        self._cancel = cancel        # cancel(market_id, bet_ids) -> bool (cashout-only)
        self._persist = persist      # persist(record: dict) -> None
        self._notify = notify        # notify(text, *, severity) -> None
        self._bus: Any = None

    def wire(self, bus: Any) -> None:
        """Sottoscrive il consumer di ``CASHOUT_FAILED`` (e solo quello)."""
        self._bus = bus
        bus.subscribe(CASHOUT_FAILED, self.on_cashout_failed)
        bus.subscribe(CASHOUT_SUCCESS, self.on_cashout_success)

    def on_cashout_success(self, payload: Any) -> None:
        """Hedge abbinato in parte: il residuo a riposo si gestisce come un UNMATCHED. Completo => nulla."""
        if not isinstance(payload, dict) or payload.get("partial") is False:
            return
        self._handle_unmatched(payload, status="PARTIAL")

    # =========================================================
    # HANDLER
    # =========================================================
    def on_cashout_failed(self, payload: Any) -> None:
        """Dispatch per status. Payload non-dict (shape legacy stringa) ⇒ diagnostico."""
        if not isinstance(payload, dict):
            # Shape legacy (es. RiskMiddleware pubblica una stringa): nessun cancel
            # (niente identità ordine), ma non resta silenzioso.
            self._safe_persist({
                "type": AUDIT_TYPE_RESIDUAL,
                "status": "UNKNOWN",
                "reason": str(payload),
            })
            self._safe_notify(f"CASHOUT_FAILED non strutturato: {payload}", severity="HIGH")
            return

        status = str(payload.get("status") or "").strip().upper()
        if status == "UNMATCHED":
            self._handle_unmatched(payload)
        elif status == "AMBIGUOUS":
            self._handle_ambiguous(payload)
        else:
            self._handle_other(payload, status)

    # =========================================================
    # UNMATCHED — un solo cancel sicuro del resting + persist + notify
    # =========================================================
    def _handle_unmatched(self, payload: Dict[str, Any], status: str = "UNMATCHED") -> None:
        bet_id = self._str(payload.get("bet_id"))
        market_id = self._str(payload.get("market_id"))
        cancel_attempted, cancel_ok, cancel_error = self._cancel_residual(market_id, bet_id)

        record = {
            "type": AUDIT_TYPE_RESIDUAL,
            "status": status,
            "cancel_attempted": cancel_attempted,
            "cancel_ok": cancel_ok,
            "cancel_error": cancel_error,
            "bet_id": bet_id,
            "market_id": market_id,
            "selection_id": payload.get("selection_id"),
            "matched": self._float(payload.get("matched")),
        }
        if status == "PARTIAL":
            record["requested"] = payload.get("requested")
            record["residual"] = payload.get("residual")
        self._safe_persist(record)
        self._publish_residual_outcome(payload, cancel_attempted=cancel_attempted, cancel_ok=cancel_ok,
                                       status=status)

        if not cancel_attempted:
            detail = "nessun cancel (bet_id/market_id mancanti)"
        elif cancel_ok:
            detail = "resting cancellato"
        else:
            detail = f"cancel NON confermato ({cancel_error or 'esito negativo'})"
        self._safe_notify(
            f"CASHOUT {status} market={market_id} bet={bet_id}: {detail}",
            severity="HIGH",
        )

    def _cancel_residual(self, market_id: str, bet_id: str) -> tuple:
        """Un solo cancel del residuo a riposo dell'hedge, mai retry. -> (tentato, confermato, errore)."""
        if not (bet_id and market_id):      # cancel SOLO con l'identità dell'ordine
            return False, False, ""
        try:
            return True, bool(self._cancel(market_id, [bet_id])), ""
        except Exception as exc:  # noqa: BLE001 - cancel best-effort, mai crash
            return True, False, str(exc)

    def _publish_residual_outcome(self, payload: Dict[str, Any], *, cancel_attempted: bool,
                                  cancel_ok: bool, status: str = "UNMATCHED") -> None:
        """Esito esplicito del residuo UNMATCHED (rilievo Codex #522): chi attende l'hedge (barriera
        d'ammissione del runtime) lo considera chiuso SOLO con cancel confermato. Mai raise."""
        if self._bus is None:
            return
        out = {
            "status": status,
            "cancel_attempted": bool(cancel_attempted),
            "cancel_ok": bool(cancel_ok),
            "bet_id": self._str(payload.get("bet_id")),
            "market_id": self._str(payload.get("market_id")),
            "selection_id": payload.get("selection_id"),
        }
        request_id = payload.get("cashout_request_id")
        if isinstance(request_id, str) and request_id:
            out["cashout_request_id"] = request_id
        try:
            self._bus.publish(CASHOUT_RESIDUAL_HANDLED, out)
        except Exception:  # noqa: BLE001 - diagnostica: mai crash del consumer
            logger.exception("[CashoutResidualHandler] publish CASHOUT_RESIDUAL_HANDLED fallito")

    # =========================================================
    # AMBIGUOUS — mai cancel, persist reconciliation + notify alta severità
    # =========================================================
    def _handle_ambiguous(self, payload: Dict[str, Any]) -> None:
        bet_id = self._str(payload.get("bet_id"))
        market_id = self._str(payload.get("market_id"))
        record = {
            "type": AUDIT_TYPE_RECONCILIATION,
            "status": "AMBIGUOUS",
            "cancel_attempted": False,  # mai cancel automatico su esito ignoto
            "bet_id": bet_id,
            "market_id": market_id,
            "selection_id": payload.get("selection_id"),
            "matched": self._float(payload.get("matched")),
            "reason": self._str(payload.get("reason")),
        }
        self._safe_persist(record)
        self._safe_notify(
            f"CASHOUT AMBIGUOUS market={market_id} bet={bet_id}: "
            f"esito ignoto, riconciliazione manuale richiesta (nessun cancel automatico)",
            severity="CRITICAL",
        )

    # =========================================================
    # ERROR / REJECTED / FAILURE — notify + persist diagnostico se c'è contesto
    # =========================================================
    def _handle_other(self, payload: Dict[str, Any], status: str) -> None:
        bet_id = self._str(payload.get("bet_id"))
        market_id = self._str(payload.get("market_id"))
        reason = self._str(payload.get("reason"))
        record = {
            "type": AUDIT_TYPE_RESIDUAL,
            "status": status or "UNKNOWN",
            "cancel_attempted": False,
            "bet_id": bet_id,
            "market_id": market_id,
            "selection_id": payload.get("selection_id"),
            "reason": reason,
        }
        # Persisti diagnostico solo se c'è qualcosa di utile da tracciare.
        if bet_id or market_id or reason:
            self._safe_persist(record)
        severity = "HIGH" if (status in {"ERROR", "FAILURE"}) else "INFO"
        self._safe_notify(
            f"CASHOUT {status or 'UNKNOWN'} market={market_id} bet={bet_id}: {reason or '-'}",
            severity=severity,
        )

    # =========================================================
    # BEST-EFFORT I/O WRAPPERS
    # =========================================================
    def _safe_persist(self, record: Dict[str, Any]) -> None:
        try:
            self._persist(record)
        except Exception:  # noqa: BLE001 - persist best-effort, mai crash dell'handler
            logger.exception("[CashoutResidualHandler] persist residuo fallita")

    def _safe_notify(self, text: str, *, severity: str) -> None:
        try:
            self._notify(text, severity=severity)
        except Exception:  # noqa: BLE001 - notify best-effort, mai crash dell'handler
            logger.exception("[CashoutResidualHandler] notify residuo fallita")

    @staticmethod
    def _str(value: Any) -> str:
        return str(value).strip() if value not in (None, "") else ""

    @staticmethod
    def _float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
