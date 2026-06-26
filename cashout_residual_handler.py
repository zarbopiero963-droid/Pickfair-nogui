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

Il cancel è limitato **esclusivamente** al cashout UNMATCHED: questo handler
sottoscrive solo ``CASHOUT_FAILED`` e chiama ``cancel`` solo in
``_handle_unmatched`` — non tocca mai QUICK_BET / dutching / cancel / replace.

Gli I/O (``cancel``/``persist``/``notify``) sono **iniettati** → testabile senza
broker, DB o Telegram. ``persist`` e ``notify`` non propagano mai eccezioni
all'EventBus (best-effort, loggate): l'handler non deve crashare il worker.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict

from core.trading_constants import CASHOUT_FAILED

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

    def wire(self, bus: Any) -> None:
        """Sottoscrive il consumer di ``CASHOUT_FAILED`` (e solo quello)."""
        bus.subscribe(CASHOUT_FAILED, self.on_cashout_failed)

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
    def _handle_unmatched(self, payload: Dict[str, Any]) -> None:
        bet_id = self._str(payload.get("bet_id"))
        market_id = self._str(payload.get("market_id"))

        cancel_attempted = False
        cancel_ok = False
        cancel_error = ""
        # Cancel SOLO se abbiamo l'identità dell'ordine; un solo tentativo, mai retry.
        if bet_id and market_id:
            cancel_attempted = True
            try:
                cancel_ok = bool(self._cancel(market_id, [bet_id]))
            except Exception as exc:  # noqa: BLE001 - cancel best-effort, mai crash
                cancel_error = str(exc)
                cancel_ok = False

        record = {
            "type": AUDIT_TYPE_RESIDUAL,
            "status": "UNMATCHED",
            "cancel_attempted": cancel_attempted,
            "cancel_ok": cancel_ok,
            "cancel_error": cancel_error,
            "bet_id": bet_id,
            "market_id": market_id,
            "selection_id": payload.get("selection_id"),
            "matched": self._float(payload.get("matched")),
        }
        self._safe_persist(record)

        if not cancel_attempted:
            detail = "nessun cancel (bet_id/market_id mancanti)"
        elif cancel_ok:
            detail = "resting cancellato"
        else:
            detail = f"cancel NON confermato ({cancel_error or 'esito negativo'})"
        self._safe_notify(
            f"CASHOUT UNMATCHED market={market_id} bet={bet_id}: {detail}",
            severity="HIGH",
        )

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
