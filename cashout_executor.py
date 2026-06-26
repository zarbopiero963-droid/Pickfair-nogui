"""Executor del cashout (Fase 2.1-A, B5).

Consuma ``CMD_EXECUTE_CASHOUT`` (già pubblicato da ``RiskMiddleware`` su
``REQ_EXECUTE_CASHOUT``), piazza il bet di green-up tramite ``OrderRouter`` —
il seam unico sim/live consentito dai guardrail no-bypass — e pubblica
``CASHOUT_SUCCESS`` / ``CASHOUT_FAILED``.

Il componente è **dormiente** finché qualcuno non emette
``REQ_EXECUTE_CASHOUT`` (routing del segnale CASHOUT/CASHOUT_ALL = Fase 2.1-B,
oppure la UI). Da solo non innesca alcun bet autonomo.

Fail-closed real-money:
- payload invalido (validato da ``SafetyLayer`` se presente) ⇒ ``CASHOUT_FAILED``,
  nessun piazzamento;
- esito incerto (``order_unknown`` da timeout/rete) ⇒ ``CASHOUT_FAILED``
  ``status=AMBIGUOUS``, **mai** retry (un retry creerebbe un secondo bet reale:
  decide la riconciliazione);
- ordine piazzato ma **non** abbinato (``matched<=0``) ⇒ ``CASHOUT_FAILED``
  ``status=UNMATCHED``: il green-up non è bloccato (il ``bet_id`` è comunque
  riportato per non lasciare l'ordine orfano);
- solo ``placed and matched>0`` ⇒ ``CASHOUT_SUCCESS`` con ``green_up``.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

CMD_EXECUTE_CASHOUT = "CMD_EXECUTE_CASHOUT"
CASHOUT_SUCCESS = "CASHOUT_SUCCESS"
CASHOUT_FAILED = "CASHOUT_FAILED"


class CashoutExecutor:
    """Piazza il bet di green-up per il cashout e pubblica l'esito sul bus."""

    def __init__(self, bus: Any, order_router: Any, safety_layer: Optional[Any] = None) -> None:
        self.bus = bus
        self.order_router = order_router
        self.safety_layer = safety_layer

    def wire(self) -> None:
        """Sottoscrive il consumer di ``CMD_EXECUTE_CASHOUT`` al bus."""
        self.bus.subscribe(CMD_EXECUTE_CASHOUT, self.on_cmd_execute_cashout)

    def on_cmd_execute_cashout(self, payload: Dict[str, Any]) -> None:
        """Esegue un singolo comando di cashout, fail-closed end-to-end."""
        if not isinstance(payload, dict):
            self._fail("payload_non_dict", status="REJECTED", payload={})
            return

        # Fail-closed: validazione (price>1, stake>0, campi richiesti).
        if self.safety_layer is not None:
            try:
                self.safety_layer.validate_cashout_request(payload)
            except Exception as exc:
                self._fail(f"validation:{exc}", status="REJECTED", payload=payload)
                return

        try:
            place_payload = {
                "market_id": payload["market_id"],
                "selection_id": payload["selection_id"],
                # "side" è già il lato da piazzare (opposto all'originale),
                # calcolato a monte dalla matematica di green-up.
                "bet_type": str(payload.get("side", "LAY")).upper(),
                "price": payload["price"],
                "stake": payload["stake"],
                "event_key": str(payload.get("event_key", "")),
                "customer_ref": str(payload.get("source", "") or ""),
                "event_name": str(payload.get("event_name", "")),
                "market_name": str(payload.get("market_name", "")),
                "runner_name": str(payload.get("runner_name", "")),
            }
        except KeyError as exc:
            self._fail(f"campo_mancante:{exc}", status="REJECTED", payload=payload)
            return

        try:
            result = self.order_router.place(place_payload)
        except Exception as exc:
            # Eccezione del broker/router: esito ignoto sul piazzamento ⇒ fail-closed.
            self._fail(f"place_exception:{exc}", status="ERROR", payload=payload)
            return

        result = result if isinstance(result, dict) else {}
        bet_id = result.get("bet_id")
        matched = self._as_float(result.get("matched"))

        if result.get("order_unknown"):
            # Esito incerto: l'ordine PUO' esistere ⇒ AMBIGUO, mai retry.
            self._fail("order_unknown", status="AMBIGUOUS", payload=payload,
                       bet_id=bet_id, matched=matched)
            return

        if not result.get("placed"):
            self._fail(result.get("error") or "not_placed", status="FAILURE",
                       payload=payload, bet_id=bet_id)
            return

        if matched <= 0.0:
            # Ordine a riposo non abbinato: green-up NON bloccato.
            self._fail("unmatched", status="UNMATCHED", payload=payload, bet_id=bet_id)
            return

        success = {
            "green_up": self._as_float(payload.get("green_up")),
            "matched": matched,
            "status": "DONE",
            "bet_id": bet_id,
            "market_id": str(payload.get("market_id", "")),
            "selection_id": payload.get("selection_id"),
        }
        logger.info("[CashoutExecutor] CASHOUT_SUCCESS matched=%s bet_id=%s", matched, bet_id)
        self.bus.publish(CASHOUT_SUCCESS, success)

    def _fail(self, reason: Any, *, status: str, payload: Dict[str, Any],
              bet_id: Any = None, matched: Any = 0.0) -> None:
        logger.warning("[CashoutExecutor] CASHOUT_FAILED reason=%s status=%s", reason, status)
        self.bus.publish(CASHOUT_FAILED, {
            "reason": str(reason),
            "status": str(status),
            "bet_id": bet_id,
            "matched": self._as_float(matched),
            "market_id": str(payload.get("market_id", "")) if isinstance(payload, dict) else "",
            "selection_id": payload.get("selection_id") if isinstance(payload, dict) else None,
        })

    @staticmethod
    def _as_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
