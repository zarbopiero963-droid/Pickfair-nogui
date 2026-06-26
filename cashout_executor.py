"""Executor del cashout (Fase 2.1-A, B5).

Consuma ``CMD_EXECUTE_CASHOUT`` (già pubblicato da ``RiskMiddleware`` su
``REQ_EXECUTE_CASHOUT``), piazza il bet di green-up tramite ``OrderRouter`` —
il seam unico sim/live consentito dai guardrail no-bypass — e pubblica
``CASHOUT_SUCCESS`` / ``CASHOUT_FAILED``.

Il componente è **dormiente** finché qualcuno non emette
``REQ_EXECUTE_CASHOUT`` (routing del segnale CASHOUT/CASHOUT_ALL = Fase 2.1-B,
oppure la UI). Da solo non innesca alcun bet autonomo.

Fail-closed real-money:
- le invarianti real-money hard girano SEMPRE (anche col ``SafetyLayer``, che
  non copre side allow-list, finitezza NaN/Inf né presenza di ``green_up``):
  campi richiesti (incl. ``green_up``), ``side∈{BACK,LAY}``, ``price>1`` finito,
  ``stake>0`` finito; payload invalido ⇒ ``CASHOUT_FAILED`` ``REJECTED``,
  nessun piazzamento;
- il lato (``side``) è quello calcolato a monte dalla matematica di green-up e
  **non viene mai defaultato**: un side mancante ⇒ rigetto (defaultare a LAY
  potrebbe piazzare un ordine reale che *aumenta* l'esposizione);
- esito incerto (``order_unknown`` da timeout/rete) ⇒ ``CASHOUT_FAILED``
  ``status=AMBIGUOUS``, **mai** retry (un retry creerebbe un secondo bet reale:
  decide la riconciliazione);
- ordine piazzato ma **non** abbinato (``matched<=0``) ⇒ ``CASHOUT_FAILED``
  ``status=UNMATCHED``: il green-up non è bloccato (il ``bet_id`` è comunque
  riportato per non lasciare l'ordine orfano);
- solo ``placed and matched>0`` ⇒ ``CASHOUT_SUCCESS`` con ``green_up``.

NOTA (Fase 2.1-B, requisiti di attivazione sicura): finché 2.1-A è dormiente
nessun ordine reale parte, ma prima di attivare il routing la 2.1-B deve:
(a) aggiungere il consumer di ``CASHOUT_FAILED`` con notifica operatore per
AMBIGUOUS/UNMATCHED/ERROR (oggi solo ``CASHOUT_SUCCESS`` è consumato da
``telegram_sender``); (b) uniformare lo shape di ``CASHOUT_FAILED`` (dict) anche
in ``RiskMiddleware`` (oggi pubblica una stringa); (c) gestire il **lifecycle
dell'ordine non abbinato/ambiguo**: un LIMIT accettato ma non subito abbinato
resta vivo e può abbinarsi *dopo* l'UNMATCHED (hedge non tracciato) — va
cancellato oppure persistito/accodato per la riconciliazione (il ``bet_id`` è
già nel payload ``CASHOUT_FAILED``).
"""
from __future__ import annotations

import logging
import math
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

CMD_EXECUTE_CASHOUT = "CMD_EXECUTE_CASHOUT"
CASHOUT_SUCCESS = "CASHOUT_SUCCESS"
CASHOUT_FAILED = "CASHOUT_FAILED"


class CashoutExecutor:
    """Piazza il bet di green-up per il cashout e pubblica l'esito sul bus."""

    _VALID_SIDES = {"BACK", "LAY"}

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

        # Fail-closed: le invarianti real-money hard girano SEMPRE (anche col
        # SafetyLayer iniettato, che NON copre side allow-list, finitezza
        # NaN/Inf né presenza di green_up). Se c'è il SafetyLayer si aggiunge
        # il suo schema. Il broad ``except`` è voluto: qualunque errore di
        # validazione diventa un REJECTED loggato e pubblicato, mai propagato a
        # crashare l'handler del bus.
        try:
            self._enforce_hard_invariants(payload)
            if self.safety_layer is not None:
                self.safety_layer.validate_cashout_request(payload)
        except Exception as exc:  # noqa: BLE001 - fail-closed intenzionale
            self._fail(f"validation:{exc}", status="REJECTED", payload=payload)
            return

        try:
            place_payload = self._build_place_payload(payload)
        except KeyError as exc:
            self._fail(f"campo_mancante:{exc}", status="REJECTED", payload=payload)
            return

        try:
            result = self.order_router.place(place_payload)
        except Exception as exc:  # noqa: BLE001 - esito ignoto => fail-closed
            self._fail(f"place_exception:{exc}", status="ERROR", payload=payload)
            return

        self._handle_result(result if isinstance(result, dict) else {}, payload)

    @classmethod
    def _enforce_hard_invariants(cls, payload: Dict[str, Any]) -> None:
        """Invarianti real-money applicate SEMPRE, anche col SafetyLayer.

        Lo schema del SafetyLayer verifica i bound price/stake e i tipi, ma NON
        la side allow-list (un ``side`` arbitrario verrebbe coerciato dal client
        live), NON la finitezza (NaN/Inf passano i confronti ``<=``) e NON è
        garantita la presenza di ``green_up`` (un cashout senza green_up
        riporterebbe un P&L falso). Difesa in profondità, indipendente
        dall'injection.
        """
        for field in ("market_id", "selection_id", "side", "price", "stake", "green_up"):
            if payload.get(field) in (None, ""):
                raise ValueError(f"campo_mancante:{field}")
        if str(payload.get("side", "")).upper() not in cls._VALID_SIDES:
            raise ValueError("side_invalido")
        price = cls._as_float(payload.get("price"))
        stake = cls._as_float(payload.get("stake"))
        green_up = cls._as_float(payload.get("green_up"))
        if not (math.isfinite(price) and math.isfinite(stake) and math.isfinite(green_up)):
            raise ValueError("valore_non_finito")
        if price <= 1.0:
            raise ValueError("price<=1")
        if stake <= 0.0:
            raise ValueError("stake<=0")

    @staticmethod
    def _build_place_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Costruisce il payload per ``OrderRouter.place``.

        ``side`` è il lato di green-up calcolato a monte: si usa ``payload["side"]``
        (KeyError se assente) — mai un default, che potrebbe aumentare l'esposizione.
        """
        return {
            "market_id": payload["market_id"],
            "selection_id": payload["selection_id"],
            "bet_type": str(payload["side"]).upper(),
            "price": payload["price"],
            "stake": payload["stake"],
            "event_key": str(payload.get("event_key", "")),
            "customer_ref": str(payload.get("source", "") or ""),
            "event_name": str(payload.get("event_name", "")),
            "market_name": str(payload.get("market_name", "")),
            "runner_name": str(payload.get("runner_name", "")),
        }

    def _handle_result(self, result: Dict[str, Any], payload: Dict[str, Any]) -> None:
        """Interpreta l'esito normalizzato del router e pubblica success/failed."""
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
