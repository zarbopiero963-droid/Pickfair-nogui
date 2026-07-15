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
import time
from typing import Any, Callable, Dict, Optional

import trading_config
from core.trading_constants import (
    CASHOUT_FAILED,
    CASHOUT_SUCCESS,
    CMD_EXECUTE_CASHOUT,
)

logger = logging.getLogger(__name__)

# Clamp di sicurezza sul grace auto-green: impedisce che un valore di config
# patologico (es. un typo 3600) blocchi l'handler del bus per minuti. Il default
# reale (trading_config.AUTO_GREEN_DELAY_SEC=2.5) e' ben dentro questo intervallo.
_MAX_AUTO_GREEN_DELAY_SEC = 30.0


class CashoutExecutor:
    """Piazza il bet di green-up per il cashout e pubblica l'esito sul bus."""

    _VALID_SIDES = {"BACK", "LAY"}

    def __init__(self, bus: Any, order_router: Any, safety_layer: Optional[Any] = None,
                 *, delay_provider: Optional[Callable[[], Any]] = None,
                 sleep_fn: Optional[Callable[[float], None]] = None) -> None:
        self.bus = bus
        self.order_router = order_router
        self.safety_layer = safety_layer
        # Grace auto-green OPT-IN (G5). ``delay_provider`` restituisce la config
        # corrente (RoserpinaConfig) letta LIVE a cashout-time; None => grace
        # disattivato (comportamento storico: green-up immediato). ``sleep_fn``
        # e' iniettabile per i test (nessun sleep reale).
        self._delay_provider = delay_provider
        self._sleep_fn = sleep_fn or time.sleep

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

        # Grace auto-green OPT-IN (G5): attende SOLO se armato, DOPO la validazione
        # fail-closed (un payload invalido rigetta subito, senza attesa) e PRIMA di
        # catturare il mode/piazzare. Default disarmato => nessuna attesa. L'attesa
        # e' bounded dal clamp; il green-up viene comunque piazzato dopo (mai
        # strandato). fail-open: qualunque errore nel provider => nessuna attesa.
        delay = self._auto_green_delay_seconds()
        if delay > 0.0:
            logger.info("[CashoutExecutor] grace auto-green %.3fs prima del green-up", delay)
            self._sleep_fn(delay)

        # Cattura il mode SIM/LIVE PRIMA del place: l'OrderRouter sceglie il
        # broker via get_client() all'inizio di place(), quindi catturarlo qui
        # (nessun I/O tra questa riga e get_client) riflette il broker effettivo.
        # Rileggerlo dopo place() esporrebbe a uno switch SIM/LIVE in volo (la
        # finestra della chiamata di rete) → flag sim errato (Codex P2).
        sim = self._is_simulation()
        try:
            result = self.order_router.place(place_payload)
        except Exception as exc:  # noqa: BLE001 - esito ignoto => fail-closed
            self._fail(f"place_exception:{exc}", status="ERROR", payload=payload)
            return

        self._handle_result(result if isinstance(result, dict) else {}, payload, sim)

    def _auto_green_delay_seconds(self) -> float:
        """Secondi di grace auto-green da applicare prima del green-up (>=0).

        OPT-IN: ritorna 0.0 (nessuna attesa) se non c'e' ``delay_provider`` o se
        ``auto_green_delay_enabled`` non e' True. FAIL-SAFE: ``auto_green_delay_sec``
        assente/non-finito/<=0 => ``trading_config.AUTO_GREEN_DELAY_SEC``. CLAMP a
        ``[0, _MAX_AUTO_GREEN_DELAY_SEC]`` (un typo di config non blocca il bus).
        FAIL-OPEN: qualunque errore di lettura config => 0.0 (mai bloccare un
        cashout reale per un problema di configurazione).
        """
        provider = self._delay_provider
        if provider is None:
            return 0.0
        try:
            config = provider()
            if not bool(getattr(config, "auto_green_delay_enabled", False)):
                return 0.0
            raw = getattr(config, "auto_green_delay_sec", None)
            sec = self._as_float(raw)
            if not (math.isfinite(sec) and sec > 0.0):
                sec = float(trading_config.AUTO_GREEN_DELAY_SEC)
            if not math.isfinite(sec) or sec <= 0.0:
                return 0.0
            return min(sec, _MAX_AUTO_GREEN_DELAY_SEC)
        except Exception:  # noqa: BLE001 - fail-open: config illeggibile non blocca il cashout
            return 0.0

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

    def _handle_result(self, result: Dict[str, Any], payload: Dict[str, Any],
                       sim: bool = False) -> None:
        """Interpreta l'esito normalizzato del router e pubblica success/failed.

        ``sim`` è il mode catturato al momento del piazzamento (non rieletto qui),
        usato per il sim-broadcast guard del ``CASHOUT_SUCCESS``.
        """
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
            # Flag per il sim-broadcast guard: un cashout eseguito in simulazione
            # NON deve fare broadcast del MASTER_CASHOUT ai follower reali
            # (telegram_sender._on_cashout_success lo filtra). Catturato al
            # piazzamento (non rieletto qui) per evitare uno switch in volo.
            "sim": bool(sim),
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

    def _is_simulation(self) -> bool:
        """True se il broker attivo è la simulazione (via ``OrderRouter.service``).

        Difensivo: se non determinabile ⇒ False (trattato come live). Usato solo
        per il flag ``sim`` del ``CASHOUT_SUCCESS`` (sim-broadcast guard).
        """
        try:
            svc = getattr(self.order_router, "service", None)
            fn = getattr(svc, "is_simulation_mode", None)
            return bool(fn()) if callable(fn) else False
        except Exception:  # noqa: BLE001 - best-effort, mai crash sul publish
            return False

    @staticmethod
    def _as_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
