"""Bridge REQ→CMD dedicato al cashout (Fase 2.1-B2.4b-1, B5).

Questo è il seam REQ→CMD *cashout-only* della pipeline di cashout. Sottoscrive
**solo** ``REQ_EXECUTE_CASHOUT``, fa normalizzazione + validazione minima +
dedup e pubblica ``CMD_EXECUTE_CASHOUT`` (consumato da ``CashoutExecutor``).

Perché un bridge dedicato e non la ``RiskMiddleware`` completa: la
``RiskMiddleware`` è il bridge centrale di TUTTI gli order type (QUICK_BET,
DUTCHING, CANCEL, REPLACE *e* CASHOUT). Cablarla nel runtime live per accendere
il solo cashout avrebbe un blast radius inaccettabile sugli altri flussi ordini
(oggi serviti da ``_NullRiskMiddleware``). Questo bridge accende il seam del
cashout **senza toccare** quei flussi: è cashout-only per costruzione.

Scelte fail-closed (più strette di ``RiskMiddleware._handle_cashout``):
- ``side`` NON viene mai defaultato a ``LAY``: un side mancante/invalido ⇒
  ``CASHOUT_FAILED`` ``REJECTED`` (defaultare a LAY potrebbe pubblicare un CMD
  che *aumenta* l'esposizione invece di chiuderla);
- ``selection_id`` deve essere un intero **esatto**: ``55.9``/``True`` ⇒ reject
  (``int()`` li tronca silenziosamente a ``55``/``1`` ⇒ hedge sul runner
  sbagliato; anche i broker coercerebbero a valle);
- ``market_id`` ``None``/garbage ⇒ reject **prima** dello stringify (``str(None)``
  darebbe ``'None'``, non vuoto, che supererebbe il check e finirebbe a piazzare
  un ordine sotto market ``'None'``);
- ``CASHOUT_FAILED`` su payload invalido è **strutturato** (dict, stessa forma
  di ``CashoutExecutor._fail``), non una stringa: il consumer a valle legge
  ``status``/``reason``/``market_id``/``selection_id`` in modo uniforme
  (residuo 2.1-A (b): "uniformare lo shape di CASHOUT_FAILED");
- **dedup su identità posizione** ``(market_id, selection_id, side)`` calcolata
  **dopo** la normalizzazione, non sul payload raw: due cashout della stessa
  posizione entro la finestra sono un doppio-click/replay e vanno soppressi
  anche se ``source``/metadata/price differiscono (niente doppio hedge); due
  posizioni distinte hanno chiave diversa (niente falso merge — il resolver
  aggrega per posizione, quindi stessa ``(market, selection, side)`` = stessa
  posizione). La chiave è una stringa da ``(str, int, str)``: non può sollevare,
  quindi non esiste un path di dedup fail-open.

Dormiente by-design: il bridge **reagisce** a ``REQ_EXECUTE_CASHOUT``, non lo
**emette**. Finché nessun trigger runtime pubblica ``REQ_EXECUTE_CASHOUT``
(Fase 2.1-B2.4b-2, ``RuntimeController``), la catena resta inerte e nessun
ordine reale parte. La validazione real-money hard gira comunque a valle, nel
``CashoutExecutor`` (difesa in profondità indipendente da questo bridge).
"""
from __future__ import annotations

import logging
import math
import threading
import time
from typing import Any, Dict

from core.trading_constants import (
    CASHOUT_FAILED,
    CMD_EXECUTE_CASHOUT,
    REQ_EXECUTE_CASHOUT,
)

logger = logging.getLogger(__name__)

_VALID_SIDES = {"BACK", "LAY"}


class CashoutRequestBridge:
    """Normalizza ``REQ_EXECUTE_CASHOUT`` in ``CMD_EXECUTE_CASHOUT`` (cashout-only)."""

    def __init__(self, bus: Any, *, duplicate_window_sec: float = 2.0,
                 gc_window_sec: float = 15.0) -> None:
        self.bus = bus
        self._duplicate_window_sec = float(duplicate_window_sec)
        self._gc_window_sec = float(gc_window_sec)
        self._recent_requests: Dict[str, float] = {}
        self._lock = threading.Lock()

    def wire(self) -> None:
        """Sottoscrive il consumer di ``REQ_EXECUTE_CASHOUT`` (e solo quello)."""
        self.bus.subscribe(REQ_EXECUTE_CASHOUT, self.on_req_execute_cashout)

    # =========================================================
    # HANDLER
    # =========================================================
    def on_req_execute_cashout(self, payload: Any) -> None:
        """Normalizza + valida (minima) + dedup ⇒ ``CMD_EXECUTE_CASHOUT``.

        Ordine: normalizzazione PRIMA del dedup, così la chiave di dedup è
        l'identità posizione normalizzata (e un payload invalido produce un
        ``CASHOUT_FAILED`` strutturato a prescindere dal dedup).
        """
        if not isinstance(payload, dict):
            self._fail("payload_non_dict", payload={})
            return

        try:
            normalized = self._normalize(payload)
        except _RejectCashout as exc:
            self._fail(str(exc), payload=payload)
            return
        except Exception as exc:  # noqa: BLE001 - fail-closed: input invalido => REJECTED
            self._fail(f"payload_invalido:{exc}", payload=payload)
            return

        if self._is_duplicate(normalized):
            logger.warning("[CashoutRequestBridge] REQ_EXECUTE_CASHOUT duplicata ignorata.")
            return

        logger.info("[CashoutRequestBridge] Forward REQ_EXECUTE_CASHOUT -> CMD_EXECUTE_CASHOUT")
        self.bus.publish(CMD_EXECUTE_CASHOUT, normalized)

    # =========================================================
    # NORMALIZE + VALIDATE (minima, fail-closed; hard invariants nell'executor)
    # =========================================================
    @classmethod
    def _normalize(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Valida i campi minimi e ritorna il payload normalizzato per il CMD.

        Validazione minima = sufficiente a non far passare garbage al CMD; la
        validazione real-money hard (difesa in profondità) resta nell'executor.
        ``side`` non è mai defaultato: mancante/invalido ⇒ reject.
        """
        market_id = cls._require_market_id(payload.get("market_id"))
        selection_id = cls._require_positive_int(payload.get("selection_id"), "selection_id")

        side = str(payload.get("side", "")).strip().upper()
        if side not in _VALID_SIDES:
            raise _RejectCashout("side_invalido")

        price = cls._require_finite(payload.get("price"), "price")
        stake = cls._require_finite(payload.get("stake"), "stake")
        green_up = cls._require_finite(payload.get("green_up"), "green_up")
        if price <= 1.0:
            raise _RejectCashout("price<=1")
        if stake <= 0.0:
            raise _RejectCashout("stake<=0")

        return {
            "market_id": market_id,
            "selection_id": selection_id,
            "side": side,
            "stake": stake,
            "price": price,
            "green_up": green_up,
            "original_pos": payload.get("original_pos"),
            "source": str(payload.get("source", "UI")),
            "event_key": str(payload.get("event_key", "")),
            "event_name": str(payload.get("event_name", "")),
            "market_name": str(payload.get("market_name", "")),
            "runner_name": str(payload.get("runner_name", "")),
        }

    @staticmethod
    def _require_market_id(value: Any) -> str:
        """Reject ``None``/bool/non-scalare PRIMA dello stringify.

        ``str(None)`` darebbe ``'None'`` (non vuoto) e supererebbe il check di
        presenza: un ``market_id`` nullo finirebbe a piazzare un ordine sotto il
        market letterale ``'None'``. Solo stringhe/numeri non-bool sono ammessi.
        """
        if value is None or isinstance(value, (bool, dict, list, tuple, set)):
            raise _RejectCashout("market_id_mancante")
        # float non-finito (NaN/Inf): ``str()`` darebbe ``'nan'``/``'inf'``, non
        # vuoti, che supererebbero il check e finirebbero a piazzare sotto un
        # market id sintetico (il JSON parser di Python accetta ``NaN``).
        if isinstance(value, float) and not math.isfinite(value):
            raise _RejectCashout("market_id_non_finito")
        market_id = str(value).strip()
        if not market_id:
            raise _RejectCashout("market_id_mancante")
        return market_id

    @staticmethod
    def _require_positive_int(value: Any, field: str) -> int:
        """Esige un intero **esatto** positivo (no troncamento di float/bool).

        ``int(55.9)`` ⇒ ``55`` e ``int(True)`` ⇒ ``1`` cambierebbero silenziosamente
        il runner di destinazione: un valore non-integrale ⇒ reject.
        """
        if value is None or value == "":
            raise _RejectCashout(f"{field}_mancante")
        if isinstance(value, bool):
            raise _RejectCashout(f"{field}_non_intero")
        if isinstance(value, int):
            out = value
        elif isinstance(value, float):
            if not value.is_integer():
                raise _RejectCashout(f"{field}_non_intero")
            out = int(value)
        else:
            s = str(value).strip()
            try:
                out = int(s)
            except (TypeError, ValueError) as exc:
                raise _RejectCashout(f"{field}_non_intero") from exc
        if out <= 0:
            raise _RejectCashout(f"{field}<=0")
        return out

    @staticmethod
    def _require_finite(value: Any, field: str) -> float:
        if value is None or value == "":
            raise _RejectCashout(f"{field}_mancante")
        if isinstance(value, bool):
            raise _RejectCashout(f"{field}_non_numerico")
        try:
            out = float(value)
        except (TypeError, ValueError) as exc:
            raise _RejectCashout(f"{field}_non_numerico") from exc
        if not math.isfinite(out):
            raise _RejectCashout(f"{field}_non_finito")
        return out

    # =========================================================
    # DEDUP (anti double-click / replay ravvicinato, su identità posizione)
    # =========================================================
    def _is_duplicate(self, normalized: Dict[str, Any]) -> bool:
        """True se la STESSA posizione è già stata inoltrata entro la finestra.

        La chiave è l'identità posizione ``(market_id, selection_id, side)``,
        non il payload raw: ``source``/metadata/price-drift non la cambiano, così
        lo stesso cashout da canali diversi non genera un doppio hedge. La chiave
        è una stringa da ``(str, int, str)`` ⇒ non può sollevare (niente path di
        dedup fail-open).
        """
        key = "{}|{}|{}".format(
            normalized["market_id"], normalized["selection_id"], normalized["side"]
        )
        now = time.time()
        with self._lock:
            self._cleanup_old_requests(now)
            previous_ts = self._recent_requests.get(key)
            if previous_ts is not None and (now - previous_ts) < self._duplicate_window_sec:
                return True
            self._recent_requests[key] = now
            return False

    def _cleanup_old_requests(self, now: float) -> None:
        self._recent_requests = {
            k: ts for k, ts in self._recent_requests.items()
            if now - ts <= self._gc_window_sec
        }

    # =========================================================
    # FAIL-CLOSED OUTPUT
    # =========================================================
    def _fail(self, reason: Any, *, payload: Dict[str, Any]) -> None:
        """Pubblica un ``CASHOUT_FAILED`` strutturato (REJECTED) sul payload invalido."""
        logger.warning("[CashoutRequestBridge] CASHOUT_FAILED reason=%s status=REJECTED", reason)
        safe = payload if isinstance(payload, dict) else {}
        raw_market = safe.get("market_id")
        self.bus.publish(CASHOUT_FAILED, {
            "reason": str(reason),
            "status": "REJECTED",
            "bet_id": None,
            "matched": 0.0,
            "market_id": str(raw_market) if isinstance(raw_market, (str, int, float)) and not isinstance(raw_market, bool) else "",
            "selection_id": safe.get("selection_id"),
        })


class _RejectCashout(Exception):
    """Reject interno con reason machine-readable (mai propagato fuori dal bridge)."""
