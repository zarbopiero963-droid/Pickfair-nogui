"""Bridge REQ→CMD dedicato al cashout (Fase 2.1-B2.4b-1, B5).

Questo è il seam REQ→CMD *cashout-only* della pipeline di cashout. Sottoscrive
**solo** ``REQ_EXECUTE_CASHOUT``, fa dedup + normalizzazione + validazione
minima e pubblica ``CMD_EXECUTE_CASHOUT`` (consumato da ``CashoutExecutor``).

Perché un bridge dedicato e non la ``RiskMiddleware`` completa: la
``RiskMiddleware`` è il bridge centrale di TUTTI gli order type (QUICK_BET,
DUTCHING, CANCEL, REPLACE *e* CASHOUT). Cablarla nel runtime live per accendere
il solo cashout avrebbe un blast radius inaccettabile sugli altri flussi ordini
(oggi serviti da ``_NullRiskMiddleware``). Questo bridge accende il seam del
cashout **senza toccare** quei flussi: è cashout-only per costruzione.

Differenze fail-closed rispetto a ``RiskMiddleware._handle_cashout``:
- ``side`` NON viene mai defaultato a ``LAY``: un side mancante/invalido ⇒
  ``CASHOUT_FAILED`` ``REJECTED`` (defaultare a LAY potrebbe pubblicare un CMD
  che *aumenta* l'esposizione invece di chiuderla);
- ``CASHOUT_FAILED`` su payload invalido è **strutturato** (dict, stessa forma
  di ``CashoutExecutor._fail``), non una stringa: l'unico consumer a valle deve
  poter leggere ``status``/``reason``/``market_id``/``selection_id`` in modo
  uniforme (residuo 2.1-A (b): "uniformare lo shape di CASHOUT_FAILED").

Dormiente by-design: il bridge **reagisce** a ``REQ_EXECUTE_CASHOUT``, non lo
**emette**. Finché nessun trigger runtime pubblica ``REQ_EXECUTE_CASHOUT``
(Fase 2.1-B2.4b-2, ``RuntimeController``), la catena resta inerte e nessun
ordine reale parte. La validazione real-money hard gira comunque a valle, nel
``CashoutExecutor`` (difesa in profondità indipendente da questo bridge).
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import threading
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

REQ_EXECUTE_CASHOUT = "REQ_EXECUTE_CASHOUT"
CMD_EXECUTE_CASHOUT = "CMD_EXECUTE_CASHOUT"
CASHOUT_FAILED = "CASHOUT_FAILED"

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
        """Dedup + validazione minima + normalizzazione ⇒ ``CMD_EXECUTE_CASHOUT``."""
        if not isinstance(payload, dict):
            self._fail("payload_non_dict", payload={})
            return

        if self._is_duplicate(payload):
            logger.warning("[CashoutRequestBridge] REQ_EXECUTE_CASHOUT duplicata ignorata.")
            return

        try:
            normalized = self._normalize(payload)
        except _RejectCashout as exc:
            self._fail(str(exc), payload=payload)
            return
        except Exception as exc:  # noqa: BLE001 - fail-closed: input invalido => REJECTED
            self._fail(f"payload_invalido:{exc}", payload=payload)
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
        market_id = str(payload.get("market_id", "")).strip()
        if not market_id:
            raise _RejectCashout("market_id_mancante")

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
    def _require_positive_int(value: Any, field: str) -> int:
        if value in (None, ""):
            raise _RejectCashout(f"{field}_mancante")
        try:
            out = int(value)
        except (TypeError, ValueError):
            raise _RejectCashout(f"{field}_non_intero")
        if out <= 0:
            raise _RejectCashout(f"{field}<=0")
        return out

    @staticmethod
    def _require_finite(value: Any, field: str) -> float:
        if value in (None, ""):
            raise _RejectCashout(f"{field}_mancante")
        try:
            out = float(value)
        except (TypeError, ValueError):
            raise _RejectCashout(f"{field}_non_numerico")
        if not math.isfinite(out):
            raise _RejectCashout(f"{field}_non_finito")
        return out

    # =========================================================
    # DEDUP (anti double-click / replay ravvicinato)
    # =========================================================
    def _is_duplicate(self, payload: Dict[str, Any]) -> bool:
        try:
            req_hash = self._request_hash(payload)
            now = time.time()
            with self._lock:
                self._cleanup_old_requests(now)
                previous_ts = self._recent_requests.get(req_hash)
                if previous_ts is not None and (now - previous_ts) < self._duplicate_window_sec:
                    return True
                self._recent_requests[req_hash] = now
                return False
        except Exception as exc:  # noqa: BLE001 - dedup best-effort, mai fail-open su crash
            logger.error("[CashoutRequestBridge] Errore calcolo duplicate hash: %s", exc)
            return False

    def _cleanup_old_requests(self, now: float) -> None:
        self._recent_requests = {
            h: ts for h, ts in self._recent_requests.items()
            if now - ts <= self._gc_window_sec
        }

    @classmethod
    def _request_hash(cls, payload: Dict[str, Any]) -> str:
        safe = cls._make_hashable(payload)
        encoded = json.dumps(safe, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @classmethod
    def _make_hashable(cls, payload: Any) -> Any:
        if isinstance(payload, dict):
            return {str(k): cls._make_hashable(v) for k, v in sorted(payload.items())}
        if isinstance(payload, (list, tuple)):
            return [cls._make_hashable(v) for v in payload]
        if isinstance(payload, (str, int, float, bool)) or payload is None:
            return payload
        return str(payload)

    # =========================================================
    # FAIL-CLOSED OUTPUT
    # =========================================================
    def _fail(self, reason: Any, *, payload: Dict[str, Any]) -> None:
        """Pubblica un ``CASHOUT_FAILED`` strutturato (REJECTED) sul payload invalido."""
        logger.warning("[CashoutRequestBridge] CASHOUT_FAILED reason=%s status=REJECTED", reason)
        safe = payload if isinstance(payload, dict) else {}
        self.bus.publish(CASHOUT_FAILED, {
            "reason": str(reason),
            "status": "REJECTED",
            "bet_id": None,
            "matched": 0.0,
            "market_id": str(safe.get("market_id", "")),
            "selection_id": safe.get("selection_id"),
        })


class _RejectCashout(Exception):
    """Reject interno con reason machine-readable (mai propagato fuori dal bridge)."""
