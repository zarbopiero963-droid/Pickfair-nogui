"""
core/safety_layer.py

Enterprise Safety Layer per Pickfair.
Obiettivi:
- validazione schema payload OMS/EventBus
- enforcement invariants di rischio
- watchdog anti-freeze
- checker saghe pendenti
- sanity checks di mercato e ordini
- API semplice e integrabile senza rompere il repo
"""

from __future__ import annotations

import logging
import math
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from core.type_helpers import safe_float, safe_int, safe_str

logger = logging.getLogger(__name__)


# =========================================================
# EXCEPTIONS
# =========================================================

class SafetyLayerError(Exception):
    """Base exception per Safety Layer."""


class PayloadValidationError(SafetyLayerError):
    """Payload non valido."""


class RiskInvariantError(SafetyLayerError):
    """Violazione regola di rischio."""


class MarketSanityError(SafetyLayerError):
    """Mercato/quote/liquidità non sane."""


# =========================================================
# DATA MODELS
# =========================================================

@dataclass
class SchemaField:
    name: str
    required: bool = True
    allowed_types: Tuple[type, ...] = field(default_factory=tuple)
    allow_none: bool = False


@dataclass
class WatchdogState:
    name: str
    last_ping: float = 0.0
    timeout_sec: float = 5.0
    enabled: bool = True
    triggered: bool = False
    last_error: str = ""


@dataclass
class PendingSagaRecord:
    customer_ref: str
    market_id: str
    selection_id: Optional[str] = None
    status: str = "PENDING"
    age_sec: float = 0.0
    raw_payload: Optional[str] = None


@dataclass(frozen=True)
class LiveExecutionGateDecision:
    """Esito deterministico dei gate SIM/LIVE."""

    allowed: bool
    effective_execution_mode: str
    reason_code: str
    refusal_message: str = ""


def assert_live_gate_or_refuse(
    *,
    execution_mode: Any,
    live_enabled: Any,
    live_readiness_ok: Any,
    kill_switch: Any,
) -> LiveExecutionGateDecision:
    """
    Gate hard fail-closed per abilitare LIVE.

    Regole:
    - default SIMULATION
    - LIVE solo se requested + live_enabled + readiness_ok + kill_switch non attivo
    - qualsiasi valore ambiguo/mancante => rifiuto LIVE
    """
    mode_raw = str(execution_mode or "").strip().upper()
    requested_live = mode_raw == "LIVE"

    if bool(kill_switch):
        return LiveExecutionGateDecision(
            allowed=False,
            effective_execution_mode="SIMULATION",
            reason_code="kill_switch_active",
            refusal_message="Kill switch attivo: LIVE disabilitato",
        )

    if not requested_live:
        # include anche mode mancante/invalido: fail-closed su SIMULATION
        return LiveExecutionGateDecision(
            allowed=False,
            effective_execution_mode="SIMULATION",
            reason_code=("simulation_mode_forced" if mode_raw == "SIMULATION" else "invalid_or_missing_execution_mode"),
            refusal_message="Modalità non LIVE: percorso simulation-only",
        )

    if not bool(live_enabled):
        return LiveExecutionGateDecision(
            allowed=False,
            effective_execution_mode="SIMULATION",
            reason_code="live_not_enabled",
            refusal_message="LIVE richiesto ma gate live_enabled=False",
        )

    if not bool(live_readiness_ok):
        return LiveExecutionGateDecision(
            allowed=False,
            effective_execution_mode="SIMULATION",
            reason_code="live_readiness_not_ok",
            refusal_message="LIVE richiesto ma readiness non OK",
        )

    return LiveExecutionGateDecision(
        allowed=True,
        effective_execution_mode="LIVE",
        reason_code="live_allowed",
    )


# =========================================================
# SAFETY LAYER
# =========================================================

class SafetyLayer:
    """
    Layer centrale di sicurezza del sistema.
    Non dipende dal resto del repo e può essere aggiunto gradualmente.
    """

    QUICK_BET_REQUEST_SCHEMA = [
        SchemaField("market_id", True, (str,)),
        SchemaField("selection_id", True, (int,)),
        SchemaField("bet_type", True, (str,)),
        SchemaField("price", True, (int, float)),
        SchemaField("stake", True, (int, float)),
        SchemaField("event_name", False, (str,)),
        SchemaField("market_name", False, (str,)),
        SchemaField("runner_name", False, (str,)),
        SchemaField("simulation_mode", False, (bool,)),
        SchemaField("source", False, (str,)),
    ]

    QUICK_BET_SUCCESS_SCHEMA = [
        SchemaField("market_id", True, (str,)),
        SchemaField("selection_id", True, (int,)),
        SchemaField("bet_type", True, (str,)),
        SchemaField("price", True, (int, float)),
        SchemaField("stake", True, (int, float)),
        SchemaField("matched", True, (int, float)),
        SchemaField("status", True, (str,)),
        SchemaField("sim", True, (bool,)),
        SchemaField("runner_name", False, (str,)),
        SchemaField("event_name", False, (str,)),
        SchemaField("market_name", False, (str,)),
        SchemaField("micro", False, (bool,)),
        SchemaField("recovered", False, (bool,)),
        SchemaField("new_balance", False, (int, float)),
    ]

    DUTCHING_REQUEST_SCHEMA = [
        SchemaField("market_id", True, (str,)),
        SchemaField("market_type", False, (str,)),
        SchemaField("event_name", False, (str,)),
        SchemaField("market_name", False, (str,)),
        SchemaField("results", True, (list,)),
        SchemaField("bet_type", True, (str,)),
        SchemaField("total_stake", True, (int, float)),
        SchemaField("use_best_price", False, (bool,)),
        SchemaField("simulation_mode", False, (bool,)),
        SchemaField("auto_green", False, (bool,)),
        SchemaField("source", False, (str,)),
    ]

    DUTCHING_SUCCESS_SCHEMA = [
        SchemaField("market_id", True, (str,)),
        SchemaField("bet_type", True, (str,)),
        SchemaField("selections", True, (list,)),
        SchemaField("matched", True, (int, float)),
        SchemaField("status", True, (str,)),
        SchemaField("sim", True, (bool,)),
        SchemaField("total_stake", True, (int, float)),
        SchemaField("event_name", False, (str,)),
        SchemaField("market_name", False, (str,)),
        SchemaField("recovered", False, (bool,)),
    ]

    CASHOUT_REQUEST_SCHEMA = [
        SchemaField("market_id", True, (str,)),
        SchemaField("selection_id", True, (int,)),
        SchemaField("side", True, (str,)),
        SchemaField("stake", True, (int, float)),
        SchemaField("price", True, (int, float)),
        SchemaField("green_up", True, (int, float)),
    ]

    CASHOUT_SUCCESS_SCHEMA = [
        SchemaField("green_up", True, (int, float)),
        SchemaField("matched", True, (int, float)),
        SchemaField("status", True, (str,)),
        SchemaField("micro", False, (bool,)),
        SchemaField("recovered", False, (bool,)),
    ]

    def __init__(self, clock: Optional[Callable[[], float]] = None):
        self._lock = threading.RLock()
        self._watchdogs: Dict[str, WatchdogState] = {}
        self._watchdog_thread: Optional[threading.Thread] = None
        self._watchdog_stop = threading.Event()
        self._watchdog_interval_sec = 1.0
        self._watchdog_callback: Optional[Callable[[str, str], None]] = None
        self._clock = clock or time.time

    # =========================================================
    # SAFE CASTS
    # =========================================================

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        return safe_float(value, default)

    def _safe_int(self, value: Any, default: int = 0) -> int:
        return safe_int(value, default)

    def _safe_str(self, value: Any, default: str = "") -> str:
        return safe_str(value, default)

    def _safe_age_seconds(self, created_at: Any, stale_after_sec: float) -> float:
        if created_at in (None, ""):
            return stale_after_sec + 1.0

        now = self._clock()
        try:
            if isinstance(created_at, (int, float)):
                return max(0.0, now - float(created_at))

            created_str = str(created_at).strip()
            if not created_str:
                return stale_after_sec + 1.0

            # prova numerico come stringa
            try:
                return max(0.0, now - float(created_str))
            except Exception:
                pass

            # normalizza ISO con eventuale Z finale
            normalized = created_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            return max(0.0, now - dt.timestamp())
        except Exception:
            return stale_after_sec + 1.0

    # =========================================================
    # GENERIC SCHEMA VALIDATION
    # =========================================================

    def _validate_schema(self, payload: Dict[str, Any], schema: List[SchemaField], name: str):
        if not isinstance(payload, dict):
            raise PayloadValidationError(f"{name}: payload non dict")

        errors: List[str] = []

        for field_def in schema:
            exists = field_def.name in payload
            value = payload.get(field_def.name)

            if field_def.required and not exists:
                errors.append(f"{field_def.name}: missing")
                continue

            if not exists:
                continue

            if value is None and not field_def.allow_none:
                errors.append(f"{field_def.name}: None non consentito")
                continue

            if value is not None and field_def.allowed_types:
                if not isinstance(value, field_def.allowed_types):
                    errors.append(
                        f"{field_def.name}: tipo invalido ({type(value).__name__})"
                    )

        if errors:
            raise PayloadValidationError(f"{name}: " + " | ".join(errors))

    # =========================================================
    # EVENT / REQUEST VALIDATION
    # =========================================================

    def validate_quick_bet_request(self, payload: Dict[str, Any]) -> bool:
        self._validate_schema(payload, self.QUICK_BET_REQUEST_SCHEMA, "QUICK_BET_REQUEST")
        self._validate_common_order_rules(payload, allow_micro=True)
        return True

    def validate_quick_bet_success(self, payload: Dict[str, Any]) -> bool:
        self._validate_schema(payload, self.QUICK_BET_SUCCESS_SCHEMA, "QUICK_BET_SUCCESS")
        self._validate_success_payload_common(payload)
        return True

    def validate_dutching_request(self, payload: Dict[str, Any]) -> bool:
        self._validate_schema(payload, self.DUTCHING_REQUEST_SCHEMA, "DUTCHING_REQUEST")
        total_stake = self._safe_float(payload.get("total_stake"), 0.0)
        if total_stake <= 0:
            raise RiskInvariantError("DUTCHING_REQUEST: total_stake <= 0")

        results = payload.get("results") or []
        if not results:
            raise PayloadValidationError("DUTCHING_REQUEST: results vuoto")

        for idx, row in enumerate(results):
            if not isinstance(row, dict):
                raise PayloadValidationError(f"DUTCHING_REQUEST: results[{idx}] non dict")
            if "selectionId" not in row:
                raise PayloadValidationError(f"DUTCHING_REQUEST: results[{idx}].selectionId missing")
            if "price" not in row:
                raise PayloadValidationError(f"DUTCHING_REQUEST: results[{idx}].price missing")
            if "stake" not in row:
                raise PayloadValidationError(f"DUTCHING_REQUEST: results[{idx}].stake missing")

            price = self._safe_float(row.get("price"), 0.0)
            stake = self._safe_float(row.get("stake"), 0.0)
            if price <= 1.0:
                raise MarketSanityError(f"DUTCHING_REQUEST: results[{idx}] price <= 1")
            if stake <= 0:
                raise RiskInvariantError(f"DUTCHING_REQUEST: results[{idx}] stake <= 0")

        return True

    def validate_dutching_success(self, payload: Dict[str, Any]) -> bool:
        self._validate_schema(payload, self.DUTCHING_SUCCESS_SCHEMA, "DUTCHING_SUCCESS")
        matched = self._safe_float(payload.get("matched"), 0.0)
        total_stake = self._safe_float(payload.get("total_stake"), 0.0)
        if matched < 0 or total_stake <= 0:
            raise RiskInvariantError("DUTCHING_SUCCESS: matched/total_stake invalidi")
        return True

    def validate_cashout_request(self, payload: Dict[str, Any]) -> bool:
        self._validate_schema(payload, self.CASHOUT_REQUEST_SCHEMA, "CASHOUT_REQUEST")
        price = self._safe_float(payload.get("price"), 0.0)
        stake = self._safe_float(payload.get("stake"), 0.0)
        if price <= 1.0:
            raise MarketSanityError("CASHOUT_REQUEST: price <= 1")
        if stake <= 0:
            raise RiskInvariantError("CASHOUT_REQUEST: stake <= 0")
        return True

    def validate_cashout_success(self, payload: Dict[str, Any]) -> bool:
        self._validate_schema(payload, self.CASHOUT_SUCCESS_SCHEMA, "CASHOUT_SUCCESS")
        matched = self._safe_float(payload.get("matched"), -1.0)
        if matched < 0:
            raise RiskInvariantError("CASHOUT_SUCCESS: matched < 0")
        return True

    # =========================================================
    # COMMON ORDER RULES
    # =========================================================

    def _validate_common_order_rules(self, payload: Dict[str, Any], allow_micro: bool = True):
        bet_type = self._safe_str(payload.get("bet_type")).upper().strip()
        price = self._safe_float(payload.get("price"), 0.0)
        stake = self._safe_float(payload.get("stake"), 0.0)

        if bet_type not in ("BACK", "LAY"):
            raise PayloadValidationError(f"bet_type invalido: {bet_type}")

        if price <= 1.0:
            raise MarketSanityError("price <= 1.0")
        if not math.isfinite(price):
            raise MarketSanityError("price non finite")

        if stake <= 0:
            raise RiskInvariantError("stake <= 0")
        if not math.isfinite(stake):
            raise RiskInvariantError("stake non finite")

        if not allow_micro and stake < 2.0:
            raise RiskInvariantError("stake < 2.0 non consentito")

        market_id = self._safe_str(payload.get("market_id")).strip()
        if not market_id:
            raise PayloadValidationError("market_id vuoto")

        selection_id = payload.get("selection_id")
        if not isinstance(selection_id, int):
            raise PayloadValidationError("selection_id non int")

    def _validate_success_payload_common(self, payload: Dict[str, Any]):
        self._validate_common_order_rules(payload, allow_micro=True)

        matched = self._safe_float(payload.get("matched"), -1.0)
        if matched < 0:
            raise RiskInvariantError("matched < 0")

        status = self._safe_str(payload.get("status")).upper().strip()
        if status not in ("MATCHED", "PARTIALLY_MATCHED", "UNMATCHED", "DRY_RUN"):
            raise PayloadValidationError(f"status non ammesso: {status}")

    # =========================================================
    # MARKET SANITY
    # =========================================================

    def _validate_best_market_offer(
        self,
        ladder: Any,
        runner_idx: int,
        ladder_name: str,
        label: str,
    ) -> None:
        if not isinstance(ladder, (list, tuple)) or not ladder:
            raise MarketSanityError(f"runner[{runner_idx}].ex.{ladder_name} vuoto o invalido")

        best_offer = ladder[0]
        if not isinstance(best_offer, dict):
            raise MarketSanityError(f"runner[{runner_idx}].ex.{ladder_name}[0] non dict")

        if "price" not in best_offer:
            raise MarketSanityError(f"runner[{runner_idx}].ex.{ladder_name}[0].price missing")

        p = self._safe_float(best_offer.get("price"), 0.0)
        if p <= 1.0:
            raise MarketSanityError(f"runner[{runner_idx}] {label} <= 1.0")

    def validate_market_book(self, market_book: Dict[str, Any]) -> bool:
        if not isinstance(market_book, dict):
            raise MarketSanityError("market_book non dict")

        runners = market_book.get("runners")
        if not isinstance(runners, list) or not runners:
            raise MarketSanityError("market_book.runners vuoto o invalido")

        for idx, runner in enumerate(runners):
            if not isinstance(runner, dict):
                raise MarketSanityError(f"runner[{idx}] non dict")

            ex = runner.get("ex", {})
            if not isinstance(ex, dict):
                raise MarketSanityError(f"runner[{idx}].ex non dict")

            if "availableToBack" not in ex:
                raise MarketSanityError(f"runner[{idx}].ex.availableToBack missing")

            available_to_back = ex.get("availableToBack")
            available_to_lay = ex.get("availableToLay", []) or []

            self._validate_best_market_offer(
                available_to_back,
                idx,
                "availableToBack",
                "best back",
            )

            if available_to_lay:
                self._validate_best_market_offer(
                    available_to_lay,
                    idx,
                    "availableToLay",
                    "best lay",
                )

        return True

    def validate_selection_prices(
        self,
        back_price: Any,
        lay_price: Any,
        max_spread_ratio: float = 0.20,
    ) -> bool:
        bp = self._safe_float(back_price, 0.0)
        lp = self._safe_float(lay_price, 0.0)

        if bp <= 1.0 or lp <= 1.0:
            raise MarketSanityError("back/lay price invalidi")

        if lp < bp:
            raise MarketSanityError("lay < back")

        spread_ratio = (lp - bp) / bp if bp > 0 else 999.0
        if spread_ratio > max_spread_ratio:
            raise MarketSanityError(f"spread eccessivo: {spread_ratio:.4f}")

        return True

    # =========================================================
    # SAGA CHECKER
    # =========================================================

    def inspect_pending_sagas(
        self,
        db,
        stale_after_sec: float = 60.0,
    ) -> List[PendingSagaRecord]:
        rows: List[PendingSagaRecord] = []
        if db is None or not hasattr(db, "get_pending_sagas"):
            return rows

        try:
            raw_rows = db.get_pending_sagas() or []
        except Exception as e:
            logger.error("[SafetyLayer] inspect_pending_sagas DB error: %s", e)
            return rows

        for row in raw_rows:
            if not isinstance(row, dict):
                continue

            created_at = row.get("created_at")
            age_sec = self._safe_age_seconds(created_at, stale_after_sec)

            raw_payload = row.get("raw_payload")
            if raw_payload in (None, ""):
                raw_payload = row.get("payload_json")

            rows.append(
                PendingSagaRecord(
                    customer_ref=self._safe_str(row.get("customer_ref")),
                    market_id=self._safe_str(row.get("market_id")),
                    selection_id=self._safe_str(row.get("selection_id")) or None,
                    status=self._safe_str(row.get("status"), "PENDING"),
                    age_sec=age_sec,
                    raw_payload=self._safe_str(raw_payload) or None,
                )
            )

        return rows

    def get_stale_pending_sagas(
        self,
        db,
        stale_after_sec: float = 60.0,
    ) -> List[PendingSagaRecord]:
        rows = self.inspect_pending_sagas(db, stale_after_sec=stale_after_sec)
        return [r for r in rows if r.status == "PENDING" and r.age_sec >= stale_after_sec]

    # =========================================================
    # WATCHDOG
    # =========================================================

    def register_watchdog(
        self,
        name: str,
        timeout_sec: float = 5.0,
    ) -> None:
        wd_name = self._safe_str(name).strip()
        if not wd_name:
            raise SafetyLayerError("watchdog name vuoto")

        with self._lock:
            self._watchdogs[wd_name] = WatchdogState(
                name=wd_name,
                last_ping=self._clock(),
                timeout_sec=max(0.5, float(timeout_sec or 5.0)),
                enabled=True,
                triggered=False,
                last_error="",
            )

    def unregister_watchdog(self, name: str) -> None:
        wd_name = self._safe_str(name).strip()
        with self._lock:
            self._watchdogs.pop(wd_name, None)

    def watchdog_ping(self, name: str) -> None:
        wd_name = self._safe_str(name).strip()
        with self._lock:
            state = self._watchdogs.get(wd_name)
            if state:
                state.last_ping = self._clock()
                state.triggered = False
                state.last_error = ""

    def get_watchdog_status(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            now = self._clock()
            status: Dict[str, Dict[str, Any]] = {}
            for name, s in self._watchdogs.items():
                age = max(0.0, now - s.last_ping)
                status[name] = {
                    "timeout_sec": s.timeout_sec,
                    "last_ping_age_sec": age,
                    "enabled": s.enabled,
                    "triggered": s.triggered,
                    "last_error": s.last_error,
                }
            return status

    def set_watchdog_callback(self, callback: Optional[Callable[[str, str], None]]) -> None:
        self._watchdog_callback = callback

    def start_watchdog(self, interval_sec: float = 1.0) -> None:
        with self._lock:
            if self._watchdog_thread and self._watchdog_thread.is_alive():
                return

            self._watchdog_interval_sec = max(0.2, float(interval_sec or 1.0))
            self._watchdog_stop.clear()
            self._watchdog_thread = threading.Thread(
                target=self._watchdog_loop,
                daemon=True,
                name="SafetyLayerWatchdog",
            )
            self._watchdog_thread.start()

    def stop_watchdog(self) -> None:
        self._watchdog_stop.set()
        thread = self._watchdog_thread
        if thread and thread.is_alive():
            thread.join(timeout=3.0)

    def _watchdog_loop(self) -> None:
        while not self._watchdog_stop.is_set():
            try:
                self._run_watchdog_check()
            except Exception as e:
                logger.exception("[SafetyLayer] Watchdog loop error: %s", e)

            self._watchdog_stop.wait(self._watchdog_interval_sec)

    def _run_watchdog_check(self) -> None:
        callback = self._watchdog_callback
        to_notify: List[Tuple[str, str]] = []

        with self._lock:
            now = self._clock()
            for name, state in self._watchdogs.items():
                if not state.enabled:
                    continue

                age = now - state.last_ping
                if age > state.timeout_sec:
                    if not state.triggered:
                        state.triggered = True
                        state.last_error = (
                            f"Watchdog timeout: {age:.2f}s > {state.timeout_sec:.2f}s"
                        )
                        to_notify.append((name, state.last_error))

        for name, error in to_notify:
            logger.error("[SafetyLayer] %s -> %s", name, error)
            if callback:
                try:
                    callback(name, error)
                except Exception:
                    logger.exception("[SafetyLayer] watchdog callback error")

    def check_watchdogs(self) -> None:
        """Public wrapper per eseguire un check watchdog singolo in modo deterministico."""
        self._run_watchdog_check()

    # =========================================================
    # HELPERS READY-TO-USE
    # =========================================================

    def safe_validate_quick_bet_request(self, payload: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        try:
            self.validate_quick_bet_request(payload)
            return True, None
        except Exception as e:
            return False, str(e)

    def safe_validate_quick_bet_success(self, payload: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        try:
            self.validate_quick_bet_success(payload)
            return True, None
        except Exception as e:
            return False, str(e)

    def safe_validate_dutching_request(self, payload: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        try:
            self.validate_dutching_request(payload)
            return True, None
        except Exception as e:
            return False, str(e)

    def safe_validate_dutching_success(self, payload: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        try:
            self.validate_dutching_success(payload)
            return True, None
        except Exception as e:
            return False, str(e)

    def safe_validate_cashout_request(self, payload: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        try:
            self.validate_cashout_request(payload)
            return True, None
        except Exception as e:
            return False, str(e)

    def safe_validate_cashout_success(self, payload: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        try:
            self.validate_cashout_success(payload)
            return True, None
        except Exception as e:
            return False, str(e)


# =========================================================
# SINGLETON
# =========================================================

_global_safety_layer: Optional[SafetyLayer] = None
_global_safety_layer_lock = threading.Lock()


def get_safety_layer() -> SafetyLayer:
    global _global_safety_layer
    if _global_safety_layer is None:
        with _global_safety_layer_lock:
            if _global_safety_layer is None:
                _global_safety_layer = SafetyLayer()
    return _global_safety_layer
