from __future__ import annotations

import logging
import math
import time
from datetime import datetime
from typing import Any, Optional

from core.duplication_guard import DuplicationGuard
from core.dutching_batch_manager import DutchingBatchManager
from core.market_tracker import MarketTracker
from core.money_management import RoserpinaMoneyManagement
from core.reconciliation_engine import ReconciliationEngine
from core.risk_desk import RiskDesk
from core.system_state import DeskMode, RuntimeMode
from core.table_manager import TableManager
from core.safety_layer import assert_live_gate_or_refuse
from core.type_helpers import safe_bool
from order_manager import TERMINAL_LIFECYCLE_EVENTS
from services.streaming_feed import StreamingConfigError, StreamingFeed
from trading_config import STRICT_LIVE_KEY_SOURCE_REQUIRED, enforce_betfair_italy_commission_pct

logger = logging.getLogger(__name__)


class RuntimeController:
    """
    Runtime controller centrale.

    Responsabilità:
    - start/stop/pause/resume runtime
    - sincronizzazione simulation/live mode
    - routing dei segnali verso CMD_QUICK_BET
    - coordinamento Roserpina / tavoli / anti-duplicazione
    - snapshot stato runtime

    Nota:
    - NON chiude automaticamente le posizioni
    - la chiusura resta manuale/comando
    """

    def __init__(
        self,
        *,
        bus,
        db,
        settings_service,
        betfair_service,
        telegram_service,
        trading_engine=None,
        executor=None,
        safe_mode=None,
    ):
        self.bus = bus
        self.db = db
        self.settings_service = settings_service
        self.betfair_service = betfair_service
        self.telegram_service = telegram_service
        self.trading_engine = trading_engine
        self.executor = executor
        self.safe_mode = safe_mode

        self.config = self.settings_service.load_roserpina_config()
        self.table_manager = TableManager(table_count=self.config.table_count)
        self.duplication_guard = DuplicationGuard()
        self.risk_desk = RiskDesk()
        self.mm = RoserpinaMoneyManagement(self.config)

        self.batch_manager = DutchingBatchManager(db, bus=bus)
        self.reconciliation_engine = self._build_reconciliation_engine()

        self.market_tracker = MarketTracker(
            bus=self.bus,
            betfair_service=self.betfair_service,
        )
        self.streaming_feed: Optional[StreamingFeed] = None
        self._market_data_cfg: dict[str, Any] = {}
        self._last_fallback_snapshot_at: float = 0.0

        self.mode = RuntimeMode.STOPPED
        self.last_error = ""
        self.last_signal_at = ""
        self.simulation_mode = False
        self.execution_mode = "SIMULATION"
        self.live_enabled = False
        self.live_readiness_ok = False
        self.last_execution_gate_reason = "startup_default"
        self.enforce_probe_readiness_gate = False
        self.last_deploy_gate_status = {
            "allowed": False,
            "reason": "DEPLOY_BLOCKED_INVALID_STATE",
            "reasons": ["DEPLOY_BLOCKED_INVALID_STATE"],
            "readiness": "UNKNOWN",
            "details": {},
        }

        # Emergency stop state — set by emergency_stop(), cleared only by reset_emergency()
        self._emergency_stopped: bool = False
        self._emergency_stopped_at: str = ""
        self._emergency_reason: str = ""
        self._io_observations: dict[str, Any] = {
            "last_operation": "",
            "last_status": "UNKNOWN",
            "last_latency_ms": 0.0,
            "slow_count": 0,
            "degraded_count": 0,
            "unavailable_count": 0,
            "total_count": 0,
            "last_error": "",
        }
        self._processed_bankroll_sync_keys: set[str] = set()
        self._processed_realized_pnl_keys: set[str] = set()
        self._processed_auto_trade_keys: set[str] = set()
        self._cycle_step_counts: dict[str, int] = {}
        self._last_bankroll_sync_result: dict[str, Any] = {
            "correlation_id": "",
            "settlement_detected": False,
            "bankroll_before": float(self.risk_desk.bankroll_current),
            "bankroll_after": float(self.risk_desk.bankroll_current),
            "bankroll_sync_status": "NOT_SETTLED",
            "balance_source": "none",
            "reason": "sync_not_triggered",
        }
        self._last_auto_trade_result: dict[str, Any] = {
            "correlation_id": "",
            "source_settlement_correlation_id": "",
            "bankroll_sync_status": "NOT_SETTLED",
            "money_management_status": "MM_STOP_CONTEXT_MISSING",
            "cycle_active": False,
            "progression_allowed": False,
            "auto_trade_enabled": False,
            "auto_trade_status": "AUTO_TRADE_NOT_ELIGIBLE",
            "next_stake": 0.0,
            "risk_status": "RISK_NOT_EVALUATED",
            "submitted": False,
            "reason": "auto_trade_not_triggered",
        }
        self._last_cycle_executor_result: dict[str, Any] = {
            "correlation_id": "",
            "source_settlement_correlation_id": "",
            "cycle_executor_enabled": False,
            "cycle_active": False,
            "progression_allowed": False,
            "bankroll_sync_status": "NOT_SETTLED",
            "money_management_status": "MM_STOP_CONTEXT_MISSING",
            "next_stake": 0.0,
            "cycle_step_index": 0,
            "max_steps_reached": False,
            "kill_switch_active": False,
            "anomaly_pause_active": False,
            "auto_trade_enabled": False,
            "auto_trade_status": "AUTO_TRADE_NOT_ELIGIBLE",
            "cycle_executor_status": "CYCLE_EXECUTOR_DISABLED",
            "risk_status": "RISK_NOT_EVALUATED",
            "submitted": False,
            "reason": "cycle_executor_not_triggered",
            "recovery_status": "RECOVERY_NO_STATE",
        }

        self._subscribe_bus()

    def _record_runtime_io(self, *, operation: str, started_at: float, ok: bool, error: str = "") -> None:
        elapsed_ms = max(0.0, (time.monotonic() - started_at) * 1000.0)
        slow_threshold_ms = 2000.0
        if ok and elapsed_ms >= slow_threshold_ms:
            status = "SLOW"
        elif ok:
            status = "SUCCESS"
        elif error:
            status = "DEGRADED"
        else:
            status = "UNAVAILABLE"
        self._io_observations["last_operation"] = str(operation)
        self._io_observations["last_status"] = status
        self._io_observations["last_latency_ms"] = round(elapsed_ms, 3)
        self._io_observations["last_error"] = str(error or "")
        self._io_observations["total_count"] = int(self._io_observations.get("total_count", 0) or 0) + 1
        if status == "SLOW":
            self._io_observations["slow_count"] = int(self._io_observations.get("slow_count", 0) or 0) + 1
        if status == "DEGRADED":
            self._io_observations["degraded_count"] = int(self._io_observations.get("degraded_count", 0) or 0) + 1
        if status == "UNAVAILABLE":
            self._io_observations["unavailable_count"] = int(self._io_observations.get("unavailable_count", 0) or 0) + 1

    @staticmethod
    def _extract_origin_metadata(signal: dict) -> tuple[dict | None, dict | None]:
        """
        Runtime boundary metadata extractor.

        RuntimeController acts as a passthrough boundary:
        - preserves structurally valid copy/pattern metadata
        - does not own allow-list normalization (TradingEngine remains authoritative)
        """
        copy_meta = signal.get("copy_meta")
        pattern_meta = signal.get("pattern_meta")
        copy_dict = dict(copy_meta) if isinstance(copy_meta, dict) else None
        pattern_dict = dict(pattern_meta) if isinstance(pattern_meta, dict) else None
        return copy_dict, pattern_dict

    @staticmethod
    def _resolve_order_origin(signal: dict, *, has_copy_meta: bool, has_pattern_meta: bool) -> str:
        """
        Resolve boundary origin with minimal inference.
        TradingEngine remains the final normalization authority.
        """
        explicit = str(signal.get("order_origin") or "").strip()
        if explicit:
            return explicit
        if has_copy_meta:
            return "COPY"
        if has_pattern_meta:
            return "PATTERN"
        return ""

    def _load_market_data_config(self) -> dict:
        if hasattr(self.settings_service, "load_market_data_config"):
            try:
                cfg = self.settings_service.load_market_data_config() or {}
                if isinstance(cfg, dict):
                    return cfg
            except Exception:
                logger.exception("Errore load_market_data_config")
        return {
            "market_data_mode": "poll",
            "enabled": False,
            "snapshot_fallback_enabled": True,
            "snapshot_fallback_interval_sec": 5,
            "market_ids": [],
        }

    def _stream_market_book_callback(self, market_book: dict) -> None:
        self.market_tracker.on_market_book(dict(market_book or {}))

    def _stream_disconnect_callback(self, payload: dict) -> None:
        logger.warning("Streaming disconnected -> fallback snapshot payload=%s", payload)
        self._snapshot_rest_fallback(reason="stream_disconnect", payload=payload)

    def _start_market_data_feed(self) -> None:
        self._market_data_cfg = self._load_market_data_config()
        mode = str(self._market_data_cfg.get("market_data_mode", "poll") or "poll").strip().lower()
        enabled = bool(self._market_data_cfg.get("enabled", False))
        if self.simulation_mode:
            return
        if mode not in {"stream", "hybrid"} or not enabled:
            return

        self.streaming_feed = StreamingFeed(
            client_getter=self.betfair_service.get_live_client,
            config=self._market_data_cfg,
            on_market_book=self._stream_market_book_callback,
            on_disconnect=self._stream_disconnect_callback,
            session_gate=self.betfair_service.ensure_stream_session_ready,
        )
        self.streaming_feed.start()

    def _stop_market_data_feed(self) -> None:
        if self.streaming_feed is None:
            return
        try:
            self.streaming_feed.stop()
        except Exception:
            logger.exception("Errore stop streaming_feed")
        finally:
            self.streaming_feed = None

    def _snapshot_rest_fallback(self, *, reason: str, payload: Optional[dict] = None) -> None:
        cfg = dict(self._market_data_cfg or self._load_market_data_config())
        if not bool(cfg.get("snapshot_fallback_enabled", True)):
            return

        now = time.monotonic()
        min_interval = max(1.0, float(cfg.get("snapshot_fallback_interval_sec", 5) or 5))
        if (now - self._last_fallback_snapshot_at) < min_interval:
            return

        market_ids = [str(mid).strip() for mid in (cfg.get("market_ids") or []) if str(mid).strip()]
        if not market_ids:
            return

        for market_id in market_ids:
            book = self.betfair_service.get_market_book_snapshot(market_id)
            if isinstance(book, dict) and book:
                self.market_tracker.on_market_book(book)
        self._last_fallback_snapshot_at = now
        self.bus.publish(
            "MARKET_DATA_FALLBACK_SNAPSHOT",
            {
                "reason": reason,
                "market_count": len(market_ids),
                "ts": datetime.utcnow().isoformat(),
                "stream_payload": dict(payload or {}),
            },
        )

    def runtime_io_snapshot(self) -> dict:
        return dict(self._io_observations)

    # =========================================================
    # INTERNAL BUILDERS
    # =========================================================
    def _build_reconciliation_engine(self) -> ReconciliationEngine:
        return ReconciliationEngine(
            db=self.db,
            bus=self.bus,
            batch_manager=self.batch_manager,
            betfair_service=self.betfair_service,
            table_manager=self.table_manager,
            duplication_guard=self.duplication_guard,
        )

    def _subscribe_bus(self) -> None:
        self.bus.subscribe("SIGNAL_RECEIVED", self._on_signal_received)
        self.bus.subscribe("QUICK_BET_FAILED", self._on_quick_bet_failed)
        self.bus.subscribe("QUICK_BET_ACCEPTED", self._on_quick_bet_accepted)
        self.bus.subscribe("QUICK_BET_PARTIAL", self._on_quick_bet_partial)
        self.bus.subscribe("QUICK_BET_FILLED", self._on_quick_bet_filled)
        self.bus.subscribe("QUICK_BET_ROLLBACK_DONE", self._on_quick_bet_rollback_done)
        self.bus.subscribe("QUICK_BET_SUCCESS", self._on_quick_bet_success)
        self.bus.subscribe("QUICK_BET_AMBIGUOUS", self._on_quick_bet_ambiguous)
        self.bus.subscribe("RUNTIME_CLOSE_POSITION", self._on_close_position)

    # =========================================================
    # CONFIG / MODE
    # =========================================================
    def set_simulation_mode(self, enabled: bool) -> None:
        """
        Metodo richiesto da mini_gui.py.
        """
        self.simulation_mode = bool(enabled)
        if hasattr(self.betfair_service, "set_simulation_mode"):
            self.betfair_service.set_simulation_mode(self.simulation_mode)

    def _safe_bool(self, value, default: bool = False) -> bool:
        return safe_bool(value, default)

    def _safe_execution_mode(self, value) -> str:
        normalized = str(value or "").strip().upper()
        if normalized in {"SIMULATION", "LIVE"}:
            return normalized
        return "SIMULATION"

    def _is_kill_switch_active(self) -> bool:
        safe_mode = self.safe_mode
        if safe_mode is None:
            return False

        getter = getattr(safe_mode, "is_enabled", None)
        if callable(getter):
            try:
                return bool(getter())
            except Exception:
                return True

        attr_enabled = getattr(safe_mode, "enabled", None)
        if attr_enabled is not None:
            try:
                return bool(attr_enabled)
            except Exception:
                return True

        attr_active = getattr(safe_mode, "is_safe_mode_active", None)
        if attr_active is not None:
            try:
                return bool(attr_active)
            except Exception:
                return True

        return False

    def _derive_live_readiness_ok(self, explicit_readiness=None) -> bool:
        if explicit_readiness is not None:
            return self._safe_bool(explicit_readiness, default=False)

        if hasattr(self.settings_service, "load_live_readiness_ok"):
            try:
                return self._safe_bool(self.settings_service.load_live_readiness_ok(), default=False)
            except Exception:
                return False

        return False

    def _derive_strict_live_key_source_required(self) -> bool:
        loader = getattr(self.settings_service, "load_strict_live_key_source_required", None)
        if callable(loader):
            try:
                return self._safe_bool(loader(), default=STRICT_LIVE_KEY_SOURCE_REQUIRED)
            except Exception:
                return bool(STRICT_LIVE_KEY_SOURCE_REQUIRED)
        return bool(STRICT_LIVE_KEY_SOURCE_REQUIRED)

    def _resolve_secret_key_source(self) -> str:
        getter = getattr(self.db, "get_secret_key_source", None)
        if callable(getter):
            try:
                src = str(getter() or "").strip().lower()
                return src or "unknown"
            except Exception:
                return "unknown"

        cipher = getattr(self.db, "_cipher", None)
        key_source = getattr(cipher, "key_source", None)
        src = str(key_source or "").strip().lower()
        return src or "unknown"

    def _validate_live_hard_stop_config(self) -> dict[str, Any]:
        required_fields = (
            "max_daily_loss",
            "max_drawdown_hard_stop_pct",
            "max_open_exposure",
        )
        missing_fields: list[str] = []
        invalid_fields: list[str] = []
        values: dict[str, float | None] = {}
        config = getattr(self, "config", None)

        for field_name in required_fields:
            if config is None or not hasattr(config, field_name):
                missing_fields.append(field_name)
                values[field_name] = None
                continue

            raw = getattr(config, field_name)
            if raw is None:
                missing_fields.append(field_name)
                values[field_name] = None
                continue

            try:
                parsed = float(raw)
            except Exception:
                invalid_fields.append(field_name)
                values[field_name] = None
                continue

            values[field_name] = parsed
            if not math.isfinite(parsed):
                invalid_fields.append(field_name)
                continue
            if parsed <= 0.0:
                invalid_fields.append(field_name)
                continue
            if field_name == "max_drawdown_hard_stop_pct" and parsed > 100.0:
                invalid_fields.append(field_name)

        return {
            "required_fields": list(required_fields),
            "missing_fields": missing_fields,
            "invalid_fields": invalid_fields,
            "values": values,
            "valid": not missing_fields and not invalid_fields,
        }

    def _get_probe_live_readiness_report(self) -> tuple[bool, str, dict]:
        probe = getattr(self, "runtime_probe", None)
        probe_required = bool(getattr(self, "enforce_probe_readiness_gate", False))
        if probe is None:
            if probe_required:
                return False, "probe_unavailable", {}
            return True, "probe_optional_unavailable", {}

        getter = getattr(probe, "get_live_readiness_report", None)
        if not callable(getter):
            if probe_required:
                return False, "probe_report_getter_missing", {}
            return True, "probe_optional_getter_missing", {}

        try:
            report = getter()
        except Exception:
            logger.exception("Errore lettura runtime probe live readiness report")
            return False, "probe_report_exception", {}

        is_valid, reason = self._validate_probe_live_readiness_report(report)
        if not is_valid:
            return False, reason, (report if isinstance(report, dict) else {})
        return True, "probe_ready", report

    def _validate_probe_live_readiness_report(self, report: Any) -> tuple[bool, str]:
        if not isinstance(report, dict):
            return False, "probe_report_malformed"

        if "ready" not in report or "level" not in report or "blockers" not in report:
            return False, "probe_report_missing_required_fields"

        ready = report.get("ready")
        level = str(report.get("level") or "").strip().upper()
        blockers = report.get("blockers")

        if not isinstance(ready, bool):
            return False, "probe_report_ready_not_bool"

        if not isinstance(blockers, list):
            return False, "probe_report_blockers_not_list"

        if level != "READY":
            return False, "probe_report_level_not_ready"

        if blockers:
            return False, "probe_report_has_blockers"

        if not ready:
            return False, "probe_report_ready_false"

        return True, "probe_report_ready"

    def _coerce_readiness_level(self, probe_report: Any) -> str:
        if not isinstance(probe_report, dict):
            return "UNKNOWN"
        level = str(probe_report.get("level") or "").strip().upper()
        return level if level in {"READY", "DEGRADED", "NOT_READY", "UNKNOWN"} else "UNKNOWN"

    def _collect_deploy_gate_reasons(self, gate, readiness_level: str, readiness_payload: dict) -> list[str]:
        reasons: list[str] = []
        blockers = list(readiness_payload.get("blockers") or [])
        probe_report = (
            ((readiness_payload.get("details") or {}).get("probe") or {}).get("report")
            if isinstance(readiness_payload, dict)
            else {}
        )
        if isinstance(probe_report, dict):
            blockers.extend(list(probe_report.get("blockers") or []))
        probe_ok = bool(readiness_payload.get("probe_ok", False))

        if not gate.allowed and str(gate.reason_code) == "kill_switch_active":
            reasons.append("DEPLOY_BLOCKED_KILL_SWITCH")

        if blockers:
            reasons.append("DEPLOY_BLOCKED_BLOCKERS_PRESENT")

        if readiness_level != "READY" or not probe_ok or not bool(readiness_payload.get("ready", False)):
            reasons.append("DEPLOY_BLOCKED_NOT_READY")

        if not reasons and not gate.allowed:
            reasons.append("DEPLOY_BLOCKED_INVALID_STATE")

        if gate.allowed:
            return ["DEPLOY_GO_READY"]

        # dedupe preserving order
        uniq: list[str] = []
        for item in reasons:
            if item not in uniq:
                uniq.append(item)
        return uniq or ["DEPLOY_BLOCKED_INVALID_STATE"]

    def get_deploy_gate_status(
        self,
        *,
        execution_mode: Optional[str] = None,
        live_enabled: Optional[bool] = None,
        live_readiness_ok: Optional[bool] = None,
    ) -> dict:
        mode = str(execution_mode if execution_mode is not None else self.execution_mode or "SIMULATION").strip().upper()
        mode = mode if mode in {"SIMULATION", "LIVE"} else "SIMULATION"
        enabled = self._safe_bool(self.live_enabled if live_enabled is None else live_enabled, default=False)
        readiness = self.evaluate_live_readiness(
            execution_mode=mode,
            live_enabled=enabled,
            live_readiness_ok=live_readiness_ok,
        )
        probe_ok = True
        probe_reason = "probe_not_required_for_non_live"
        probe_report = {}

        if mode == "LIVE":
            probe_ok, probe_reason, probe_report = self._get_probe_live_readiness_report()
            if not probe_ok:
                readiness["ready"] = False

        readiness.setdefault("details", {})
        readiness["details"]["probe"] = {
            "ok": probe_ok,
            "reason": probe_reason,
            "report": probe_report,
        }
        readiness["probe_ok"] = probe_ok

        readiness_level = self._coerce_readiness_level(probe_report)
        if mode != "LIVE":
            readiness_level = "READY"
        elif probe_ok and readiness_level == "UNKNOWN":
            readiness_level = "READY"
        gate = assert_live_gate_or_refuse(
            execution_mode=mode,
            live_enabled=enabled,
            live_readiness_ok=(bool(readiness.get("ready", False)) and readiness_level == "READY"),
            kill_switch=self._is_kill_switch_active(),
        )
        reasons = self._collect_deploy_gate_reasons(gate, readiness_level, readiness)
        return {
            "allowed": bool(gate.allowed),
            "reason": reasons[0],
            "reasons": reasons,
            "execution_mode": mode,
            "effective_execution_mode": gate.effective_execution_mode,
            "readiness": readiness_level,
            "details": {
                "gate_reason_code": gate.reason_code,
                "readiness_payload": readiness,
            },
        }

    def is_deploy_allowed(self, **kwargs) -> bool:
        return bool(self.get_deploy_gate_status(**kwargs).get("allowed", False))

    def enforce_deploy_gate(self, **kwargs) -> dict:
        status = self.get_deploy_gate_status(**kwargs)
        self.last_deploy_gate_status = status
        gate_reason_code = str(((status.get("details") or {}).get("gate_reason_code")) or "")
        self.last_execution_gate_reason = gate_reason_code or str(status.get("reason") or "DEPLOY_BLOCKED_INVALID_STATE")

        if status["allowed"]:
            logger.info("[DEPLOY GATE] GO: readiness=READY")
        else:
            logger.warning("[DEPLOY GATE] NO-GO: reason=%s", ",".join(status.get("reasons") or [status.get("reason", "")]))

        return status

    def evaluate_live_readiness(
        self,
        *,
        execution_mode: Optional[str] = None,
        live_enabled: Optional[bool] = None,
        live_readiness_ok: Optional[bool] = None,
    ) -> dict:
        blockers = []
        details = {}

        runtime_mode_value = getattr(getattr(self, "mode", None), "value", None)
        known_runtime_modes = {item.value for item in RuntimeMode}
        runtime_mode_known = runtime_mode_value in known_runtime_modes
        runtime_initialized = all(
            (
                getattr(self, "config", None) is not None,
                getattr(self, "table_manager", None) is not None,
                getattr(self, "duplication_guard", None) is not None,
                getattr(self, "risk_desk", None) is not None,
                getattr(self, "reconciliation_engine", None) is not None,
            )
        )
        runtime_half_started = False
        if runtime_mode_value == RuntimeMode.ACTIVE.value:
            try:
                runtime_half_started = not (
                    bool(self.betfair_service.status().get("connected"))
                    and bool(self.telegram_service.status().get("connected"))
                )
            except Exception:
                runtime_half_started = True
        startup_failed = bool(getattr(self, "last_error", ""))

        details["runtime_state"] = {
            "mode": runtime_mode_value,
            "mode_known": runtime_mode_known,
            "initialized": runtime_initialized,
            "half_started": runtime_half_started,
            "startup_failed": startup_failed,
        }

        if not runtime_mode_known:
            blockers.append("READINESS_SIGNAL_UNKNOWN")
        if not runtime_initialized:
            blockers.append("RUNTIME_NOT_INITIALIZED")
        if runtime_half_started:
            blockers.append("RUNTIME_HALF_STARTED")
        if startup_failed and runtime_mode_value != RuntimeMode.ACTIVE.value:
            blockers.append("STARTUP_FAILED")

        kill_switch_active = bool(self._is_kill_switch_active())
        safe_mode_blocks_live = kill_switch_active
        details["safety_state"] = {
            "kill_switch_active": kill_switch_active,
            "safe_mode_blocks_live": safe_mode_blocks_live,
        }
        if kill_switch_active:
            blockers.append("KILL_SWITCH_ACTIVE")
        if safe_mode_blocks_live:
            blockers.append("SAFE_MODE_BLOCKING")

        has_live_dependency = bool(
            getattr(self, "betfair_service", None) is not None
            and callable(getattr(self.betfair_service, "connect", None))
        )
        details["live_dependency_state"] = {
            "betfair_service_present": getattr(self, "betfair_service", None) is not None,
            "betfair_connect_callable": callable(getattr(getattr(self, "betfair_service", None), "connect", None)),
            "has_required_live_dependency": has_live_dependency,
        }
        if not has_live_dependency:
            blockers.append("LIVE_DEPENDENCY_MISSING")

        normalized_execution_mode = str(execution_mode if execution_mode is not None else self.execution_mode).strip().upper()
        effective_live_enabled = self._safe_bool(
            self.live_enabled if live_enabled is None else live_enabled,
            default=False,
        )
        configured_live_readiness_ok = self._derive_live_readiness_ok(live_readiness_ok)
        strict_live_key_source_required = self._derive_strict_live_key_source_required()
        key_source = self._resolve_secret_key_source()
        allowed_key_sources = {"env", "file_existing", "file_generated"}
        key_source_passed = key_source in allowed_key_sources
        execution_mode_valid = normalized_execution_mode in {"SIMULATION", "LIVE"}
        contradictory_state = (
            (normalized_execution_mode == "LIVE" and bool(getattr(self, "simulation_mode", False)))
            or (normalized_execution_mode == "LIVE" and not effective_live_enabled)
        )
        details["execution_state"] = {
            "execution_mode": normalized_execution_mode,
            "execution_mode_valid": execution_mode_valid,
            "live_enabled": effective_live_enabled,
            "configured_live_readiness_ok": configured_live_readiness_ok,
            "simulation_mode": bool(getattr(self, "simulation_mode", False)),
            "contradictory_state": contradictory_state,
        }
        details["key_source_state"] = {
            "key_source": key_source,
            "strict_live_key_source_required": strict_live_key_source_required,
            "passed": (not strict_live_key_source_required) or key_source_passed,
            "allowed_sources": sorted(allowed_key_sources),
        }
        hard_stop_config_state = self._validate_live_hard_stop_config()
        details["hard_stop_config_state"] = hard_stop_config_state

        if not execution_mode_valid:
            blockers.append("INVALID_EXECUTION_MODE")
        if normalized_execution_mode == "LIVE" and not effective_live_enabled:
            blockers.append("LIVE_NOT_ENABLED")
        if normalized_execution_mode == "LIVE" and not configured_live_readiness_ok:
            blockers.append("LIVE_READINESS_FLAG_NOT_OK")
        if normalized_execution_mode == "LIVE" and strict_live_key_source_required and not key_source_passed:
            blockers.append("LIVE_KEY_SOURCE_UNSAFE")
        if normalized_execution_mode == "LIVE" and hard_stop_config_state["missing_fields"]:
            blockers.append("LIVE_HARD_STOP_CONFIG_MISSING")
        if normalized_execution_mode == "LIVE" and hard_stop_config_state["invalid_fields"]:
            blockers.append("LIVE_HARD_STOP_CONFIG_INVALID")
        if contradictory_state:
            blockers.append("CONTRADICTORY_STATE")

        is_live_request = normalized_execution_mode == "LIVE"
        unique_blockers = sorted(set(blockers))
        ready = is_live_request and not unique_blockers
        level = "READY" if ready else ("DEGRADED" if (not is_live_request and execution_mode_valid) else "NOT_READY")
        return {
            "ready": ready,
            "level": level,
            "blockers": unique_blockers,
            "details": details,
        }

    def is_live_readiness_ok(self, **kwargs) -> bool:
        return bool(self.evaluate_live_readiness(**kwargs).get("ready", False))

    def is_live_allowed(self) -> bool:
        if self._is_kill_switch_active():
            return False

        if self._safe_execution_mode(self.execution_mode) != "LIVE":
            return False

        if not self._safe_bool(self.live_enabled, default=False):
            return False

        try:
            deploy_status = self.get_deploy_gate_status(
                execution_mode="LIVE",
                live_enabled=self.live_enabled,
                live_readiness_ok=self.live_readiness_ok,
            )
        except Exception:
            return False

        if not isinstance(deploy_status, dict):
            return False

        if deploy_status.get("allowed") is not True:
            return False

        readiness_level = str(deploy_status.get("readiness") or "UNKNOWN").upper()

        if readiness_level in {"UNKNOWN", "NOT_READY"}:
            return False

        return True

    def get_effective_execution_mode(self) -> str:
        if not self.is_live_allowed():
            return "SIMULATION"
        return "LIVE"

    def reload_config(self) -> None:
        self.config = self.settings_service.load_roserpina_config()
        self.mm = RoserpinaMoneyManagement(self.config)
        self.table_manager = TableManager(table_count=self.config.table_count)
        self.reconciliation_engine = self._build_reconciliation_engine()

    def _desk_mode(self) -> DeskMode:
        return self.mm.determine_desk_mode(
            bankroll_current=self.risk_desk.bankroll_current,
            equity_peak=self.risk_desk.equity_peak,
        )

    def _runtime_active(self) -> bool:
        return self.mode == RuntimeMode.ACTIVE

    def force_lockdown(self, reason: str = "") -> dict:
        self.mode = RuntimeMode.LOCKDOWN
        self.last_error = reason or "LOCKDOWN"
        status = self.get_status()
        self.bus.publish("RUNTIME_LOCKDOWN", status)
        return {
            "locked": True,
            "status": status,
        }

    # =========================================================
    # EMERGENCY STOP
    # =========================================================
    @property
    def is_emergency_stopped(self) -> bool:
        return self._emergency_stopped

    def emergency_stop(self, reason: str = "") -> dict:
        """
        Global emergency stop.

        1. Sets _emergency_stopped flag — all live order entry refused immediately.
        2. Disables live_enabled.
        3. Forces LOCKDOWN runtime mode.
        4. Attempts cancel-all open/pending orders via live Betfair client.
        5. Emits EMERGENCY_STOP_TRIGGERED event with full detail.

        Errors in downstream cancellation do NOT silently allow normal trading —
        the runtime stays LOCKED regardless of cancel outcome.

        Returns a structured result dict with cancel outcomes.
        To resume trading after an emergency stop you MUST call reset_emergency()
        first, then start() again.
        """
        triggered_at = datetime.utcnow().isoformat()
        self._emergency_stopped = True
        self._emergency_stopped_at = triggered_at
        self._emergency_reason = reason or "EMERGENCY_STOP"

        # Hard-close live gate
        self.live_enabled = False
        self.execution_mode = "SIMULATION"
        self.set_simulation_mode(True)

        # Force LOCKDOWN
        self.force_lockdown(self._emergency_reason)

        # Attempt cancel-all open/pending orders
        cancel_results: list = []
        cancel_errors: list = []
        cancelled_count = 0
        error_count = 0

        try:
            pending = self.db.get_pending_sagas() if hasattr(self.db, "get_pending_sagas") else []
        except Exception as exc:
            logger.exception("emergency_stop: get_pending_sagas failed")
            pending = []
            cancel_errors.append({"stage": "get_pending_sagas", "error": str(exc)})

        # Group by market_id for efficient batch cancel
        by_market: dict = {}
        for saga in pending:
            mid = str(saga.get("market_id") or "").strip()
            bet_id = str(saga.get("bet_id") or "").strip()
            customer_ref = str(saga.get("customer_ref") or "").strip()
            if mid:
                by_market.setdefault(mid, []).append({
                    "bet_id": bet_id,
                    "customer_ref": customer_ref,
                })

        live_client = None
        try:
            live_client = self.betfair_service.get_live_client()
        except Exception as exc:
            logger.warning("emergency_stop: cannot get live client: %s", exc)
            cancel_errors.append({"stage": "get_live_client", "error": str(exc)})

        if live_client is not None and by_market:
            for market_id, orders in by_market.items():
                try:
                    response = live_client.cancel_orders(
                        market_id=market_id,
                        # bet_ids omitted → cancel ALL unmatched orders on market
                    )
                    if response.get("ok"):
                        cancel_results.append({
                            "market_id": market_id,
                            "order_count": len(orders),
                            "ok": True,
                            "response": response,
                        })
                        cancelled_count += response.get("cancelled_count", len(orders))
                    else:
                        err_msg = response.get("error", "cancel_orders returned ok=False")
                        logger.warning(
                            "emergency_stop: cancel_orders ok=False for market %s: %s",
                            market_id,
                            err_msg,
                        )
                        cancel_results.append({
                            "market_id": market_id,
                            "order_count": len(orders),
                            "ok": False,
                            "error": err_msg,
                        })
                        error_count += len(orders)
                except Exception as exc:
                    logger.warning(
                        "emergency_stop: cancel_orders failed for market %s: %s",
                        market_id,
                        exc,
                    )
                    cancel_results.append({
                        "market_id": market_id,
                        "order_count": len(orders),
                        "ok": False,
                        "error": str(exc),
                    })
                    error_count += len(orders)
        elif not by_market:
            logger.info("emergency_stop: no open/pending orders to cancel")

        result = {
            "emergency_stopped": True,
            "triggered_at": triggered_at,
            "reason": self._emergency_reason,
            "pending_count": len(pending),
            "markets_attempted": len(by_market),
            "cancelled_count": cancelled_count,
            "cancel_error_count": error_count,
            "cancel_results": cancel_results,
            "cancel_errors": cancel_errors,
            "live_client_available": live_client is not None,
        }

        # Emit observable structured event
        self.bus.publish("EMERGENCY_STOP_TRIGGERED", result)

        logger.critical(
            "EMERGENCY STOP TRIGGERED at=%s reason=%r markets=%d cancelled=%d errors=%d",
            triggered_at,
            self._emergency_reason,
            len(by_market),
            cancelled_count,
            error_count,
        )

        return result

    def reset_emergency(self) -> dict:
        """
        Clear the emergency-stopped flag so the runtime can be restarted.
        Does NOT restart the runtime — call start() after this.
        """
        self._emergency_stopped = False
        self._emergency_stopped_at = ""
        self._emergency_reason = ""
        self.bus.publish("EMERGENCY_STOP_RESET", {"reset_at": datetime.utcnow().isoformat()})
        return {"emergency_reset": True}

    # =========================================================
    # LIFECYCLE
    # =========================================================
    def start(
        self,
        password: Optional[str] = None,
        simulation_mode: Optional[bool] = None,
        execution_mode: Optional[str] = None,
        live_enabled: Optional[bool] = None,
        live_readiness_ok: Optional[bool] = None,
    ) -> dict:
        self.reload_config()

        requested_execution_mode = self._safe_execution_mode(execution_mode)
        if execution_mode is None and simulation_mode is not None:
            requested_execution_mode = "SIMULATION" if bool(simulation_mode) else "LIVE"

        requested_live_enabled = False
        if live_enabled is not None:
            requested_live_enabled = self._safe_bool(live_enabled, default=False)
        else:
            try:
                if hasattr(self.settings_service, "load_live_enabled"):
                    requested_live_enabled = self._safe_bool(
                        self.settings_service.load_live_enabled(),
                        default=False,
                    )
                else:
                    data = self.settings_service.get_all_settings()
                    requested_live_enabled = (
                        str(data.get("execution_mode", "SIMULATION")).strip().upper() == "LIVE"
                        or self._safe_bool(data.get("live_enabled"), default=False)
                    )
            except Exception:
                requested_live_enabled = False

        deploy_gate = self.enforce_deploy_gate(
            execution_mode=requested_execution_mode,
            live_enabled=requested_live_enabled,
            live_readiness_ok=live_readiness_ok,
        )
        readiness = dict((deploy_gate.get("details") or {}).get("readiness_payload") or {})

        self.execution_mode = str(deploy_gate.get("effective_execution_mode") or "SIMULATION")
        self.live_enabled = requested_live_enabled
        self.live_readiness_ok = bool(readiness.get("ready", False))

        if requested_execution_mode == "LIVE" and not deploy_gate["allowed"]:
            status = self.get_status()
            self.bus.publish(
                "LIVE_EXECUTION_REFUSED",
                {
                    "reason_code": (deploy_gate.get("details") or {}).get("gate_reason_code", deploy_gate["reason"]),
                    "deploy_gate_reason_code": deploy_gate["reason"],
                    "message": "LIVE richiesto ma deploy gate NO-GO",
                    "requested_execution_mode": requested_execution_mode,
                    "effective_execution_mode": self.execution_mode,
                    "deploy_gate": deploy_gate,
                    "readiness": readiness,
                },
            )
            return {
                "ok": False,
                "started": False,
                "refused": True,
                "reason": "deploy_gate_no_go",
                "reason_code": (deploy_gate.get("details") or {}).get("gate_reason_code", deploy_gate["reason"]),
                "deploy_gate_reason_code": deploy_gate["reason"],
                "refusal_message": "LIVE richiesto ma deploy gate NO-GO",
                "requested_execution_mode": requested_execution_mode,
                "effective_execution_mode": self.execution_mode,
                "deploy_gate": deploy_gate,
                "readiness": readiness,
                "status": status,
            }

        # sincronizzazione da headless_main / mini_gui
        self.set_simulation_mode(self.execution_mode != "LIVE")

        # reset anti-duplicazione a ogni start
        self.duplication_guard = DuplicationGuard()
        self.reconciliation_engine = self._build_reconciliation_engine()

        start_connect = time.monotonic()
        try:
            session = self.betfair_service.connect(
                password=password,
                simulation_mode=self.simulation_mode,
            )
            self._record_runtime_io(operation="betfair_connect", started_at=start_connect, ok=True)
        except Exception as exc:
            self._record_runtime_io(operation="betfair_connect", started_at=start_connect, ok=False, error=str(exc))
            raise
        start_funds = time.monotonic()
        try:
            funds = self.betfair_service.get_account_funds()
            self._record_runtime_io(operation="betfair_get_account_funds", started_at=start_funds, ok=True)
        except Exception as exc:
            self._record_runtime_io(
                operation="betfair_get_account_funds",
                started_at=start_funds,
                ok=False,
                error=str(exc),
            )
            raise
        self.risk_desk.sync_bankroll(float(funds.get("available", 0.0) or 0.0))

        start_telegram = time.monotonic()
        try:
            telegram_result = self.telegram_service.start()
            self._record_runtime_io(operation="telegram_start", started_at=start_telegram, ok=True)
        except Exception as exc:
            self._record_runtime_io(operation="telegram_start", started_at=start_telegram, ok=False, error=str(exc))
            raise
        start_market_data = time.monotonic()
        try:
            self._start_market_data_feed()
            self._record_runtime_io(operation="market_data_feed_start", started_at=start_market_data, ok=True)
        except StreamingConfigError as exc:
            self._record_runtime_io(operation="market_data_feed_start", started_at=start_market_data, ok=False, error=str(exc))
            raise RuntimeError(f"MARKET_DATA_CONFIG_INVALID:{exc}") from exc
        except Exception as exc:
            self._record_runtime_io(operation="market_data_feed_start", started_at=start_market_data, ok=False, error=str(exc))
            logger.exception("Errore start market data feed: %s", exc)

        try:
            self.reconciliation_engine.reconcile_all_open_batches()
        except Exception:
            logger.exception("Errore reconcile_all_open_batches")

        self.mode = RuntimeMode.ACTIVE
        self.last_error = ""
        status = self.get_status()
        self.bus.publish("RUNTIME_STARTED", status)

        return {
            "started": True,
            "betfair": session,
            "funds": funds,
            "telegram": telegram_result,
            "status": status,
        }

    def stop(self) -> dict:
        self._stop_market_data_feed()
        self.telegram_service.stop()
        self.betfair_service.disconnect()
        self.mode = RuntimeMode.STOPPED
        status = self.get_status()
        self.bus.publish("RUNTIME_STOPPED", status)
        return {
            "stopped": True,
            "status": status,
        }

    def pause(self) -> dict:
        self.mode = RuntimeMode.PAUSED
        status = self.get_status()
        self.bus.publish("RUNTIME_PAUSED", status)
        return {
            "paused": True,
            "status": status,
        }

    def resume(self) -> dict:
        if self.mode == RuntimeMode.LOCKDOWN:
            return {
                "resumed": False,
                "reason": "lockdown_attivo",
                "status": self.get_status(),
            }

        self.mode = RuntimeMode.ACTIVE
        status = self.get_status()
        self.bus.publish("RUNTIME_RESUMED", status)
        return {
            "resumed": True,
            "status": status,
        }

    def reset_cycle(self) -> dict:
        self.table_manager.reset_all()
        self.duplication_guard.clear()
        self.risk_desk.reset_recovery_cycle()

        if self.simulation_mode and hasattr(self.betfair_service, "reset_simulation"):
            try:
                self.betfair_service.reset_simulation()
            except Exception:
                logger.exception("Errore reset_simulation")

        status = self.get_status()
        self.bus.publish("RUNTIME_CYCLE_RESET", status)
        return {
            "reset": True,
            "status": status,
        }

    # =========================================================
    # SIGNAL FLOW
    # =========================================================
    def _reject_signal(self, signal: dict, reason: str) -> None:
        self.bus.publish(
            "SIGNAL_REJECTED",
            {
                "reason": reason,
                "signal": signal,
                "ts": datetime.utcnow().isoformat(),
            },
        )

    def _event_current_exposure(self, event_key: str) -> float:
        table = self.table_manager.find_by_event_key(event_key)
        if not table:
            return 0.0
        return float(table.current_exposure or 0.0)

    def _on_signal_received(self, signal: dict) -> None:
        """
        Runtime signal gate for Telegram/UI-driven order intents.

        Ownership boundary:
        - this method validates runtime readiness + anti-duplication + MM/table checks
        - it does NOT resolve Telegram text parsing (owned upstream)
        - it forwards already-normalized copy/pattern metadata to TradingEngine
          as passthrough context (no strategy rewrite here)
        """
        signal = dict(signal or {})
        self.last_signal_at = datetime.utcnow().isoformat()

        # Emergency stop hard gate — refuses ALL live order entry
        if self._emergency_stopped:
            self._reject_signal(
                signal,
                f"emergency_stop_active:triggered_at={self._emergency_stopped_at}",
            )
            return

        if str(self.execution_mode).upper() == "LIVE":
            # Session guard — refuse live signals when session is known-invalid.
            _svc = self.betfair_service
            if _svc is not None and getattr(_svc, "_session_invalid", False):
                self._reject_signal(signal, "session_invalid_live_blocked")
                return

            deploy_gate = self.enforce_deploy_gate(
                execution_mode="LIVE",
                live_enabled=self.live_enabled,
                live_readiness_ok=self.live_readiness_ok,
            )
            if not deploy_gate["allowed"]:
                self.execution_mode = "SIMULATION"
                self.set_simulation_mode(True)
                self._reject_signal(signal, f"deploy_gate_no_go:{deploy_gate['reason']}")
                self.bus.publish(
                    "LIVE_EXECUTION_REFUSED",
                    {
                        "reason_code": deploy_gate["reason"],
                        "reasons": deploy_gate.get("reasons", []),
                        "requested_execution_mode": "LIVE",
                        "effective_execution_mode": "SIMULATION",
                        "deploy_gate": deploy_gate,
                    },
                )
                return

        if not self._runtime_active():
            self._reject_signal(signal, f"runtime_non_attivo:{self.mode.value}")
            return

        required = ["market_id", "selection_id"]
        missing = [k for k in required if signal.get(k) in (None, "")]
        if missing:
            self._reject_signal(signal, f"campi_mancanti:{','.join(missing)}")
            return

        copy_meta, pattern_meta = self._extract_origin_metadata(signal)
        if isinstance(copy_meta, dict) and isinstance(pattern_meta, dict):
            self._reject_signal(signal, "copy_pattern_mutually_exclusive")
            return

        event_key = self.duplication_guard.build_event_key(signal)
        signal["event_key"] = event_key

        if self.config.anti_duplication_enabled and not self.duplication_guard.acquire(event_key):
            self._reject_signal(signal, "duplicato_bloccato")
            return

        table = self.table_manager.allocate(
            event_key=event_key,
            allow_recovery=bool(self.config.allow_recovery),
        )
        if table is None:
            if self.config.anti_duplication_enabled:
                self.duplication_guard.release(event_key)
            self._reject_signal(signal, "nessun_tavolo_disponibile")
            return

        total_exposure = self.table_manager.total_exposure()
        event_exposure = self._event_current_exposure(event_key)

        decision = self.mm.calculate(
            signal=signal,
            bankroll_current=self.risk_desk.bankroll_current,
            equity_peak=self.risk_desk.equity_peak,
            current_total_exposure=total_exposure,
            event_current_exposure=event_exposure,
            table=table,
        )

        if not decision.approved:
            if self.config.anti_duplication_enabled:
                self.duplication_guard.release(event_key)
            if decision.desk_mode == DeskMode.LOCKDOWN:
                self.force_lockdown(decision.reason)
            self._reject_signal(signal, decision.reason)
            return

        payload = {
            "market_id": str(signal.get("market_id")),
            "selection_id": int(signal.get("selection_id")),
            "bet_type": str(
                signal.get("bet_type")
                or signal.get("side")
                or signal.get("action")
                or "BACK"
            ).upper(),
            "price": float(signal.get("price") or signal.get("odds")),
            "stake": float(decision.recommended_stake),
            "event_name": signal.get("event") or signal.get("match") or signal.get("event_name") or "",
            "market_name": signal.get("market") or signal.get("market_name") or signal.get("market_type") or "",
            "runner_name": signal.get("selection") or signal.get("runner_name") or signal.get("runnerName") or "",
            "simulation_mode": bool(signal.get("simulation_mode", self.simulation_mode)),
            "event_key": event_key,
            "table_id": decision.table_id,
            "batch_id": str(signal.get("batch_id") or ""),
            "roserpina_reason": decision.reason,
            "roserpina_mode": decision.desk_mode.value,
        }
        routing_contract = signal.get("telegram_routing_contract")
        if isinstance(routing_contract, str) and routing_contract.strip():
            payload["telegram_routing_contract"] = routing_contract.strip()
        route_target = signal.get("telegram_route_target")
        if isinstance(route_target, str) and route_target.strip():
            payload["telegram_route_target"] = route_target.strip()
        has_copy_meta = isinstance(copy_meta, dict)
        has_pattern_meta = isinstance(pattern_meta, dict)
        if has_copy_meta and has_pattern_meta:
            # Defensive fail-closed guard: Runtime should never forward both.
            self._reject_signal(signal, "copy_pattern_mutually_exclusive")
            return
        if has_copy_meta:
            payload["copy_meta"] = dict(copy_meta)
        elif has_pattern_meta:
            payload["pattern_meta"] = dict(pattern_meta)
        order_origin = self._resolve_order_origin(
            signal,
            has_copy_meta=has_copy_meta,
            has_pattern_meta=has_pattern_meta,
        )
        if order_origin:
            payload["order_origin"] = order_origin

        self.table_manager.activate(
            table_id=decision.table_id,
            event_key=event_key,
            exposure=float(decision.recommended_stake),
            market_id=payload["market_id"],
            selection_id=payload["selection_id"],
            meta={
                "event_name": payload["event_name"],
                "market_name": payload["market_name"],
                "runner_name": payload["runner_name"],
                "bet_type": payload["bet_type"],
                "price": payload["price"],
                "simulation_mode": payload["simulation_mode"],
            },
        )
        self.bus.publish(
            "SIGNAL_APPROVED",
            {
                "signal": signal,
                "decision": {
                    "table_id": decision.table_id,
                    "recommended_stake": decision.recommended_stake,
                    "desk_mode": decision.desk_mode.value,
                    "reason": decision.reason,
                    "metadata": decision.metadata,
                },
            },
        )
        self.bus.publish("CMD_QUICK_BET", payload)

    # =========================================================
    # BET LIFECYCLE
    # =========================================================
    def _release_if_terminal(self, payload: dict, *, event_name: str) -> None:
        if event_name not in TERMINAL_LIFECYCLE_EVENTS:
            return
        payload = dict(payload or {})
        event_key = str(payload.get("event_key") or "")
        table_id = payload.get("table_id")

        if event_key:
            self.duplication_guard.release(event_key)

        if table_id is not None:
            try:
                self.table_manager.force_unlock(int(table_id))
            except Exception:
                logger.exception("Errore force_unlock table_id=%s", table_id)

    def _on_quick_bet_failed(self, payload: dict) -> None:
        self._release_if_terminal(payload, event_name="QUICK_BET_FAILED")

    def _on_quick_bet_accepted(self, payload: dict) -> None:
        self._release_if_terminal(payload, event_name="QUICK_BET_ACCEPTED")

    def _on_quick_bet_partial(self, payload: dict) -> None:
        self._release_if_terminal(payload, event_name="QUICK_BET_PARTIAL")

    def _on_quick_bet_filled(self, payload: dict) -> None:
        self._release_if_terminal(payload, event_name="QUICK_BET_FILLED")

    def _on_quick_bet_success(self, payload: dict) -> None:
        self._release_if_terminal(payload, event_name="QUICK_BET_SUCCESS")

    def _on_quick_bet_ambiguous(self, payload: dict) -> None:
        self._release_if_terminal(payload, event_name="QUICK_BET_AMBIGUOUS")

    def _on_quick_bet_rollback_done(self, payload: dict) -> None:
        self._release_if_terminal(payload, event_name="QUICK_BET_ROLLBACK_DONE")

    # =========================================================
    # MANUAL/EXTERNAL CLOSE POSITION
    # =========================================================
    def _on_close_position(self, payload: dict) -> None:
        if not isinstance(payload, dict):
            return

        settlement = self._extract_settlement_contract(payload)
        table_id = payload.get("table_id")
        pnl = float(settlement["net_pnl"])
        event_key = str(payload.get("event_key") or "")
        batch_id = str(payload.get("batch_id") or "")
        validation_status = str(settlement.get("settlement_validation") or "accepted")
        settlement_acceptance = str(settlement.get("settlement_acceptance") or "")
        non_authoritative_close = validation_status == "rejected_non_canonical_settlement"

        if validation_status.startswith("rejected") and not non_authoritative_close:
            sync_result = {
                "correlation_id": str(payload.get("correlation_id") or payload.get("event_key") or ""),
                "settlement_detected": True,
                "bankroll_before": float(self.risk_desk.bankroll_current),
                "bankroll_after": float(self.risk_desk.bankroll_current),
                "bankroll_sync_status": "SYNC_FAILED_INVALID_SETTLEMENT_CONTRACT",
                "balance_source": "none",
                "reason": str(settlement.get("reason") or "SETTLEMENT_CONTRACT_REJECTED"),
                "settlement_acceptance": settlement_acceptance,
            }
            auto_trade_result = {
                "correlation_id": str(payload.get("correlation_id") or payload.get("event_key") or ""),
                "source_settlement_correlation_id": str(payload.get("correlation_id") or ""),
                "bankroll_sync_status": "SYNC_FAILED_INVALID_SETTLEMENT_CONTRACT",
                "money_management_status": "MM_STOP_SETTLEMENT_CONTRACT_REJECTED",
                "cycle_active": False,
                "progression_allowed": False,
                "auto_trade_enabled": False,
                "auto_trade_status": "AUTO_TRADE_REJECTED_SETTLEMENT",
                "next_stake": 0.0,
                "risk_status": "RISK_NOT_EVALUATED",
                "submitted": False,
                "reason": str(settlement.get("reason") or "SETTLEMENT_CONTRACT_REJECTED"),
                "settlement_acceptance": settlement_acceptance,
            }
            self._last_bankroll_sync_result = dict(sync_result)
            self._last_auto_trade_result = dict(auto_trade_result)
            self._last_cycle_executor_result = dict(auto_trade_result)
            self.bus.publish("BANKROLL_SYNC_RESULT", dict(sync_result))
            self.bus.publish("AUTO_TRADE_MM_RESULT", dict(auto_trade_result))
            return

        if table_id is not None:
            self.table_manager.release(int(table_id), pnl=pnl)

        if event_key:
            self.duplication_guard.release(event_key)
        settlement_key = self._build_bankroll_sync_key(payload)
        recovery_probe = self._read_cycle_recovery_state(settlement_key)
        if self._should_fail_closed_on_recovery(recovery_probe):
            fail_result = self._build_fail_closed_recovery_result(payload=payload, probe=recovery_probe)
            sync_result = {
                "correlation_id": str(payload.get("correlation_id") or payload.get("event_key") or ""),
                "settlement_detected": True,
                "bankroll_before": float(self.risk_desk.bankroll_current),
                "bankroll_after": float(self.risk_desk.bankroll_current),
                "bankroll_sync_status": "SYNC_SKIPPED_DUPLICATE",
                "balance_source": "durable_checkpoint",
                "reason": "RECOVERY_FAIL_CLOSED",
                "recovery_status": str(fail_result.get("recovery_status") or "RECOVERY_STATE_AMBIGUOUS"),
            }
            self._last_bankroll_sync_result = dict(sync_result)
            self._last_auto_trade_result = dict(fail_result)
            self._last_cycle_executor_result = dict(fail_result)
            self.bus.publish("BANKROLL_SYNC_RESULT", dict(sync_result))
            self.bus.publish("AUTO_TRADE_MM_RESULT", dict(fail_result))
            return
        self._persist_cycle_checkpoint(
            settlement_key=settlement_key,
            payload=payload,
            checkpoint_stage="SETTLEMENT_DETECTED",
            next_trade_submission_status="NOT_ATTEMPTED",
            reason="settlement_detected",
            recovery_status=recovery_probe.get("status", "RECOVERY_NO_STATE"),
        )
        if (
            not non_authoritative_close
            and settlement_key
            and settlement_key not in self._processed_realized_pnl_keys
        ):
            self._apply_realized_pnl_without_mutating_bankroll(pnl)
            self._processed_realized_pnl_keys.add(settlement_key)
        sync_result = self._sync_bankroll_post_settlement(payload)
        self._last_bankroll_sync_result = dict(sync_result)
        self.bus.publish("BANKROLL_SYNC_RESULT", dict(sync_result))
        auto_trade_result = self._evaluate_and_maybe_submit_auto_next_trade(payload=payload, sync_result=sync_result)
        self._last_auto_trade_result = dict(auto_trade_result)
        self._last_cycle_executor_result = dict(auto_trade_result)
        self.bus.publish("AUTO_TRADE_MM_RESULT", dict(auto_trade_result))

        current_drawdown = self.risk_desk.drawdown_pct()

        if batch_id:
            self.bus.publish(
                "BATCH_POSITION_CLOSED",
                {
                    "batch_id": batch_id,
                    "pnl": pnl,
                    "gross_pnl": float(settlement["gross_pnl"]),
                    "commission_amount": float(settlement["commission_amount"]),
                    "net_pnl": float(settlement["net_pnl"]),
                    "commission_pct": float(settlement["commission_pct"]),
                    "settlement_basis": str(settlement["settlement_basis"]),
                    "settlement_source": str(settlement["settlement_source"]),
                    "settlement_kind": str(settlement["settlement_kind"]),
                    "settlement_authority": str(settlement["settlement_authority"]),
                    "settlement_validation": str(settlement["settlement_validation"]),
                    "settlement_acceptance": str(settlement["settlement_acceptance"]),
                    "event_key": event_key,
                },
            )

        if current_drawdown >= self.config.auto_reset_drawdown_pct:
            self.table_manager.reset_all()
            self.duplication_guard.clear()
            self.risk_desk.reset_recovery_cycle()

            self.bus.publish(
                "ROSERPINA_AUTO_RESET",
                {
                    "reason": "drawdown_limit",
                    "drawdown_pct": current_drawdown,
                },
            )

        if current_drawdown >= self.config.lockdown_drawdown_pct:
            self.force_lockdown("Drawdown oltre soglia lockdown")

    def _apply_realized_pnl_without_mutating_bankroll(self, pnl: float) -> None:
        bankroll_before = float(self.risk_desk.bankroll_current)
        self.risk_desk.apply_closed_pnl(pnl)
        if float(self.risk_desk.bankroll_current) != bankroll_before:
            self.risk_desk.sync_bankroll(bankroll_before)

    @staticmethod
    def _extract_settlement_contract(payload: dict) -> dict[str, float | str]:
        body = dict(payload or {})
        explicit_net_raw = body.get("net_pnl") if "net_pnl" in body else None
        legacy_net_raw = body.get("pnl") if "pnl" in body else None
        has_explicit_net = explicit_net_raw is not None
        has_legacy_net = legacy_net_raw is not None
        has_canonical_contract = all(
            k in body and body.get(k) is not None
            for k in (
                "gross_pnl",
                "commission_amount",
                "net_pnl",
                "commission_pct",
                "settlement_basis",
                "settlement_source",
                "settlement_kind",
            )
        )
        if has_canonical_contract and has_explicit_net:
            net_pnl = explicit_net_raw
            settlement_authority = "explicit_contract"
            settlement_validation = "accepted"
            settlement_acceptance = "ACCEPT_REALIZED_SETTLEMENT"
        elif has_legacy_net:
            net_pnl = legacy_net_raw
            settlement_authority = "legacy_compat"
            settlement_validation = "rejected_non_canonical_settlement"
            settlement_acceptance = "REJECT_NON_CANONICAL_SETTLEMENT"
        else:
            net_pnl = 0.0
            settlement_authority = "rejected_ambiguous"
            settlement_validation = "rejected_ambiguous"
            settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
        net_pnl_f = float(net_pnl if net_pnl is not None else 0.0)
        has_explicit_gross = "gross_pnl" in body and body.get("gross_pnl") is not None
        has_explicit_commission = "commission_amount" in body and body.get("commission_amount") is not None
        if has_explicit_gross:
            gross_pnl_f = float(body.get("gross_pnl") or 0.0)
        elif has_explicit_net and has_explicit_commission:
            gross_pnl_f = float(net_pnl_f + float(body.get("commission_amount") or 0.0))
        else:
            gross_pnl_f = float(net_pnl_f)

        if has_explicit_commission:
            commission_amount_f = float(body.get("commission_amount") or 0.0)
        elif has_explicit_gross and has_explicit_net:
            commission_amount_f = float(gross_pnl_f - net_pnl_f)
        else:
            commission_amount_f = 0.0
        commission_pct_f = float(body.get("commission_pct", 0.0) or 0.0)
        settlement_source = str(
            body.get("settlement_source")
            or body.get("source")
            or ("legacy_compat" if settlement_authority == "legacy_compat" else "")
        )
        settlement_kind = str(
            body.get("settlement_kind")
            or ("legacy_compat" if settlement_authority == "legacy_compat" else "")
        )
        settlement_basis = str(
            body.get("settlement_basis")
            or ("legacy_compat" if settlement_authority == "legacy_compat" else "")
        )
        reason = ""
        if settlement_authority == "legacy_compat":
            if not settlement_source:
                settlement_source = "legacy_compat"
            if not settlement_kind:
                settlement_kind = "legacy_compat"
            settlement_validation = "rejected_non_canonical_settlement"
            reason = "LEGACY_SETTLEMENT_NON_AUTHORITATIVE"
            settlement_acceptance = "REJECT_NON_CANONICAL_SETTLEMENT"
        elif settlement_authority.startswith("rejected"):
            reason = "MISSING_CANONICAL_SETTLEMENT_FIELDS"
            settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"

        if settlement_validation == "accepted":
            if settlement_kind != "realized_settlement":
                settlement_validation = "rejected_non_realized_settlement"
                reason = "SETTLEMENT_KIND_NOT_REALIZED"
                settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
            elif settlement_basis != "market_net_realized":
                settlement_validation = "rejected_non_market_net_basis"
                reason = "SETTLEMENT_BASIS_NOT_MARKET_NET_REALIZED"
                settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
            elif not settlement_source:
                settlement_validation = "rejected_ambiguous_source"
                reason = "MISSING_SETTLEMENT_SOURCE"
                settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"

        if settlement_validation == "accepted" and settlement_kind == "realized_settlement":
            arithmetic_tolerance = 1e-9
            finite_values = (
                math.isfinite(gross_pnl_f),
                math.isfinite(commission_amount_f),
                math.isfinite(net_pnl_f),
                math.isfinite(commission_pct_f),
            )
            if not all(finite_values):
                settlement_validation = "rejected_non_finite_settlement_values"
                reason = "SETTLEMENT_VALUES_NOT_FINITE"
                settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
            elif abs((gross_pnl_f - commission_amount_f) - net_pnl_f) > arithmetic_tolerance:
                settlement_validation = "rejected_arithmetic_incoherent_settlement"
                reason = "SETTLEMENT_ARITHMETIC_INCOHERENT"
                settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
            elif gross_pnl_f <= 0.0:
                if commission_amount_f > arithmetic_tolerance:
                    settlement_validation = "rejected_non_zero_commission_on_non_positive_gross"
                    reason = "NON_POSITIVE_GROSS_REQUIRES_ZERO_COMMISSION"
                    settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
                elif commission_amount_f < -arithmetic_tolerance:
                    if net_pnl_f > arithmetic_tolerance:
                        settlement_validation = "rejected_impossible_negative_rebate_positive_net"
                        reason = "NEGATIVE_REBATE_CANNOT_CREATE_POSITIVE_NET_ON_LOSS"
                        settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
                    elif abs(commission_amount_f) - abs(gross_pnl_f) > arithmetic_tolerance:
                        settlement_validation = "rejected_negative_rebate_exceeds_gross_abs_bound"
                        reason = "NEGATIVE_REBATE_EXCEEDS_GROSS_ABS_BOUND"
                        settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
            elif gross_pnl_f > 0.0:
                expected_commission = gross_pnl_f * (commission_pct_f / 100.0)
                if abs(commission_amount_f - expected_commission) > arithmetic_tolerance:
                    settlement_validation = "rejected_commission_amount_policy_mismatch"
                    reason = "COMMISSION_AMOUNT_POLICY_MISMATCH"
                    settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
        if settlement_validation == "accepted" and settlement_kind == "realized_settlement":
            try:
                enforce_betfair_italy_commission_pct(
                    commission_pct_f,
                    context=f"runtime_controller:{settlement_source}",
                )
            except ValueError:
                settlement_validation = "rejected_policy_violation"
                reason = "BETFAIR_ITALY_COMMISSION_POLICY_VIOLATION"
                settlement_acceptance = "REJECT_POLICY_VIOLATION"
        return {
            "gross_pnl": gross_pnl_f,
            "commission_amount": commission_amount_f,
            "net_pnl": net_pnl_f,
            "commission_pct": commission_pct_f,
            "settlement_basis": settlement_basis,
            "settlement_source": settlement_source,
            "settlement_kind": settlement_kind,
            "settlement_authority": settlement_authority,
            "settlement_validation": settlement_validation,
            "settlement_acceptance": settlement_acceptance,
            "reason": reason,
        }

    def _sync_bankroll_post_settlement(self, payload: dict) -> dict:
        bankroll_before = float(self.risk_desk.bankroll_current)
        correlation_id = str(
            payload.get("correlation_id")
            or payload.get("customer_ref")
            or payload.get("event_key")
            or ""
        )
        result = {
            "correlation_id": correlation_id,
            "settlement_detected": True,
            "bankroll_before": bankroll_before,
            "bankroll_after": bankroll_before,
            "bankroll_sync_status": "NOT_SETTLED",
            "balance_source": "none",
            "reason": "",
        }

        settlement_key = self._build_bankroll_sync_key(payload)
        if not settlement_key:
            result["bankroll_sync_status"] = "SYNC_FAILED_INVALID_BALANCE"
            result["reason"] = "MISSING_SETTLEMENT_KEY"
            return result

        durable_state = self._read_cycle_recovery_state(settlement_key)
        state_body = durable_state.get("state", {})
        if bool(state_body.get("bankroll_synced")):
            self._processed_bankroll_sync_keys.add(settlement_key)
            result["recovery_status"] = "RECOVERY_SKIPPED_DUPLICATE"
        elif durable_state.get("status"):
            result["recovery_status"] = str(durable_state.get("status"))

        if settlement_key in self._processed_bankroll_sync_keys:
            result["bankroll_sync_status"] = "SYNC_SKIPPED_DUPLICATE"
            result["reason"] = "SETTLEMENT_ALREADY_SYNCED"
            return result

        started_at = time.monotonic()
        try:
            funds = self.betfair_service.get_account_funds()
            self._record_runtime_io(
                operation="betfair_get_account_funds",
                started_at=started_at,
                ok=True,
            )
        except Exception as exc:
            self._record_runtime_io(
                operation="betfair_get_account_funds",
                started_at=started_at,
                ok=False,
                error=str(exc),
            )
            result["bankroll_sync_status"] = "SYNC_FAILED_BALANCE_UNAVAILABLE"
            result["reason"] = f"BALANCE_FETCH_ERROR:{type(exc).__name__}"
            return result

        if not isinstance(funds, dict):
            result["bankroll_sync_status"] = "SYNC_FAILED_BALANCE_UNAVAILABLE"
            result["reason"] = "BALANCE_PAYLOAD_NOT_DICT"
            return result

        available = funds.get("available")
        try:
            available_f = float(available)
        except Exception:
            result["bankroll_sync_status"] = "SYNC_FAILED_INVALID_BALANCE"
            result["reason"] = "BALANCE_NOT_NUMERIC"
            return result

        if not math.isfinite(available_f) or available_f < 0.0:
            result["bankroll_sync_status"] = "SYNC_FAILED_INVALID_BALANCE"
            result["reason"] = "BALANCE_NOT_FINITE_OR_NEGATIVE"
            return result

        trusted_zero = bool(
            funds.get("ok")
            or funds.get("authoritative")
            or funds.get("balance_confirmed")
        )
        if available_f == 0.0 and not trusted_zero:
            result["bankroll_sync_status"] = "SYNC_FAILED_BALANCE_UNAVAILABLE"
            result["reason"] = "BALANCE_ZERO_AMBIGUOUS_OR_FALLBACK"
            return result

        self.risk_desk.sync_bankroll(available_f)
        self._processed_bankroll_sync_keys.add(settlement_key)
        result["bankroll_after"] = float(self.risk_desk.bankroll_current)
        result["bankroll_sync_status"] = "SYNC_SUCCESS"
        result["balance_source"] = "exchange_available"
        result["reason"] = "BALANCE_SYNCED_FROM_EXCHANGE"
        self._persist_cycle_checkpoint(
            settlement_key=settlement_key,
            payload=payload,
            checkpoint_stage="BANKROLL_SYNC_DONE",
            bankroll_sync_status=result["bankroll_sync_status"],
            next_trade_submission_status="NOT_ATTEMPTED",
            reason=result["reason"],
            recovery_status=str(result.get("recovery_status") or "RECOVERY_STATE_LOADED"),
        )
        return result

    @staticmethod
    def _build_bankroll_sync_key(payload: dict) -> str:
        parts = [
            str(payload.get("batch_id") or "").strip(),
            str(payload.get("event_key") or "").strip(),
            str(payload.get("table_id") or "").strip(),
            str(payload.get("bet_id") or "").strip(),
            str(payload.get("order_id") or "").strip(),
        ]
        parts = [p for p in parts if p]
        if not parts:
            return ""
        return "|".join(parts)

    def _evaluate_and_maybe_submit_auto_next_trade(self, *, payload: dict, sync_result: dict) -> dict:
        settlement_key = self._build_bankroll_sync_key(payload)
        source_corr_id = str(sync_result.get("correlation_id") or "")
        recovery_enabled = bool(payload.get("recovery_enabled", self.config.allow_recovery))
        resume_submit_enabled = bool(payload.get("resume_submit_enabled", False))
        result: dict[str, Any] = {
            "correlation_id": f"auto-next::{source_corr_id}" if source_corr_id else "",
            "source_settlement_correlation_id": source_corr_id,
            "recovery_enabled": recovery_enabled,
            "resume_submit_enabled": resume_submit_enabled,
            "cycle_executor_enabled": bool(payload.get("cycle_executor_enabled", False)),
            "cycle_step_index": 0,
            "max_steps_reached": False,
            "kill_switch_active": bool(self._is_kill_switch_active()),
            "anomaly_pause_active": False,
            "cycle_executor_status": "CYCLE_NOT_ELIGIBLE",
            "bankroll_sync_status": str(sync_result.get("bankroll_sync_status") or "NOT_SETTLED"),
            "money_management_status": "MM_STOP_CONTEXT_MISSING",
            "cycle_active": False,
            "progression_allowed": False,
            "auto_trade_enabled": bool(payload.get("auto_trade_enabled", False)),
            "auto_trade_status": "AUTO_TRADE_NOT_ELIGIBLE",
            "next_stake": 0.0,
            "risk_status": "RISK_NOT_EVALUATED",
            "submitted": False,
            "reason": "",
            "recovery_status": "RECOVERY_NO_STATE",
            "checkpoint_stage": "",
            "checkpoint_valid": False,
            "checkpoint_ambiguous": False,
        }
        checkpoint_capable = callable(getattr(self.db, "get_cycle_recovery_state", None))
        recovery_probe = self._read_cycle_recovery_state(settlement_key)
        probe_status = str(recovery_probe.get("status") or "RECOVERY_NO_STATE")
        result["recovery_status"] = probe_status
        recovery_state = recovery_probe.get("state", {})
        checkpoint = recovery_state.get("checkpoint", {}) if isinstance(recovery_state, dict) else {}
        result["checkpoint_stage"] = str(checkpoint.get("checkpoint_stage") or recovery_state.get("stage") or "")
        result["checkpoint_ambiguous"] = bool(recovery_state.get("ambiguous"))
        result["checkpoint_valid"] = bool(recovery_state.get("exists")) and not bool(recovery_state.get("ambiguous"))
        has_checkpoint = bool(recovery_state.get("exists"))

        if not recovery_enabled and has_checkpoint:
            result["auto_trade_status"] = "AUTO_TRADE_DISABLED"
            result["cycle_executor_status"] = "CYCLE_NOT_ELIGIBLE"
            result["recovery_status"] = "RECOVERY_DISABLED"
            result["reason"] = "recovery_disabled"
            return result

        if self._should_fail_closed_on_recovery(recovery_probe):
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_DUPLICATE"
            result["cycle_executor_status"] = "CYCLE_AMBIGUOUS"
            result["recovery_status"] = "RECOVERY_STATE_AMBIGUOUS"
            result["checkpoint_valid"] = False
            result["reason"] = "recovery_state_ambiguous"
            self._persist_cycle_checkpoint(
                settlement_key=settlement_key,
                payload=payload,
                checkpoint_stage="CYCLE_AMBIGUOUS",
                bankroll_sync_status=result["bankroll_sync_status"],
                money_management_status=result["money_management_status"],
                next_trade_submission_status="AMBIGUOUS",
                reason=result["reason"],
                is_ambiguous=True,
                recovery_status="RECOVERY_STATE_AMBIGUOUS",
            )
            return result
        if bool(recovery_state.get("submit_confirmed")) or bool(recovery_state.get("submit_attempted")):
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_DUPLICATE"
            result["cycle_executor_status"] = "CYCLE_SKIPPED_DUPLICATE"
            result["recovery_status"] = "RECOVERY_SKIPPED_ALREADY_SUBMITTED"
            result["reason"] = "durable_submit_already_confirmed"
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            return result
        if checkpoint_capable and not resume_submit_enabled and result["cycle_executor_enabled"]:
            result["auto_trade_status"] = "AUTO_TRADE_DISABLED"
            result["cycle_executor_status"] = "CYCLE_NOT_ELIGIBLE"
            result["recovery_status"] = "RECOVERY_READY_NO_SUBMIT"
            result["reason"] = "resume_submit_disabled"
            return result

        if not result["cycle_executor_enabled"]:
            result["auto_trade_status"] = "AUTO_TRADE_DISABLED"
            result["cycle_executor_status"] = "CYCLE_EXECUTOR_DISABLED"
            result["recovery_status"] = "RECOVERY_READY_NO_SUBMIT" if has_checkpoint else result["recovery_status"]
            result["reason"] = "cycle_executor_disabled"
            self._persist_cycle_checkpoint(
                settlement_key=settlement_key,
                payload=payload,
                checkpoint_stage="CYCLE_BLOCKED",
                bankroll_sync_status=result["bankroll_sync_status"],
                next_trade_submission_status="NOT_ATTEMPTED",
                reason=result["reason"],
                recovery_status=result["recovery_status"],
            )
            return result

        if result["kill_switch_active"]:
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_MM_BLOCKED"
            result["cycle_executor_status"] = "CYCLE_STOPPED_KILL_SWITCH"
            result["recovery_status"] = "RECOVERY_SKIPPED_MM_BLOCKED" if has_checkpoint else result["recovery_status"]
            result["reason"] = "kill_switch_active"
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            self._persist_cycle_checkpoint(
                settlement_key=settlement_key,
                payload=payload,
                checkpoint_stage="CYCLE_BLOCKED",
                bankroll_sync_status=result["bankroll_sync_status"],
                next_trade_submission_status="NOT_ATTEMPTED",
                reason=result["reason"],
                recovery_status=result["recovery_status"],
            )
            return result

        if result["bankroll_sync_status"] != "SYNC_SUCCESS":
            if result["bankroll_sync_status"] == "SYNC_SKIPPED_DUPLICATE":
                result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_DUPLICATE"
                result["cycle_executor_status"] = "CYCLE_SKIPPED_DUPLICATE"
                result["reason"] = "settlement_already_processed"
            else:
                result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_SYNC_FAILED"
                result["cycle_executor_status"] = "CYCLE_SKIPPED_SYNC_FAILED"
                result["reason"] = "bankroll_sync_not_success"
            return result

        if not result["auto_trade_enabled"]:
            result["auto_trade_status"] = "AUTO_TRADE_DISABLED"
            result["cycle_executor_status"] = "CYCLE_NOT_ELIGIBLE"
            result["recovery_status"] = "RECOVERY_READY_NO_SUBMIT" if has_checkpoint else result["recovery_status"]
            result["reason"] = "auto_trade_disabled"
            return result

        if settlement_key and settlement_key in self._processed_auto_trade_keys:
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_DUPLICATE"
            result["cycle_executor_status"] = "CYCLE_SKIPPED_DUPLICATE"
            result["recovery_status"] = "RECOVERY_SKIPPED_DUPLICATE" if has_checkpoint else result["recovery_status"]
            result["reason"] = "settlement_auto_trade_already_evaluated"
            return result

        mm_context = payload.get("mm_context")
        signal = mm_context.get("next_signal") if isinstance(mm_context, dict) else None
        table_ctx = mm_context.get("table") if isinstance(mm_context, dict) else None
        cycle_id = mm_context.get("cycle_id", "") if isinstance(mm_context, dict) else ""
        cycle_active = bool(mm_context.get("cycle_active", True)) if isinstance(mm_context, dict) else False
        target_reached = bool(mm_context.get("target_reached", False)) if isinstance(mm_context, dict) else False
        table_id = mm_context.get("table_id") if isinstance(mm_context, dict) else None

        table = table_ctx if table_ctx is not None else (
            {"table_id": table_id} if table_id is not None else None
        )
        decision = self.mm.evaluate_next_trade_after_settlement(
            signal=signal,
            bankroll_current=float(self.risk_desk.bankroll_current),
            equity_peak=float(self.risk_desk.equity_peak),
            current_total_exposure=self.table_manager.total_exposure(),
            event_current_exposure=0.0,
            table=table,
            cycle_id=str(cycle_id or ""),
            cycle_active=cycle_active,
            target_reached=target_reached,
        )
        result["money_management_status"] = decision.money_management_status
        result["cycle_active"] = bool(decision.cycle_active)
        result["progression_allowed"] = bool(decision.progression_allowed)
        result["next_stake"] = float(decision.next_stake or 0.0)
        cycle_id = str(getattr(decision, "cycle_id", "") or "")
        current_step_index = int(self._cycle_step_counts.get(cycle_id, 0) or 0)
        result["cycle_step_index"] = current_step_index
        self._persist_cycle_checkpoint(
            settlement_key=settlement_key,
            payload=payload,
            checkpoint_stage="MM_DECISION_DONE",
            bankroll_sync_status=result["bankroll_sync_status"],
            money_management_status=result["money_management_status"],
            cycle_active=result["cycle_active"],
            progression_allowed=result["progression_allowed"],
            next_stake=result["next_stake"],
            step_index=result["cycle_step_index"],
            next_trade_submission_status="NOT_ATTEMPTED",
            reason="mm_decision_done",
            recovery_status=result["recovery_status"],
        )

        max_steps = mm_context.get("max_steps") if isinstance(mm_context, dict) else None
        max_steps_value = None
        try:
            if max_steps is not None:
                max_steps_value = int(max_steps)
        except Exception:
            max_steps_value = None
        if max_steps_value is not None and max_steps_value >= 0 and current_step_index >= max_steps_value:
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_MM_BLOCKED"
            result["cycle_executor_status"] = "CYCLE_STOPPED_MAX_STEPS"
            result["recovery_status"] = "RECOVERY_STOPPED_CLOSED" if has_checkpoint else result["recovery_status"]
            result["max_steps_reached"] = True
            result["reason"] = "max_steps_reached"
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            return result
        if max_steps_value is not None and max_steps_value >= 0 and not cycle_id:
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_MM_BLOCKED"
            result["cycle_executor_status"] = "CYCLE_STOPPED_MAX_STEPS"
            result["recovery_status"] = "RECOVERY_STOPPED_CLOSED" if has_checkpoint else result["recovery_status"]
            result["max_steps_reached"] = True
            result["reason"] = "max_steps_requires_cycle_id"
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            return result

        if decision.money_management_status != "MM_CONTINUE_ALLOWED":
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_MM_BLOCKED"
            if decision.money_management_status == "MM_STOP_TARGET_REACHED":
                result["cycle_executor_status"] = "CYCLE_STOPPED_TARGET_REACHED"
                result["recovery_status"] = "RECOVERY_STOPPED_TARGET_REACHED" if has_checkpoint else result["recovery_status"]
            elif decision.money_management_status == "MM_STOP_CYCLE_CLOSED":
                result["cycle_executor_status"] = "CYCLE_STOPPED_CLOSED"
                result["recovery_status"] = "RECOVERY_STOPPED_CLOSED" if has_checkpoint else result["recovery_status"]
            else:
                result["cycle_executor_status"] = "CYCLE_SKIPPED_MM_BLOCKED"
                result["recovery_status"] = "RECOVERY_SKIPPED_MM_BLOCKED" if has_checkpoint else result["recovery_status"]
            result["reason"] = str(decision.stop_reason or "mm_blocked")
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            return result

        if not math.isfinite(result["next_stake"]) or result["next_stake"] <= 0.0:
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_INVALID_STAKE"
            result["money_management_status"] = "MM_STOP_INVALID_STAKE"
            result["cycle_executor_status"] = "CYCLE_SKIPPED_INVALID_STAKE"
            result["recovery_status"] = "RECOVERY_SKIPPED_INVALID_STAKE" if has_checkpoint else result["recovery_status"]
            result["reason"] = "invalid_next_stake"
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            return result

        risk_allowed, risk_reason = self._risk_allows_auto_trade()
        result["risk_status"] = "RISK_APPROVED" if risk_allowed else "RISK_REJECTED"
        if not risk_allowed:
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_RISK_REJECTED"
            result["cycle_executor_status"] = "CYCLE_SKIPPED_RISK_REJECTED"
            result["recovery_status"] = "RECOVERY_SKIPPED_RISK_REJECTED" if has_checkpoint else result["recovery_status"]
            result["reason"] = risk_reason
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            return result

        if self._has_open_trade_conflict(decision.table_id):
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_EXISTING_INFLIGHT"
            result["cycle_executor_status"] = "CYCLE_SKIPPED_EXISTING_INFLIGHT"
            result["recovery_status"] = "RECOVERY_SKIPPED_EXISTING_INFLIGHT" if has_checkpoint else result["recovery_status"]
            result["reason"] = "existing_inflight_trade"
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            return result

        try:
            self._persist_cycle_checkpoint(
                settlement_key=settlement_key,
                payload=payload,
                checkpoint_stage="NEXT_TRADE_SUBMIT_ATTEMPTED",
                bankroll_sync_status=result["bankroll_sync_status"],
                money_management_status=result["money_management_status"],
                cycle_active=result["cycle_active"],
                progression_allowed=result["progression_allowed"],
                next_stake=result["next_stake"],
                step_index=result["cycle_step_index"],
                next_trade_submission_status="ATTEMPTED",
                reason="submit_attempt",
                recovery_status=result["recovery_status"],
            )
            submit_payload = self._build_auto_trade_payload(signal=signal or {}, decision_stake=result["next_stake"])
        except Exception as exc:
            result["auto_trade_status"] = "AUTO_TRADE_SUBMIT_FAILED"
            result["cycle_executor_status"] = "CYCLE_SUBMIT_FAILED"
            result["recovery_status"] = "RECOVERY_SUBMIT_FAILED" if has_checkpoint else result["recovery_status"]
            result["reason"] = f"submit_payload_invalid:{type(exc).__name__}"
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            self._persist_cycle_checkpoint(
                settlement_key=settlement_key,
                payload=payload,
                checkpoint_stage="CYCLE_AMBIGUOUS",
                bankroll_sync_status=result["bankroll_sync_status"],
                money_management_status=result["money_management_status"],
                cycle_active=result["cycle_active"],
                progression_allowed=result["progression_allowed"],
                next_stake=result["next_stake"],
                step_index=result["cycle_step_index"],
                next_trade_submission_status="AMBIGUOUS",
                reason=result["reason"],
                is_ambiguous=True,
                recovery_status="RECOVERY_STATE_AMBIGUOUS",
            )
            return result

        table_id = submit_payload.get("table_id", decision.table_id)
        if table_id is not None:
            self.table_manager.activate(
                table_id=int(table_id),
                event_key=str(submit_payload.get("event_key") or ""),
                exposure=float(result["next_stake"]),
                market_id=str(submit_payload.get("market_id") or ""),
                selection_id=submit_payload.get("selection_id"),
                meta={
                    "event_name": submit_payload.get("event_name") or "",
                    "market_name": submit_payload.get("market_name") or "",
                    "runner_name": submit_payload.get("runner_name") or "",
                    "bet_type": submit_payload.get("bet_type") or "",
                    "price": float(submit_payload.get("price") or 0.0),
                    "simulation_mode": bool(submit_payload.get("simulation_mode", self.simulation_mode)),
                    "auto_trade_source": submit_payload.get("auto_trade_source") or "",
                },
            )

        self.bus.publish("CMD_QUICK_BET", submit_payload)
        result["auto_trade_status"] = "AUTO_TRADE_SUBMITTED"
        result["cycle_executor_status"] = "CYCLE_STEP_SUBMITTED"
        result["recovery_status"] = "RECOVERY_STEP_SUBMITTED" if has_checkpoint else result["recovery_status"]
        result["submitted"] = True
        result["reason"] = "submitted"
        if cycle_id:
            self._cycle_step_counts[cycle_id] = current_step_index + 1
            result["cycle_step_index"] = self._cycle_step_counts[cycle_id]
        if settlement_key:
            self._processed_auto_trade_keys.add(settlement_key)
        self._persist_cycle_checkpoint(
            settlement_key=settlement_key,
            payload=payload,
            checkpoint_stage="NEXT_TRADE_SUBMIT_CONFIRMED",
            bankroll_sync_status=result["bankroll_sync_status"],
            money_management_status=result["money_management_status"],
            cycle_active=result["cycle_active"],
            progression_allowed=result["progression_allowed"],
            next_stake=result["next_stake"],
            step_index=result["cycle_step_index"],
            next_trade_submission_status="SUBMITTED",
            reason=result["reason"],
            recovery_status=result["recovery_status"],
        )
        return result

    def _read_cycle_recovery_state(self, settlement_key: str) -> dict[str, Any]:
        default = {"status": "RECOVERY_NO_STATE", "state": {}}
        key = str(settlement_key or "").strip()
        if not key:
            return default
        getter = getattr(self.db, "get_cycle_recovery_state", None)
        if not callable(getter):
            return default
        try:
            state = getter(key) or {}
            if not isinstance(state, dict):
                return {"status": "RECOVERY_STATE_INVALID", "state": {}}
            if not bool(state.get("exists")):
                return default
            if bool(state.get("ambiguous")):
                return {"status": "RECOVERY_STATE_AMBIGUOUS", "state": state}
            if bool(state.get("processed")):
                return {"status": "RECOVERY_SKIPPED_DUPLICATE", "state": state}
            return {"status": "RECOVERY_STATE_LOADED", "state": state}
        except Exception:
            logger.exception("Errore read cycle recovery state")
            return {"status": "RECOVERY_STATE_INVALID", "state": {}}

    @staticmethod
    def _should_fail_closed_on_recovery(probe: dict[str, Any]) -> bool:
        status = str((probe or {}).get("status") or "")
        if status in {"RECOVERY_STATE_AMBIGUOUS", "RECOVERY_STATE_INVALID"}:
            return True
        state = (probe or {}).get("state") or {}
        return bool(state.get("ambiguous"))

    def _build_fail_closed_recovery_result(self, *, payload: dict, probe: dict[str, Any]) -> dict[str, Any]:
        settlement_key = self._build_bankroll_sync_key(payload)
        reason = "recovery_state_ambiguous"
        status = str(probe.get("status") or "RECOVERY_STATE_AMBIGUOUS")
        if status == "RECOVERY_STATE_INVALID":
            reason = "recovery_state_invalid"
        self._persist_cycle_checkpoint(
            settlement_key=settlement_key,
            payload=payload,
            checkpoint_stage="CYCLE_AMBIGUOUS",
            bankroll_sync_status="NOT_SETTLED",
            money_management_status="MM_STOP_CONTEXT_MISSING",
            next_trade_submission_status="AMBIGUOUS",
            reason=reason,
            is_ambiguous=True,
            recovery_status=status,
        )
        return {
            "correlation_id": f"auto-next::{str(payload.get('correlation_id') or '')}",
            "source_settlement_correlation_id": str(payload.get("correlation_id") or ""),
            "cycle_executor_enabled": bool(payload.get("cycle_executor_enabled", False)),
            "cycle_step_index": 0,
            "max_steps_reached": False,
            "kill_switch_active": bool(self._is_kill_switch_active()),
            "anomaly_pause_active": False,
            "cycle_executor_status": "CYCLE_AMBIGUOUS",
            "bankroll_sync_status": "SYNC_SKIPPED_DUPLICATE",
            "money_management_status": "MM_STOP_CONTEXT_MISSING",
            "cycle_active": False,
            "progression_allowed": False,
            "auto_trade_enabled": bool(payload.get("auto_trade_enabled", False)),
            "auto_trade_status": "AUTO_TRADE_SKIPPED_DUPLICATE",
            "next_stake": 0.0,
            "risk_status": "RISK_NOT_EVALUATED",
            "submitted": False,
            "reason": reason,
            "recovery_status": status,
        }

    def _persist_cycle_checkpoint(
        self,
        *,
        settlement_key: str,
        payload: dict,
        checkpoint_stage: str,
        bankroll_sync_status: str = "NOT_SETTLED",
        money_management_status: str = "MM_STOP_CONTEXT_MISSING",
        cycle_active: bool = False,
        progression_allowed: bool = False,
        next_stake: float = 0.0,
        step_index: int = 0,
        round_index: int = 0,
        next_trade_submission_status: str = "NOT_ATTEMPTED",
        reason: str = "",
        is_ambiguous: bool = False,
        recovery_status: str = "RECOVERY_STATE_LOADED",
    ) -> None:
        key = str(settlement_key or "").strip()
        if not key:
            return
        writer = getattr(self.db, "upsert_cycle_recovery_checkpoint", None)
        if not callable(writer):
            return
        existing: dict[str, Any] = {}
        existing_getter = getattr(self.db, "get_cycle_recovery_checkpoint", None)
        if callable(existing_getter):
            try:
                loaded = existing_getter(key)
                if isinstance(loaded, dict):
                    existing = dict(loaded)
            except Exception:
                logger.exception("Errore read existing checkpoint settlement_key=%s", key)

        stage_rank = {
            "SETTLEMENT_DETECTED": 10,
            "BANKROLL_SYNC_DONE": 20,
            "MM_DECISION_DONE": 30,
            "NEXT_TRADE_SUBMIT_ATTEMPTED": 40,
            "NEXT_TRADE_SUBMIT_CONFIRMED": 50,
            "CYCLE_BLOCKED": 60,
            "CYCLE_AMBIGUOUS": 70,
        }
        submit_rank = {
            "NOT_ATTEMPTED": 10,
            "ATTEMPTED": 20,
            "SUBMITTED": 30,
            "CONFIRMED": 40,
            "AMBIGUOUS": 50,
        }
        mm_context = payload.get("mm_context") if isinstance(payload, dict) else None
        cycle_id = str(mm_context.get("cycle_id") or "") if isinstance(mm_context, dict) else ""
        incoming_stage = str(checkpoint_stage or "SETTLEMENT_DETECTED")
        existing_stage = str(existing.get("checkpoint_stage") or "")
        effective_stage = incoming_stage
        if stage_rank.get(existing_stage, 0) > stage_rank.get(incoming_stage, 0):
            effective_stage = existing_stage

        incoming_submit = str(next_trade_submission_status or "NOT_ATTEMPTED")
        existing_submit = str(existing.get("next_trade_submission_status") or "")
        effective_submit = incoming_submit
        if submit_rank.get(existing_submit, 0) > submit_rank.get(incoming_submit, 0):
            effective_submit = existing_submit

        effective_reason = str(reason or existing.get("reason") or "")
        effective_ambiguous = bool(is_ambiguous) or bool(existing.get("is_ambiguous", False))
        record = {
            "settlement_correlation_id": str(payload.get("correlation_id") or payload.get("event_key") or ""),
            "cycle_id": cycle_id,
            "table_id": payload.get("table_id"),
            "strategy_context": {"auto_trade_source": "settlement_mm_gate", "recovery_status": recovery_status},
            "checkpoint_stage": effective_stage,
            "bankroll_sync_status": str(bankroll_sync_status or "NOT_SETTLED"),
            "money_management_status": str(money_management_status or "MM_STOP_CONTEXT_MISSING"),
            "cycle_active": bool(cycle_active),
            "progression_allowed": bool(progression_allowed),
            "next_stake": float(next_stake or 0.0),
            "step_index": int(step_index or 0),
            "round_index": int(round_index or 0),
            "next_trade_submission_status": effective_submit,
            "idempotency_key": key,
            "reason": effective_reason,
            "is_ambiguous": effective_ambiguous,
        }
        try:
            writer(key, record)
        except Exception:
            logger.exception("Errore persist checkpoint settlement_key=%s", key)

    def _risk_allows_auto_trade(self) -> tuple[bool, str]:
        if not self._runtime_active():
            return False, "runtime_not_active"
        if self._desk_mode() == DeskMode.LOCKDOWN:
            return False, "desk_lockdown"
        return True, "risk_approved"

    def _has_open_trade_conflict(self, table_id: Optional[int]) -> bool:
        if table_id is None:
            return False
        table = self.table_manager.get_table(int(table_id))
        if table is None:
            return False
        return bool(table.current_event_key)

    def _build_auto_trade_payload(self, *, signal: dict, decision_stake: float) -> dict:
        payload = {
            "market_id": str(signal.get("market_id")),
            "selection_id": int(signal.get("selection_id")),
            "bet_type": str(
                signal.get("bet_type")
                or signal.get("side")
                or signal.get("action")
                or "BACK"
            ).upper(),
            "price": float(signal.get("price") or signal.get("odds")),
            "stake": float(decision_stake),
            "event_name": signal.get("event") or signal.get("match") or signal.get("event_name") or "",
            "market_name": signal.get("market") or signal.get("market_name") or signal.get("market_type") or "",
            "runner_name": signal.get("selection") or signal.get("runner_name") or signal.get("runnerName") or "",
            "simulation_mode": bool(signal.get("simulation_mode", self.simulation_mode)),
            "event_key": self.duplication_guard.build_event_key(signal),
            "batch_id": str(signal.get("batch_id") or ""),
            "auto_trade_source": "settlement_mm_gate",
        }
        table_id = signal.get("table_id")
        if table_id is not None:
            payload["table_id"] = int(table_id)
        return payload

    # =========================================================
    # STATUS
    # =========================================================
    def get_status(self) -> dict:
        started_at = time.monotonic()
        try:
            funds = self.betfair_service.get_account_funds()
            self._record_runtime_io(operation="betfair_get_account_funds", started_at=started_at, ok=True)
        except Exception as exc:
            self._record_runtime_io(
                operation="betfair_get_account_funds",
                started_at=started_at,
                ok=False,
                error=str(exc),
            )
            raise
        bankroll_current = float(
            funds.get("available", self.risk_desk.bankroll_current)
            or self.risk_desk.bankroll_current
        )

        if bankroll_current != float(self.risk_desk.bankroll_current):
            self.risk_desk.sync_bankroll(bankroll_current)

        snapshot = self.risk_desk.build_snapshot(
            runtime_mode=self.mode,
            desk_mode=self._desk_mode(),
            total_exposure=self.table_manager.total_exposure(),
            telegram_connected=bool(self.telegram_service.status().get("connected")),
            betfair_connected=bool(self.betfair_service.status().get("connected")),
            active_tables=len(self.table_manager.active_tables()),
            recovery_tables=len(self.table_manager.recovery_tables()),
            last_error=self.last_error,
            last_signal_at=self.last_signal_at,
        )

        data = self.risk_desk.as_dict(snapshot)
        data["tables"] = self.table_manager.snapshot()
        data["duplication_guard"] = self.duplication_guard.snapshot()
        data["simulation_mode"] = bool(self.simulation_mode)
        data["execution_mode"] = str(self.execution_mode)
        data["live_enabled"] = bool(self.live_enabled)
        data["live_readiness_ok"] = bool(self.live_readiness_ok)
        data["kill_switch_active"] = bool(self._is_kill_switch_active())
        data["execution_gate_reason"] = str(self.last_execution_gate_reason)
        data["deploy_gate"] = dict(self.last_deploy_gate_status or {})
        data["runtime_io"] = self.runtime_io_snapshot()
        data["broker_status"] = self.betfair_service.status()
        data["account_funds"] = funds
        data["bankroll_sync"] = dict(self._last_bankroll_sync_result or {})
        data["auto_trade_mm"] = dict(self._last_auto_trade_result or {})
        data["cycle_executor"] = dict(self._last_cycle_executor_result or {})
        if self.streaming_feed is not None:
            try:
                data["streaming_feed"] = self.streaming_feed.status()
            except Exception:
                data["streaming_feed"] = {"running": False, "error": "status_unavailable"}
        else:
            data["streaming_feed"] = {"running": False}

        if self.simulation_mode and hasattr(self.betfair_service, "simulation_snapshot"):
            try:
                data["simulation_snapshot"] = self.betfair_service.simulation_snapshot()
            except Exception:
                logger.exception("Errore simulation_snapshot")
                data["simulation_snapshot"] = {}

        return data
