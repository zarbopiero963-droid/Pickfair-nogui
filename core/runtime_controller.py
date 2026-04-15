from __future__ import annotations

import logging
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

        if not execution_mode_valid:
            blockers.append("INVALID_EXECUTION_MODE")
        if normalized_execution_mode == "LIVE" and not effective_live_enabled:
            blockers.append("LIVE_NOT_ENABLED")
        if normalized_execution_mode == "LIVE" and not configured_live_readiness_ok:
            blockers.append("LIVE_READINESS_FLAG_NOT_OK")
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

        copy_meta = signal.get("copy_meta")
        pattern_meta = signal.get("pattern_meta")
        if "copy_meta" in signal and copy_meta is not None and not isinstance(copy_meta, dict):
            self._reject_signal(signal, "copy_meta_invalid")
            return
        if "pattern_meta" in signal and pattern_meta is not None and not isinstance(pattern_meta, dict):
            self._reject_signal(signal, "pattern_meta_invalid")
            return
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
        if isinstance(copy_meta, dict):
            payload["copy_meta"] = dict(copy_meta)
        if isinstance(pattern_meta, dict):
            payload["pattern_meta"] = dict(pattern_meta)
        derived_origin = str(signal.get("order_origin") or "").strip().upper()
        if not derived_origin and isinstance(copy_meta, dict):
            derived_origin = "COPY"
        if not derived_origin and isinstance(pattern_meta, dict):
            derived_origin = "PATTERN"
        if derived_origin:
            payload["order_origin"] = derived_origin

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

        table_id = payload.get("table_id")
        pnl = float(payload.get("pnl", 0.0) or 0.0)
        event_key = str(payload.get("event_key") or "")
        batch_id = str(payload.get("batch_id") or "")

        if table_id is not None:
            self.table_manager.release(int(table_id), pnl=pnl)

        if event_key:
            self.duplication_guard.release(event_key)

        self.risk_desk.apply_closed_pnl(pnl)

        current_drawdown = self.risk_desk.drawdown_pct()

        if batch_id:
            self.bus.publish(
                "BATCH_POSITION_CLOSED",
                {
                    "batch_id": batch_id,
                    "pnl": pnl,
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
