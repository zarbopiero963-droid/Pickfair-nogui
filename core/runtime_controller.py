from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from core.duplication_guard import DuplicationGuard
from core.dutching_batch_manager import DutchingBatchManager
from core.market_tracker import MarketTracker
from core.money_management import RoserpinaMoneyManagement
from core.reconciliation_engine import ReconciliationEngine
from core.risk_desk import RiskDesk
from core.system_state import DeskMode, RuntimeMode
from core.table_manager import TableManager
from core.safety_layer import assert_live_gate_or_refuse
from order_manager import TERMINAL_LIFECYCLE_EVENTS

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

        self.mode = RuntimeMode.STOPPED
        self.last_error = ""
        self.last_signal_at = ""
        self.simulation_mode = False
        self._startup_in_progress = False
        self._startup_failed = False
        self.execution_mode = "SIMULATION"
        self.live_enabled = False
        self.live_readiness_ok = False
        self.last_execution_gate_reason = "startup_default"

        self._subscribe_bus()

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
        if value is None:
            return bool(default)
        if isinstance(value, bool):
            return value
        txt = str(value).strip().lower()
        if txt in {"1", "true", "yes", "on"}:
            return True
        if txt in {"0", "false", "no", "off"}:
            return False
        return bool(default)

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

    def is_ready(self) -> bool:
        return not self._startup_in_progress and not self._startup_failed and self.mode != RuntimeMode.LOCKDOWN

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
        self._startup_in_progress = True
        self._startup_failed = False
        try:
            self.reload_config()

            # sincronizzazione da headless_main / mini_gui
            if simulation_mode is not None:
                self.set_simulation_mode(simulation_mode)
            else:
                self.set_simulation_mode(self.simulation_mode)
        requested_execution_mode = self._safe_execution_mode(execution_mode)
        if execution_mode is None and simulation_mode is not None:
            requested_execution_mode = "SIMULATION" if bool(simulation_mode) else "LIVE"

        requested_live_enabled = False
        if live_enabled is not None:
            requested_live_enabled = self._safe_bool(live_enabled, default=False)
        else:
            try:
                if hasattr(self.settings_service, "load_live_enabled"):
                    requested_live_enabled = bool(self.settings_service.load_live_enabled())
                else:
                    data = self.settings_service.get_all_settings()
                    requested_live_enabled = (
                        str(data.get("execution_mode", "SIMULATION")).strip().upper() == "LIVE"
                        or bool(data.get("live_enabled", False))
                    )
            except Exception:
                requested_live_enabled = False

        requested_readiness = self._derive_live_readiness_ok(live_readiness_ok)

        gate = assert_live_gate_or_refuse(
            execution_mode=requested_execution_mode,
            live_enabled=requested_live_enabled,
            live_readiness_ok=requested_readiness,
            kill_switch=self._is_kill_switch_active(),
        )

        self.execution_mode = gate.effective_execution_mode
        self.live_enabled = requested_live_enabled
        self.live_readiness_ok = requested_readiness
        self.last_execution_gate_reason = gate.reason_code

        if requested_execution_mode == "LIVE" and not gate.allowed:
            status = self.get_status()
            self.bus.publish(
                "LIVE_EXECUTION_REFUSED",
                {
                    "reason_code": gate.reason_code,
                    "message": gate.refusal_message,
                    "requested_execution_mode": requested_execution_mode,
                    "effective_execution_mode": gate.effective_execution_mode,
                },
            )
            return {
                "ok": False,
                "started": False,
                "refused": True,
                "reason": "live_not_enabled",
                "reason_code": gate.reason_code,
                "refusal_message": gate.refusal_message,
                "requested_execution_mode": requested_execution_mode,
                "effective_execution_mode": gate.effective_execution_mode,
                "status": status,
            }

        # sincronizzazione da headless_main / mini_gui
        self.set_simulation_mode(self.execution_mode != "LIVE")

            # reset anti-duplicazione a ogni start
            self.duplication_guard = DuplicationGuard()
            self.reconciliation_engine = self._build_reconciliation_engine()

            session = self.betfair_service.connect(
                password=password,
                simulation_mode=self.simulation_mode,
            )
            funds = self.betfair_service.get_account_funds()
            self.risk_desk.sync_bankroll(float(funds.get("available", 0.0) or 0.0))

            telegram_result = self.telegram_service.start()

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
        except Exception as exc:
            self._startup_failed = True
            self.last_error = str(exc)
            raise
        finally:
            self._startup_in_progress = False

    def stop(self) -> dict:
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
        signal = dict(signal or {})
        self.last_signal_at = datetime.utcnow().isoformat()

        if not self._runtime_active():
            self._reject_signal(signal, f"runtime_non_attivo:{self.mode.value}")
            return

        required = ["market_id", "selection_id"]
        missing = [k for k in required if signal.get(k) in (None, "")]
        if missing:
            self._reject_signal(signal, f"campi_mancanti:{','.join(missing)}")
            return

        event_key = self.duplication_guard.build_event_key(signal)
        signal["event_key"] = event_key

        if self.config.anti_duplication_enabled and self.duplication_guard.is_duplicate(event_key):
            self._reject_signal(signal, "duplicato_bloccato")
            return

        table = self.table_manager.allocate(
            event_key=event_key,
            allow_recovery=bool(self.config.allow_recovery),
        )
        if table is None:
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
        self.duplication_guard.register(event_key)

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
        funds = self.betfair_service.get_account_funds()
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
        data["broker_status"] = self.betfair_service.status()
        data["account_funds"] = funds

        if self.simulation_mode and hasattr(self.betfair_service, "simulation_snapshot"):
            try:
                data["simulation_snapshot"] = self.betfair_service.simulation_snapshot()
            except Exception:
                logger.exception("Errore simulation_snapshot")
                data["simulation_snapshot"] = {}

        return data

    def evaluate_live_readiness(self, *, runtime_probe=None, context: Optional[dict] = None) -> dict:
        context = dict(context or {})
        execution_mode = str(context.get("execution_mode") or "").strip().upper()
        password = context.get("password")

        blockers = []
        details = {
            "execution_mode": execution_mode or "UNKNOWN",
            "runtime_mode": getattr(self.mode, "value", str(self.mode)),
            "simulation_mode": bool(self.simulation_mode),
            "startup_in_progress": bool(self._startup_in_progress),
            "startup_failed": bool(self._startup_failed),
        }

        runtime_ready = not self._startup_in_progress and not self._startup_failed and self.mode != RuntimeMode.LOCKDOWN
        details["runtime_state_ok"] = runtime_ready
        if not runtime_ready:
            blockers.append("RUNTIME_NOT_INITIALIZED")

        kill_switch_active = self.mode == RuntimeMode.LOCKDOWN
        details["kill_switch_active"] = kill_switch_active
        if kill_switch_active:
            blockers.append("KILL_SWITCH_ACTIVE")

        safe_mode_active = False
        safe_mode_obj = getattr(getattr(runtime_probe, "safe_mode", None), "is_enabled", None)
        if callable(safe_mode_obj):
            try:
                safe_mode_active = bool(safe_mode_obj())
            except Exception:
                safe_mode_active = True
        policy = {}
        if self.settings_service is not None and hasattr(self.settings_service, "load_live_readiness_policy"):
            policy = dict(self.settings_service.load_live_readiness_policy() or {})
        safe_mode_blocks = bool(policy.get("safe_mode_blocks_live", True))
        details["safe_mode_active"] = safe_mode_active
        details["safe_mode_blocks_live"] = safe_mode_blocks
        if safe_mode_active and safe_mode_blocks:
            blockers.append("SAFE_MODE_BLOCKING")

        live_dependency_ok = False
        if self.betfair_service is not None and not bool(self.simulation_mode):
            has_connect = callable(getattr(self.betfair_service, "connect", None))
            cfg_ok = False
            if self.settings_service is not None and hasattr(self.settings_service, "has_live_credentials_configured"):
                cfg_ok = bool(self.settings_service.has_live_credentials_configured())
            pwd = password if password is not None else (
                self.settings_service.load_password() if self.settings_service and hasattr(self.settings_service, "load_password") else ""
            )
            pwd_ok = bool(str(pwd or "").strip())
            details["live_configured"] = cfg_ok
            details["live_password_available"] = pwd_ok
            live_dependency_ok = has_connect and cfg_ok and pwd_ok
        details["live_dependency_ok"] = live_dependency_ok
        if not live_dependency_ok:
            blockers.append("LIVE_DEPENDENCY_MISSING")

        probe_health = None
        probe_ok = False
        if runtime_probe is not None and hasattr(runtime_probe, "collect_health"):
            try:
                probe_health = runtime_probe.collect_health()
                unknown = any((v or {}).get("status") == "UNKNOWN" for v in (probe_health or {}).values())
                probe_ok = isinstance(probe_health, dict) and bool(probe_health) and not unknown
                details["probe_unknown_count"] = sum(1 for v in probe_health.values() if (v or {}).get("status") == "UNKNOWN")
            except Exception as exc:
                details["probe_error"] = str(exc)
                probe_ok = False
        details["probe_ok"] = probe_ok
        if not probe_ok:
            blockers.append("READINESS_SIGNAL_UNKNOWN")

        execution_mode_valid = execution_mode == "LIVE"
        details["execution_mode_valid_for_live"] = execution_mode_valid
        if not execution_mode_valid:
            blockers.append("INVALID_EXECUTION_MODE")

        ready = len(blockers) == 0
        level = "READY" if ready else "NOT_READY"
        return {
            "ready": ready,
            "level": level,
            "blockers": blockers,
            "details": details,
        }

    def get_live_readiness_report(self, *, runtime_probe=None, context: Optional[dict] = None) -> dict:
        return self.evaluate_live_readiness(runtime_probe=runtime_probe, context=context)

    def is_live_readiness_ok(self, *, runtime_probe=None, context: Optional[dict] = None) -> bool:
        report = self.get_live_readiness_report(runtime_probe=runtime_probe, context=context)
        return bool(report.get("ready", False))
