from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from core.duplication_guard import DuplicationGuard
from core.money_management import RoserpinaMoneyManagement
from core.risk_desk import RiskDesk
from core.system_state import DeskMode, RuntimeMode
from core.table_manager import TableManager

logger = logging.getLogger(__name__)


class RuntimeController:
    def __init__(
        self,
        *,
        bus,
        db,
        settings_service,
        betfair_service,
        telegram_service,
        trading_engine=None,
    ):
        self.bus = bus
        self.db = db
        self.settings_service = settings_service
        self.betfair_service = betfair_service
        self.telegram_service = telegram_service
        self.trading_engine = trading_engine

        self.config = self.settings_service.load_roserpina_config()
        self.table_manager = TableManager(table_count=self.config.table_count)
        self.duplication_guard = DuplicationGuard()
        self.risk_desk = RiskDesk()
        self.mm = RoserpinaMoneyManagement(self.config)

        self.mode = RuntimeMode.STOPPED
        self.last_error = ""
        self.last_signal_at = ""

        self.bus.subscribe("SIGNAL_RECEIVED", self._on_signal_received)
        self.bus.subscribe("QUICK_BET_FAILED", self._on_quick_bet_failed)
        self.bus.subscribe("RUNTIME_CLOSE_POSITION", self._on_close_position)

    def _desk_mode(self) -> DeskMode:
        return self.mm.determine_desk_mode(
            bankroll_current=self.risk_desk.bankroll_current,
            equity_peak=self.risk_desk.equity_peak,
        )

    def reload_config(self) -> None:
        self.config = self.settings_service.load_roserpina_config()
        self.mm = RoserpinaMoneyManagement(self.config)
        self.table_manager = TableManager(table_count=self.config.table_count)

    def start(self, password: str | None = None) -> dict:
        self.reload_config()

        session = self.betfair_service.connect(password=password)
        funds = self.betfair_service.get_account_funds()
        self.risk_desk.sync_bankroll(float(funds.get("available", 0.0) or 0.0))

        self.telegram_service.start()
        self.mode = RuntimeMode.ACTIVE
        self.last_error = ""

        self.bus.publish("RUNTIME_STARTED", self.get_status())
        return {
            "started": True,
            "betfair": session,
            "funds": funds,
            "status": self.get_status(),
        }

    def pause(self) -> dict:
        self.mode = RuntimeMode.PAUSED
        self.bus.publish("RUNTIME_PAUSED", self.get_status())
        return {"paused": True, "status": self.get_status()}

    def resume(self) -> dict:
        if self.mode == RuntimeMode.LOCKDOWN:
            return {"resumed": False, "reason": "lockdown_attivo", "status": self.get_status()}
        self.mode = RuntimeMode.ACTIVE
        self.bus.publish("RUNTIME_RESUMED", self.get_status())
        return {"resumed": True, "status": self.get_status()}

    def stop(self) -> dict:
        self.telegram_service.stop()
        self.betfair_service.disconnect()
        self.mode = RuntimeMode.STOPPED
        self.bus.publish("RUNTIME_STOPPED", self.get_status())
        return {"stopped": True, "status": self.get_status()}

    def force_lockdown(self, reason: str = "") -> dict:
        self.mode = RuntimeMode.LOCKDOWN
        self.last_error = reason or "LOCKDOWN manuale/automatico"
        self.bus.publish("RUNTIME_LOCKDOWN", self.get_status())
        return {"locked": True, "status": self.get_status()}

    def reset_cycle(self) -> dict:
        self.table_manager.reset_all()
        self.duplication_guard = DuplicationGuard()
        self.risk_desk.reset_recovery_cycle()
        self.bus.publish("RUNTIME_CYCLE_RESET", self.get_status())
        return {"reset": True, "status": self.get_status()}

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
        self.last_signal_at = datetime.utcnow().isoformat()

        if self.mode != RuntimeMode.ACTIVE:
            self._reject_signal(signal, f"runtime_non_attivo:{self.mode.value}")
            return

        required = ["market_id", "selection_id"]
        missing = [k for k in required if not signal.get(k)]
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
            "bet_type": str(signal.get("bet_type") or signal.get("side") or signal.get("action") or "BACK").upper(),
            "price": float(signal.get("price") or signal.get("odds")),
            "stake": float(decision.recommended_stake),
            "event_name": signal.get("event") or signal.get("match") or "",
            "market_name": signal.get("market") or signal.get("market_name") or signal.get("market_type") or "",
            "runner_name": signal.get("selection") or "",
            "simulation_mode": False,
            "event_key": event_key,
            "table_id": decision.table_id,
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

    def _on_quick_bet_failed(self, payload) -> None:
        event_key = ""
        table_id: Optional[int] = None

        if isinstance(payload, dict):
            event_key = str(payload.get("event_key") or "")
            table_id = payload.get("table_id")

        if event_key:
            self.duplication_guard.release(event_key)

        if table_id:
            try:
                self.table_manager.release(int(table_id), pnl=0.0)
            except Exception:
                pass

    def _on_close_position(self, payload: dict) -> None:
        if not isinstance(payload, dict):
            return

        table_id = payload.get("table_id")
        pnl = float(payload.get("pnl", 0.0) or 0.0)
        event_key = str(payload.get("event_key") or "")

        if table_id:
            self.table_manager.release(int(table_id), pnl=pnl)

        if event_key:
            self.duplication_guard.release(event_key)

        self.risk_desk.apply_closed_pnl(pnl)

        if self.risk_desk.drawdown_pct() >= self.config.auto_reset_drawdown_pct:
            self.table_manager.reset_all()
            self.duplication_guard = DuplicationGuard()
            self.risk_desk.reset_recovery_cycle()
            self.bus.publish(
                "ROSERPINA_AUTO_RESET",
                {
                    "reason": "drawdown_limit",
                    "drawdown_pct": self.risk_desk.drawdown_pct(),
                },
            )

        if self.risk_desk.drawdown_pct() >= self.config.lockdown_drawdown_pct:
            self.force_lockdown("Drawdown oltre soglia lockdown")

    def get_status(self) -> dict:
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
        return data
