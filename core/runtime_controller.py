from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from core.duplication_guard import DuplicationGuard
from core.dutching_batch_manager import DutchingBatchManager
from core.money_management import RoserpinaMoneyManagement
from core.reconciliation_engine import ReconciliationEngine
from core.risk_desk import RiskDesk
from core.system_state import DeskMode, RuntimeMode
from core.table_manager import TableManager

# 🔥 AGGIUNTE OBBLIGATORIE
from core.pnl_engine import PnLEngine
from core.market_tracker import MarketTracker

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

        # =========================================================
        # CONFIG
        # =========================================================
        self.config = self.settings_service.load_roserpina_config()

        # =========================================================
        # CORE COMPONENTS
        # =========================================================
        self.table_manager = TableManager(table_count=self.config.table_count)

        # 🔥 importantissimo: reset ad ogni start runtime
        self.duplication_guard = DuplicationGuard()

        self.risk_desk = RiskDesk()
        self.mm = RoserpinaMoneyManagement(self.config)

        self.batch_manager = DutchingBatchManager(db, bus=bus)

        self.reconciliation_engine = ReconciliationEngine(
            db=db,
            bus=bus,
            batch_manager=self.batch_manager,
            betfair_service=betfair_service,
            table_manager=self.table_manager,
            duplication_guard=self.duplication_guard,
        )

        # =========================================================
        # 🔥 NUOVI COMPONENTI
        # =========================================================
        self.pnl_engine = PnLEngine(bus=self.bus)

        self.market_tracker = MarketTracker(
            bus=self.bus,
            betfair_service=self.betfair_service,
        )

        # =========================================================
        # STATE
        # =========================================================
        self.mode = RuntimeMode.STOPPED
        self.last_error = ""
        self.last_signal_at = ""

        # =========================================================
        # EVENT BUS
        # =========================================================
        self.bus.subscribe("SIGNAL_RECEIVED", self._on_signal_received)
        self.bus.subscribe("QUICK_BET_FAILED", self._on_quick_bet_failed)
        self.bus.subscribe("QUICK_BET_ACCEPTED", self._on_quick_bet_accepted)
        self.bus.subscribe("QUICK_BET_PARTIAL", self._on_quick_bet_partial)
        self.bus.subscribe("QUICK_BET_FILLED", self._on_quick_bet_filled)
        self.bus.subscribe("QUICK_BET_ROLLBACK_DONE", self._on_quick_bet_rollback_done)
        self.bus.subscribe("RUNTIME_CLOSE_POSITION", self._on_close_position)

    # =========================================================
    # CONFIG
    # =========================================================
    def reload_config(self) -> None:
        self.config = self.settings_service.load_roserpina_config()
        self.mm = RoserpinaMoneyManagement(self.config)
        self.table_manager = TableManager(table_count=self.config.table_count)

    # =========================================================
    # MODES
    # =========================================================
    def _desk_mode(self) -> DeskMode:
        return self.mm.determine_desk_mode(
            bankroll_current=self.risk_desk.bankroll_current,
            equity_peak=self.risk_desk.equity_peak,
        )

    def _runtime_active(self) -> bool:
        return self.mode == RuntimeMode.ACTIVE

    # =========================================================
    # CONTROL
    # =========================================================
    def start(self, password: Optional[str] = None) -> dict:
        self.reload_config()

        # 🔥 reset duplication guard ad ogni start
        self.duplication_guard = DuplicationGuard()

        session = self.betfair_service.connect(password=password)
        funds = self.betfair_service.get_account_funds()

        self.risk_desk.sync_bankroll(float(funds.get("available", 0.0) or 0.0))

        self.telegram_service.start()
        self.reconciliation_engine.reconcile_all_open_batches()

        self.mode = RuntimeMode.ACTIVE
        self.last_error = ""

        self.bus.publish("RUNTIME_STARTED", self.get_status())

        return {
            "started": True,
            "betfair": session,
            "funds": funds,
            "status": self.get_status(),
        }

    def stop(self) -> dict:
        self.telegram_service.stop()
        self.betfair_service.disconnect()

        self.mode = RuntimeMode.STOPPED

        self.bus.publish("RUNTIME_STOPPED", self.get_status())

        return {"stopped": True, "status": self.get_status()}

    def pause(self) -> dict:
        self.mode = RuntimeMode.PAUSED
        self.bus.publish("RUNTIME_PAUSED", self.get_status())
        return {"paused": True, "status": self.get_status()}

    def resume(self) -> dict:
        if self.mode == RuntimeMode.LOCKDOWN:
            return {"resumed": False, "reason": "lockdown_attivo"}

        self.mode = RuntimeMode.ACTIVE
        self.bus.publish("RUNTIME_RESUMED", self.get_status())
        return {"resumed": True, "status": self.get_status()}

    # =========================================================
    # SIGNAL FLOW
    # =========================================================
    def _on_signal_received(self, signal: dict) -> None:
        self.last_signal_at = datetime.utcnow().isoformat()

        if not self._runtime_active():
            return

        event_key = self.duplication_guard.build_event_key(signal)

        if self.duplication_guard.is_duplicate(event_key):
            return

        table = self.table_manager.allocate(event_key=event_key)
        if table is None:
            return

        decision = self.mm.calculate(
            signal=signal,
            bankroll_current=self.risk_desk.bankroll_current,
            equity_peak=self.risk_desk.equity_peak,
            current_total_exposure=self.table_manager.total_exposure(),
            event_current_exposure=0.0,
            table=table,
        )

        if not decision.approved:
            return

        payload = {
            "market_id": signal.get("market_id"),
            "selection_id": signal.get("selection_id"),
            "bet_type": signal.get("bet_type", "BACK"),
            "price": signal.get("price"),
            "stake": decision.recommended_stake,
            "event_key": event_key,
            "table_id": decision.table_id,
        }

        self.table_manager.activate(
            table_id=decision.table_id,
            event_key=event_key,
            exposure=decision.recommended_stake,
        )

        self.duplication_guard.register(event_key)

        self.bus.publish("CMD_QUICK_BET", payload)

    # =========================================================
    # BET EVENTS
    # =========================================================
    def _on_quick_bet_failed(self, payload: dict) -> None:
        self._release(payload)

    def _on_quick_bet_accepted(self, payload: dict) -> None:
        pass

    def _on_quick_bet_partial(self, payload: dict) -> None:
        pass

    def _on_quick_bet_filled(self, payload: dict) -> None:
        pass

    def _on_quick_bet_rollback_done(self, payload: dict) -> None:
        self._release(payload)

    # =========================================================
    # CLOSE POSITION (PnL Engine)
    # =========================================================
    def _on_close_position(self, payload: dict) -> None:
        table_id = payload.get("table_id")
        pnl = float(payload.get("pnl", 0.0) or 0.0)
        event_key = payload.get("event_key")

        if table_id is not None:
            self.table_manager.release(int(table_id), pnl=pnl)

        if event_key:
            self.duplication_guard.release(event_key)

        self.risk_desk.apply_closed_pnl(pnl)

    # =========================================================
    # HELPERS
    # =========================================================
    def _release(self, payload: dict) -> None:
        event_key = payload.get("event_key")
        table_id = payload.get("table_id")

        if event_key:
            self.duplication_guard.release(event_key)

        if table_id is not None:
            self.table_manager.force_unlock(int(table_id))

    # =========================================================
    # STATUS
    # =========================================================
    def get_status(self) -> dict:
        return {
            "mode": self.mode.value,
            "bankroll_current": self.risk_desk.bankroll_current,
            "drawdown_pct": self.risk_desk.drawdown_pct(),
            "total_exposure": self.table_manager.total_exposure(),
            "active_tables": len(self.table_manager.active_tables()),
            "last_error": self.last_error,
            "last_signal_at": self.last_signal_at,
            "tables": self.table_manager.snapshot(),
        }