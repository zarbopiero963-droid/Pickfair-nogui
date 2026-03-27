from __future__ import annotations

import logging
import signal
import sys
import time
from typing import Optional

from database import Database
from event_bus import EventBus
from executor_manager import ExecutorManager
from shutdown_manager import ShutdownManager

from services.setting_service import SettingsService
from services.betfair_service import BetfairService
from services.telegram_service import TelegramService

from core.trading_engine import TradingEngine
from core.runtime_controller import RuntimeController


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


class HeadlessApp:
    """
    Bootstrap headless Pickfair.

    Responsabilità:
    - inizializza core/services/runtime
    - avvia runtime live o simulation
    - gestisce shutdown pulito
    - resta in loop senza GUI
    """

    def __init__(self):
        self.db: Optional[Database] = None
        self.bus: Optional[EventBus] = None
        self.executor: Optional[ExecutorManager] = None
        self.shutdown: Optional[ShutdownManager] = None

        self.settings_service: Optional[SettingsService] = None
        self.betfair_service: Optional[BetfairService] = None
        self.telegram_service: Optional[TelegramService] = None

        self.trading_engine: Optional[TradingEngine] = None
        self.runtime: Optional[RuntimeController] = None

        self._running = False

    # =========================================================
    # BOOTSTRAP
    # =========================================================
    def build(self) -> None:
        self.db = Database()
        self.bus = EventBus()
        self.executor = ExecutorManager(max_workers=4, default_timeout=30)
        self.shutdown = ShutdownManager()

        self.settings_service = SettingsService(self.db)
        self.betfair_service = BetfairService(self.settings_service)
        self.telegram_service = TelegramService(
            self.settings_service,
            self.db,
            self.bus,
        )

        self.trading_engine = TradingEngine(
            bus=self.bus,
            db=self.db,
            client_getter=self.betfair_service.get_client,
            executor=self.executor,
        )

        self.runtime = RuntimeController(
            bus=self.bus,
            db=self.db,
            settings_service=self.settings_service,
            betfair_service=self.betfair_service,
            telegram_service=self.telegram_service,
            trading_engine=self.trading_engine,
            executor=self.executor,
        )

        self._wire_bus()
        self._register_shutdown_hooks()

    def _register_shutdown_hooks(self) -> None:
        self._register_shutdown_hook(
            "telegram_stop",
            self.telegram_service.stop,
            priority=10,
        )
        self._register_shutdown_hook(
            "betfair_disconnect",
            self.betfair_service.disconnect,
            priority=20,
        )
        self._register_shutdown_hook(
            "db_close",
            self.db.close_all_connections,
            priority=30,
        )
        self._register_shutdown_hook(
            "executor_shutdown",
            self.executor.shutdown,
            priority=40,
        )

    def _register_shutdown_hook(self, name, fn, priority=100):
        if hasattr(self.shutdown, "register"):
            try:
                self.shutdown.register(name, fn, priority=priority)
                return
            except TypeError:
                try:
                    self.shutdown.register(name, fn)
                    return
                except TypeError:
                    pass

        if hasattr(self.shutdown, "register_shutdown_hook"):
            try:
                self.shutdown.register_shutdown_hook(name, fn, priority=priority)
                return
            except TypeError:
                try:
                    self.shutdown.register_shutdown_hook(fn)
                    return
                except TypeError:
                    pass

    def _wire_bus(self) -> None:
        self.bus.subscribe("RUNTIME_STARTED", self._on_runtime_started)
        self.bus.subscribe("RUNTIME_PAUSED", self._on_runtime_paused)
        self.bus.subscribe("RUNTIME_RESUMED", self._on_runtime_resumed)
        self.bus.subscribe("RUNTIME_STOPPED", self._on_runtime_stopped)
        self.bus.subscribe("RUNTIME_LOCKDOWN", self._on_runtime_lockdown)

        self.bus.subscribe("TELEGRAM_STATUS", self._on_telegram_status)
        self.bus.subscribe("SIGNAL_RECEIVED", self._on_signal_received)
        self.bus.subscribe("SIGNAL_APPROVED", self._on_signal_approved)
        self.bus.subscribe("SIGNAL_REJECTED", self._on_signal_rejected)

        self.bus.subscribe("QUICK_BET_SUBMITTED", self._on_quick_bet_submitted)
        self.bus.subscribe("QUICK_BET_ACCEPTED", self._on_quick_bet_accepted)
        self.bus.subscribe("QUICK_BET_PARTIAL", self._on_quick_bet_partial)
        self.bus.subscribe("QUICK_BET_FILLED", self._on_quick_bet_filled)
        self.bus.subscribe("QUICK_BET_FAILED", self._on_quick_bet_failed)
        self.bus.subscribe("QUICK_BET_ROLLBACK_DONE", self._on_quick_bet_rollback_done)

    # =========================================================
    # EVENT LOGGING
    # =========================================================
    def _on_runtime_started(self, payload):
        logger.info("RUNTIME_STARTED -> %s", payload)

    def _on_runtime_paused(self, payload):
        logger.info("RUNTIME_PAUSED -> %s", payload)

    def _on_runtime_resumed(self, payload):
        logger.info("RUNTIME_RESUMED -> %s", payload)

    def _on_runtime_stopped(self, payload):
        logger.info("RUNTIME_STOPPED -> %s", payload)

    def _on_runtime_lockdown(self, payload):
        logger.warning("RUNTIME_LOCKDOWN -> %s", payload)

    def _on_telegram_status(self, payload):
        logger.info("TELEGRAM_STATUS -> %s", payload)

    def _on_signal_received(self, payload):
        logger.info("SIGNAL_RECEIVED -> %s", payload)

    def _on_signal_approved(self, payload):
        logger.info("SIGNAL_APPROVED -> %s", payload)

    def _on_signal_rejected(self, payload):
        logger.warning("SIGNAL_REJECTED -> %s", payload)

    def _on_quick_bet_submitted(self, payload):
        logger.info("QUICK_BET_SUBMITTED -> %s", payload)

    def _on_quick_bet_accepted(self, payload):
        logger.info("QUICK_BET_ACCEPTED -> %s", payload)

    def _on_quick_bet_partial(self, payload):
        logger.info("QUICK_BET_PARTIAL -> %s", payload)

    def _on_quick_bet_filled(self, payload):
        logger.info("QUICK_BET_FILLED -> %s", payload)

    def _on_quick_bet_failed(self, payload):
        logger.error("QUICK_BET_FAILED -> %s", payload)

    def _on_quick_bet_rollback_done(self, payload):
        logger.warning("QUICK_BET_ROLLBACK_DONE -> %s", payload)

    # =========================================================
    # ARGUMENTS
    # =========================================================
    def _parse_args(self) -> dict:
        args = [str(x).strip().lower() for x in sys.argv[1:]]

        simulation_mode = True
        if "--live" in args or "live" in args:
            simulation_mode = False
        elif "--simulation" in args or "simulation" in args or "--sim" in args or "sim" in args:
            simulation_mode = True
        else:
            try:
                sim_cfg = self.settings_service.load_simulation_config()
                simulation_mode = bool(sim_cfg.get("enabled", True))
            except Exception:
                simulation_mode = True

        password = None
        for item in sys.argv[1:]:
            raw = str(item)
            if raw.startswith("--password="):
                password = raw.split("=", 1)[1]
                break

        return {
            "simulation_mode": simulation_mode,
            "password": password,
        }

    # =========================================================
    # RUN
    # =========================================================
    def start(self) -> int:
        self.build()
        args = self._parse_args()

        simulation_mode = bool(args["simulation_mode"])
        password = args["password"]

        mode_txt = "SIMULATION" if simulation_mode else "LIVE"
        logger.info("Avvio runtime headless in modalità %s", mode_txt)

        try:
            result = self.runtime.start(
                password=password,
                simulation_mode=simulation_mode,
            )
            logger.info("Runtime avviato -> %s", result)
        except Exception as exc:
            logger.exception("Errore avvio runtime: %s", exc)
            return 1

        self._running = True
        self._install_signal_handlers()

        try:
            while self._running:
                time.sleep(1.0)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt ricevuto, shutdown...")
        finally:
            self.stop()

        return 0

    def stop(self) -> None:
        self._running = False

        try:
            if self.runtime is not None:
                try:
                    self.runtime.stop()
                except Exception:
                    logger.exception("Errore stop runtime")
        finally:
            try:
                if hasattr(self.shutdown, "shutdown"):
                    self.shutdown.shutdown()
                elif hasattr(self.shutdown, "run"):
                    self.shutdown.run()
            except Exception:
                logger.exception("Errore shutdown manager")

    # =========================================================
    # SIGNAL HANDLERS
    # =========================================================
    def _install_signal_handlers(self) -> None:
        def _handler(signum, frame):
            logger.info("Segnale ricevuto: %s", signum)
            self._running = False

        try:
            signal.signal(signal.SIGINT, _handler)
        except Exception:
            pass

        try:
            signal.signal(signal.SIGTERM, _handler)
        except Exception:
            pass


def main() -> int:
    try:
        app = HeadlessApp()
        return app.start()
    except Exception as exc:
        logger.exception("Errore fatale in headless_main: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())