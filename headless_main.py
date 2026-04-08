from __future__ import annotations

import logging
import signal
import sys
import time
from typing import Any, Optional

from database import Database
from core.event_bus import EventBus
from executor_manager import ExecutorManager
from shutdown_manager import ShutdownManager

from services.settings_service import SettingsService
from services.betfair_service import BetfairService
from services.telegram_alerts_service import TelegramAlertsService
from services.telegram_service import TelegramService

from core.trading_engine import TradingEngine
from core.runtime_controller import RuntimeController
from observability import (
    AlertsManager,
    DiagnosticsService,
    HealthRegistry,
    IncidentsManager,
    MetricsRegistry,
    RuntimeProbe,
    SnapshotService,
    WatchdogService,
)
from observability.cleanup_service import CleanupService
from observability.diagnostic_bundle_builder import DiagnosticBundleBuilder
from observability.retention_manager import RetentionManager
from safe_mode import get_safe_mode_manager


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


class HeadlessApp:
    """
    Bootstrap headless Pickfair.

    Perimetro di questo file:
    - wiring componenti
    - bootstrap robusto
    - start/stop idempotenti
    - recovery bootstrap trigger
    - cleanup sicuro su failure parziale
    - loop headless senza GUI

    Non decide la correttezza business di:
    - recovery reale degli ordini
    - reconciliation con exchange
    - money management
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
        self.health_registry: Optional[HealthRegistry] = None
        self.metrics_registry: Optional[MetricsRegistry] = None
        self.alerts_manager: Optional[AlertsManager] = None
        self.incidents_manager: Optional[IncidentsManager] = None
        self.runtime_probe: Optional[RuntimeProbe] = None
        self.snapshot_service: Optional[SnapshotService] = None
        self.watchdog_service: Optional[WatchdogService] = None
        self.diagnostics_service: Optional[DiagnosticsService] = None
        self.retention_manager: Optional[RetentionManager] = None
        self.cleanup_service: Optional[CleanupService] = None
        self.telegram_alerts_service: Optional[TelegramAlertsService] = None
        self.safe_mode = None

        self._running = False
        self._built = False
        self._signal_handlers_installed = False

    # =========================================================
    # INTERNAL STATE
    # =========================================================
    def _reset_runtime_refs(self) -> None:
        self.db = None
        self.bus = None
        self.executor = None
        self.shutdown = None

        self.settings_service = None
        self.betfair_service = None
        self.telegram_service = None

        self.trading_engine = None
        self.runtime = None
        self.health_registry = None
        self.metrics_registry = None
        self.alerts_manager = None
        self.incidents_manager = None
        self.runtime_probe = None
        self.snapshot_service = None
        self.watchdog_service = None
        self.diagnostics_service = None
        self.retention_manager = None
        self.cleanup_service = None
        self.telegram_alerts_service = None
        self.safe_mode = None

        self._built = False
        self._running = False

    def _cleanup_partial_build(self) -> None:
        """
        Cleanup difensivo se build() fallisce a metà.
        Sempre safe/idempotente.
        """
        try:
            if self.watchdog_service is not None:
                try:
                    self.watchdog_service.stop()
                except Exception:
                    logger.exception("Errore cleanup watchdog_service")
            if self.cleanup_service is not None:
                try:
                    self.cleanup_service.stop()
                except Exception:
                    logger.exception("Errore cleanup cleanup_service")
            if self.telegram_service is not None:
                try:
                    self.telegram_service.stop()
                except Exception:
                    logger.exception("Errore cleanup telegram_service")
        finally:
            try:
                if self.betfair_service is not None:
                    try:
                        self.betfair_service.disconnect()
                    except Exception:
                        logger.exception("Errore cleanup betfair_service")
            finally:
                try:
                    if self.executor is not None:
                        try:
                            self.executor.shutdown(wait=False, cancel_futures=True)
                        except TypeError:
                            try:
                                self.executor.shutdown(wait=False)
                            except Exception:
                                logger.exception("Errore cleanup executor")
                        except Exception:
                            logger.exception("Errore cleanup executor")
                finally:
                    try:
                        if self.db is not None:
                            try:
                                self.db.close_all_connections()
                            except Exception:
                                logger.exception("Errore cleanup db")
                    finally:
                        self._reset_runtime_refs()

    def _ensure_built_components(self) -> None:
        if self.db is None:
            raise RuntimeError("Database non inizializzato")
        if self.bus is None:
            raise RuntimeError("EventBus non inizializzato")
        if self.executor is None:
            raise RuntimeError("Executor non inizializzato")
        if self.shutdown is None:
            raise RuntimeError("ShutdownManager non inizializzato")
        if self.settings_service is None:
            raise RuntimeError("SettingsService non inizializzato")
        if self.betfair_service is None:
            raise RuntimeError("BetfairService non inizializzato")
        if self.telegram_service is None:
            raise RuntimeError("TelegramService non inizializzato")
        if self.trading_engine is None:
            raise RuntimeError("TradingEngine non inizializzato")
        if self.runtime is None:
            raise RuntimeError("RuntimeController non inizializzato")

    # =========================================================
    # BOOTSTRAP
    # =========================================================
    def build(self) -> None:
        """
        Costruzione completa dei componenti.
        Safe anche dopo uno stop o un build fallito.
        """
        if self._built:
            return

        self._reset_runtime_refs()

        try:
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

            self.safe_mode = get_safe_mode_manager()

            self.trading_engine = TradingEngine(
                bus=self.bus,
                db=self.db,
                client_getter=self.betfair_service.get_client,
                executor=self.executor,
                safe_mode=self.safe_mode,
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

            self.health_registry = HealthRegistry()
            self.metrics_registry = MetricsRegistry()
            self.alerts_manager = AlertsManager()
            self.incidents_manager = IncidentsManager()

            try:
                telegram_sender = None

                getter = getattr(self.telegram_service, "get_sender", None)
                if callable(getter):
                    telegram_sender = getter()

                if telegram_sender is None:
                    telegram_sender = getattr(self.telegram_service, "sender", None)

                has_sender_method = any(
                    callable(getattr(telegram_sender, name, None))
                    for name in ("send_alert_message", "send_message", "enqueue_message", "send")
                ) if telegram_sender is not None else False

                if has_sender_method:
                    self.telegram_alerts_service = TelegramAlertsService(
                        settings_service=self.settings_service,
                        telegram_sender=telegram_sender,
                    )

                    if self.alerts_manager is not None:
                        register = getattr(self.alerts_manager, "register_notifier", None)
                        if callable(register):
                            register(self.telegram_alerts_service.notify_alert)
                else:
                    logger.warning(
                        "TelegramAlertsService non inizializzato: sender non valido "
                        "(metodi richiesti: send_alert_message/send_message/enqueue_message/send)"
                    )

            except Exception:
                logger.exception("Impossibile inizializzare TelegramAlertsService")

            self.runtime_probe = RuntimeProbe(
                db=self.db,
                trading_engine=self.trading_engine,
                runtime_controller=self.runtime if "runtime_controller" in locals() else self.runtime,
                betfair_service=self.betfair_service,
                safe_mode=self.safe_mode,
                shutdown_manager=self.shutdown if "shutdown_manager" in locals() else self.shutdown,
                telegram_service=self.telegram_service,
                settings_service=self.settings_service,
                telegram_alerts_service=self.telegram_alerts_service,
            )

            self.snapshot_service = SnapshotService(
                db=self.db,
                probe=self.runtime_probe,
                health_registry=self.health_registry,
                metrics_registry=self.metrics_registry,
                alerts_manager=self.alerts_manager,
                incidents_manager=self.incidents_manager,
            )

            self.watchdog_service = WatchdogService(
                probe=self.runtime_probe,
                health_registry=self.health_registry,
                metrics_registry=self.metrics_registry,
                alerts_manager=self.alerts_manager,
                incidents_manager=self.incidents_manager,
                snapshot_service=self.snapshot_service,
                settings_service=self.settings_service,
                interval_sec=5.0,
            )

            self.diagnostics_service = DiagnosticsService(
                builder=DiagnosticBundleBuilder(export_dir="diagnostics_exports"),
                probe=self.runtime_probe,
                health_registry=self.health_registry,
                metrics_registry=self.metrics_registry,
                alerts_manager=self.alerts_manager,
                incidents_manager=self.incidents_manager,
                db=self.db,
                safe_mode=self.safe_mode,
                log_paths=[
                    "logs/app.log",
                    "logs/trading.log",
                    "logs/alerts.log",
                    "logs/audit.log",
                    "logs/incidents.log",
                ],
            )

            self.retention_manager = RetentionManager(
                db=self.db,
                diagnostics_export_dir="diagnostics_exports",
                snapshots_max_age_days=7,
                exports_max_age_days=7,
                exports_keep_last=20,
            )
            self.cleanup_service = CleanupService(
                retention_manager=self.retention_manager,
                interval_sec=3600.0,
            )

            try:
                self.trading_engine.metrics_registry = self.metrics_registry
            except Exception:
                pass

            try:
                self.health_registry.set_component("database", "READY", reason="startup")
                self.health_registry.set_component("trading_engine", "READY", reason="startup")
                self.health_registry.set_component("watchdog_service", "READY", reason="startup")
            except Exception:
                pass

            self.watchdog_service.start()
            self.cleanup_service.start()

            self._wire_bus()
            self._register_shutdown_hooks()
            self._ensure_built_components()

            self._built = True

        except Exception:
            logger.exception("Errore durante build headless")
            self._cleanup_partial_build()
            raise

    def _register_shutdown_hooks(self) -> None:
        if not self.shutdown:
            return

        if self.telegram_service is not None:
            self._register_shutdown_hook(
                "telegram_stop",
                self.telegram_service.stop,
                priority=10,
            )

        if self.betfair_service is not None:
            self._register_shutdown_hook(
                "betfair_disconnect",
                self.betfair_service.disconnect,
                priority=20,
            )

        if self.db is not None:
            self._register_shutdown_hook(
                "db_close",
                self.db.close_all_connections,
                priority=30,
            )

        if self.executor is not None:
            self._register_shutdown_hook(
                "executor_shutdown",
                self.executor.shutdown,
                priority=40,
            )

    def _register_shutdown_hook(self, name: str, fn: Any, priority: int = 100) -> None:
        if self.shutdown is None or fn is None:
            return

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
        if self.bus is None:
            return

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
        self.bus.subscribe("QUICK_BET_SUCCESS", self._on_quick_bet_success)
        self.bus.subscribe("QUICK_BET_AMBIGUOUS", self._on_quick_bet_ambiguous)

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

    def _on_quick_bet_success(self, payload):
        logger.info("QUICK_BET_SUCCESS -> %s", payload)

    def _on_quick_bet_ambiguous(self, payload):
        logger.warning("QUICK_BET_AMBIGUOUS -> %s", payload)

    # =========================================================
    # ARGUMENTS
    # =========================================================
    def _parse_args(self) -> dict:
        args = [str(x).strip().lower() for x in sys.argv[1:]]

        simulation_mode = True
        if "--live" in args or "live" in args:
            simulation_mode = False
        elif (
            "--simulation" in args
            or "simulation" in args
            or "--sim" in args
            or "sim" in args
        ):
            simulation_mode = True
        else:
            try:
                if self.settings_service is not None and hasattr(
                    self.settings_service, "load_simulation_config"
                ):
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
    # RECOVERY / HEALTH
    # =========================================================
    def _run_boot_recovery(self) -> None:
        if self.trading_engine is None:
            return

        try:
            result = self.trading_engine.recover_after_restart()
            logger.info("Boot recovery -> %s", result)
            if isinstance(result, dict) and result.get("ok") is False:
                raise RuntimeError(result.get("error") or "recover_after_restart fallita")
        except Exception:
            logger.exception("Errore boot recovery")
            raise

    def _validate_runtime_start_result(self, result: Any) -> None:
        if result is None:
            raise RuntimeError("Runtime.start() ha restituito None")

        if isinstance(result, dict) and result.get("ok") is False:
            raise RuntimeError(
                result.get("error")
                or result.get("reason")
                or "Runtime.start() fallita"
            )

        if self.runtime is None:
            raise RuntimeError("Runtime non disponibile dopo start")

    # =========================================================
    # RUN
    # =========================================================
    def start(self) -> int:
        if self._running:
            logger.warning("HeadlessApp già in esecuzione")
            return 0

        try:
            self.build()
            self._run_boot_recovery()
        except Exception as exc:
            logger.exception("Errore bootstrap headless: %s", exc)
            return 1

        args = self._parse_args()
        simulation_mode = bool(args["simulation_mode"])
        password = args["password"]

        mode_txt = "SIMULATION" if simulation_mode else "LIVE"
        logger.info("Avvio runtime headless in modalità %s", mode_txt)

        if self.runtime is None:
            logger.error("Runtime non disponibile dopo build")
            self._cleanup_partial_build()
            return 1

        try:
            result = self.runtime.start(
                password=password,
                simulation_mode=simulation_mode,
            )
            self._validate_runtime_start_result(result)
            logger.info("Runtime avviato -> %s", result)
        except Exception as exc:
            logger.exception("Errore avvio runtime: %s", exc)
            self.stop()
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
        """
        Stop idempotente.
        Deve lasciare lo stato pulito per eventuale nuovo build/start.
        """
        was_running = self._running
        self._running = False

        try:
            if self.runtime is not None:
                try:
                    self.runtime.stop()
                except Exception:
                    logger.exception("Errore stop runtime")
            if self.diagnostics_service is not None:
                try:
                    bundle_path = self.diagnostics_service.export_bundle()
                    logger.info("Diagnostics bundle exported: %s", bundle_path)
                except Exception:
                    logger.exception("Diagnostics export failed during shutdown")
            if self.watchdog_service is not None:
                try:
                    self.watchdog_service.stop()
                except Exception:
                    logger.exception("Watchdog stop failed")
            if self.cleanup_service is not None:
                try:
                    self.cleanup_service.stop()
                except Exception:
                    logger.exception("CleanupService stop failed")
        finally:
            try:
                if self.shutdown is not None:
                    if hasattr(self.shutdown, "shutdown"):
                        self.shutdown.shutdown()
                    elif hasattr(self.shutdown, "run"):
                        self.shutdown.run()
            except Exception:
                logger.exception("Errore shutdown manager")
            finally:
                if was_running or self._built:
                    self._reset_runtime_refs()

    # =========================================================
    # SIGNAL HANDLERS
    # =========================================================
    def _install_signal_handlers(self) -> None:
        if self._signal_handlers_installed:
            return

        def _handler(signum, frame):
            _ = frame
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

        self._signal_handlers_installed = True


def main() -> int:
    app: Optional[HeadlessApp] = None
    try:
        app = HeadlessApp()
        return app.start()
    except Exception as exc:
        if app is not None and app.alerts_manager is not None and app.incidents_manager is not None:
            try:
                app.alerts_manager.upsert_alert(
                    "HEADLESS_FATAL",
                    "critical",
                    "Fatal error in headless_main",
                    details={"error": str(exc)},
                )
                app.incidents_manager.open_incident(
                    "HEADLESS_FATAL",
                    "Headless Main Fatal",
                    "critical",
                    details={"error": str(exc)},
                )
            except Exception:
                logger.exception("Failed to register fatal observability event")
        logger.exception("Errore fatale in headless_main: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
