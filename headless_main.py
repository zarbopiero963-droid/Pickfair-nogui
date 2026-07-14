from __future__ import annotations

import logging
import signal
import os
import sys
import threading
import time
from typing import Any, Optional

from database import Database
from core.event_bus import EventBus
from executor_manager import ExecutorManager
from shutdown_manager import ShutdownManager

from observability.sanitizers import sanitize_dict
from services.settings_service import SettingsService
from services.betfair_service import BetfairService
from services.telegram_alerts_service import TelegramAlertsService
from services.telegram_service import TelegramService

from core.trading_engine import TradingEngine
from core.runtime_controller import RuntimeController
from core.order_router import OrderRouter
from cashout_executor import CashoutExecutor
from cashout_request_bridge import CashoutRequestBridge
from cashout_cancel_adapter import CashoutCancelAdapter
from cashout_residual_handler import CashoutResidualHandler
from telegram_sender import get_telegram_sender
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
        self.order_router: Optional[OrderRouter] = None
        self.cashout_executor: Optional[CashoutExecutor] = None
        self.cashout_request_bridge: Optional[CashoutRequestBridge] = None
        self.cashout_residual_handler: Optional[CashoutResidualHandler] = None
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
        self._services_started = False
        self._services_lock = threading.Lock()
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
        self.order_router = None
        self.cashout_executor = None
        self.cashout_request_bridge = None
        self.cashout_residual_handler = None
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
        self._services_started = False
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
    def _start_background_services(self) -> None:
        """Avvia (idempotente) i thread di osservabilita' watchdog/cleanup.
        Separato da build() cosi' "componenti costruiti" e "servizi avviati"
        sono stati distinti: il preflight costruisce senza avviare, un avvio
        reale successivo puo' avviarli. Guard atomica (lock) per evitare un
        doppio start in caso di build() concorrenti (rilievo Fugu/Fable)."""
        with self._services_lock:
            if self._services_started:
                return
            self.watchdog_service.start()
            self.cleanup_service.start()
            self._services_started = True

    def build(self, start_services: bool = True) -> None:
        """
        Costruzione completa dei componenti.
        Safe anche dopo uno stop o un build fallito.

        Con ``start_services=False`` costruisce il runtime SENZA avviare i
        thread di osservabilita' (watchdog/cleanup): usato dal preflight
        read-only, che deve valutare la readiness senza avviare nulla.
        """
        if self._built:
            # Gia' costruito: avvia i servizi ORA se richiesto e non ancora
            # avviati. Copre il caso "preflight (start_services=False) seguito da
            # un avvio reale sullo stesso HeadlessApp": senza questo, watchdog e
            # cleanup resterebbero permanentemente inattivi (rilievo GPT-5.6).
            if start_services:
                self._start_background_services()
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
                safe_mode=self.safe_mode,
            )
            self.trading_engine.runtime_controller = self.runtime
            self.trading_engine.simulation_broker = getattr(self, "simulation_broker", None)
            self.trading_engine.betfair_client = self.betfair_service.get_client()

            self._wire_cashout_execution_chain()

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

            # Prefer direct evidence from live runtime objects over heuristic gauges.
            # async_db_writer is pulled from trading_engine if available; the probe
            # degrades gracefully when it is None.
            _async_db_writer = getattr(self.trading_engine, "async_db_writer", None)

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
                event_bus=self.bus,
                async_db_writer=_async_db_writer,
            )
            if self.runtime is not None:
                self.runtime.runtime_probe = self.runtime_probe
                self.runtime.enforce_probe_readiness_gate = True

            self.snapshot_service = SnapshotService(
                db=self.db,
                probe=self.runtime_probe,
                health_registry=self.health_registry,
                metrics_registry=self.metrics_registry,
                alerts_manager=self.alerts_manager,
                incidents_manager=self.incidents_manager,
            )

            # Default-on: anomaly reviewer runs unless settings explicitly disable it.
            # None from settings (not configured) preserves this default.
            anomaly_enabled = True
            anomaly_alerts_enabled = False
            anomaly_actions_enabled = False
            if self.settings_service is not None:
                try:
                    _ae = self.settings_service.load_anomaly_enabled()
                    if _ae is not None:
                        anomaly_enabled = bool(_ae)
                    else:
                        logger.info(
                            "anomaly_enabled not configured in settings; "
                            "anomaly reviewer running in default-on mode"
                        )
                except Exception:
                    logger.warning(
                        "Failed loading anomaly_enabled toggle; "
                        "anomaly reviewer running in default-on mode (anomaly_enabled=True)"
                    )
                try:
                    anomaly_alerts_enabled = bool(self.settings_service.load_anomaly_alerts_enabled())
                except Exception:
                    logger.exception("Failed loading anomaly_alerts_enabled toggle; fallback False")
                try:
                    anomaly_actions_enabled = bool(self.settings_service.load_anomaly_actions_enabled())
                except Exception:
                    logger.exception("Failed loading anomaly_actions_enabled toggle; fallback False")

            self.watchdog_service = WatchdogService(
                probe=self.runtime_probe,
                health_registry=self.health_registry,
                metrics_registry=self.metrics_registry,
                alerts_manager=self.alerts_manager,
                incidents_manager=self.incidents_manager,
                snapshot_service=self.snapshot_service,
                settings_service=self.settings_service,
                anomaly_enabled=anomaly_enabled,
                anomaly_alerts_enabled=anomaly_alerts_enabled,
                anomaly_actions_enabled=anomaly_actions_enabled,
                anomaly_alert_service=self.telegram_alerts_service,
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

            if start_services:
                self._start_background_services()

            self._wire_bus()
            self._register_shutdown_hooks()
            self._ensure_built_components()

            self._built = True

        except Exception:
            logger.exception("Errore durante build headless")
            self._cleanup_partial_build()
            raise

    def _wire_cashout_execution_chain(self) -> None:
        """Cabla la catena di ESECUZIONE + residuo del cashout (Fase 2.1-B2.4b-1/-2).

        Sottoscrive al bus la catena ``REQ_EXECUTE_CASHOUT`` →
        ``CMD_EXECUTE_CASHOUT`` → piazzamento green-up → gestione del residuo:

        - ``OrderRouter``: seam unico di piazzamento sim/live (sceglie il broker
          attivo via ``betfair_service.get_client()``, mode-aware);
        - ``CashoutExecutor``: consuma ``CMD_EXECUTE_CASHOUT``, applica le
          invarianti real-money hard e piazza l'hedge tramite l'``OrderRouter``;
        - ``CashoutRequestBridge``: bridge cashout-only che consuma
          ``REQ_EXECUTE_CASHOUT`` (dedup + normalizzazione + ``CASHOUT_FAILED``
          strutturato) e pubblica ``CMD_EXECUTE_CASHOUT``;
        - ``CashoutResidualHandler``: consuma ``CASHOUT_FAILED`` e gestisce il
          residuo (UNMATCHED ⇒ un solo cancel sicuro del resting + persist +
          notify; AMBIGUOUS ⇒ no-cancel + reconciliation + notify alta; altri ⇒
          notify + persist diagnostico). ``cancel`` via ``CashoutCancelAdapter``
          (cashout-only), ``persist`` su ``audit_events``, ``notify`` su Telegram.

        ATTIVA da B2.4b-2: il trigger runtime (``RuntimeController`` sui segnali
        CASHOUT/CASHOUT_ALL) emette ``REQ_EXECUTE_CASHOUT``. Il wiring degli altri
        order type (QUICK_BET, DUTCHING, CANCEL, REPLACE) NON è toccato: bridge e
        residual handler sono cashout-only (sottoscrivono solo eventi cashout).
        """
        if self.bus is None or self.betfair_service is None:
            raise RuntimeError("Bus/BetfairService non inizializzati per il wiring cashout")

        self.order_router = OrderRouter(self.betfair_service)
        self.cashout_executor = CashoutExecutor(self.bus, self.order_router)
        self.cashout_executor.wire()

        self.cashout_request_bridge = CashoutRequestBridge(self.bus)
        self.cashout_request_bridge.wire()

        svc = self.betfair_service
        # Accessori risolti lazy (lambda): l'adapter legge is_simulation/broker
        # solo a cancel-time (su un CASHOUT_FAILED UNMATCHED), mai durante build().
        # Così un betfair_service incompleto non fa crashare il boot, e la scelta
        # sim/live resta valutata al momento del cancel reale.
        residual_cancel = CashoutCancelAdapter(
            is_simulation=lambda: svc.is_simulation_mode(),
            live_cancel=lambda **kw: (c.cancel_orders(**kw) if (c := svc.get_live_client()) is not None else False),
            sim_cancel=lambda **kw: (b.cancel_orders(**kw) if (b := svc.get_simulation_broker()) is not None else False),
        )
        self.cashout_residual_handler = CashoutResidualHandler(
            cancel=residual_cancel.cancel,
            # Lazy: il record è scritto solo a CASHOUT_FAILED-time, mai a build();
            # _safe_persist nel handler assorbe eventuali errori di scrittura.
            persist=lambda record: self.db.insert_audit_event(record),
            notify=self._notify_cashout_residual,
        )
        self.cashout_residual_handler.wire(self.bus)

        logger.info(
            "Catena cashout cablata: "
            "CashoutRequestBridge[REQ_EXECUTE_CASHOUT] -> "
            "CashoutExecutor[CMD_EXECUTE_CASHOUT] -> OrderRouter; "
            "CashoutResidualHandler[CASHOUT_FAILED]"
        )

    def _resolve_telegram_sender(self) -> Any:
        """Risolve il sender Telegram dal ``telegram_service`` (come telegram_alerts).

        Il global ``get_telegram_sender()`` è inizializzato solo se costruito con
        un client (in headless non lo è), quindi NON ci si affida a quello: si usa
        il sender del ``telegram_service`` (lo stesso che consuma CASHOUT_SUCCESS),
        con fallback al global per percorsi alternativi.
        """
        sender = None
        try:
            svc = self.telegram_service
            if svc is not None:
                getter = getattr(svc, "get_sender", None)
                if callable(getter):
                    sender = getter()
                if sender is None:
                    sender = getattr(svc, "sender", None)
        except Exception:
            sender = None
        if sender is None:
            try:
                sender = get_telegram_sender()
            except Exception:
                sender = None
        return sender

    def _notify_cashout_residual(self, text: str, *, severity: str = "HIGH") -> None:
        """Notifica operatore del residuo cashout (best-effort, mai solleva).

        Prova i metodi del sender in ordine (``queue_default_message`` → invio
        diretto); se nessuno è disponibile o l'invio fallisce, logga. Il
        ``message_type`` dedicato distingue queste notifiche dagli altri invii.
        """
        msg = f"[{severity}] {text}"
        sender = self._resolve_telegram_sender()
        if sender is not None:
            q = getattr(sender, "queue_default_message", None)
            if callable(q):
                try:
                    q(msg, message_type="CASHOUT_RESIDUAL")
                    return
                except Exception:
                    logger.exception("Notifica residuo via queue_default_message fallita")
            for name in ("send_message", "enqueue_message", "send"):
                fn = getattr(sender, name, None)
                if callable(fn):
                    try:
                        fn(msg)
                        return
                    except Exception:
                        logger.exception("Notifica residuo via %s fallita", name)
        logger.warning("[CASHOUT_RESIDUAL][%s] %s", severity, text)

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
        logger.info("RUNTIME_STARTED -> %s", sanitize_dict(payload))

    def _on_runtime_paused(self, payload):
        logger.info("RUNTIME_PAUSED -> %s", sanitize_dict(payload))

    def _on_runtime_resumed(self, payload):
        logger.info("RUNTIME_RESUMED -> %s", sanitize_dict(payload))

    def _on_runtime_stopped(self, payload):
        logger.info("RUNTIME_STOPPED -> %s", sanitize_dict(payload))

    def _on_runtime_lockdown(self, payload):
        logger.warning("RUNTIME_LOCKDOWN -> %s", sanitize_dict(payload))

    def _on_telegram_status(self, payload):
        logger.info("TELEGRAM_STATUS -> %s", sanitize_dict(payload))

    def _on_signal_received(self, payload):
        logger.info("SIGNAL_RECEIVED -> %s", sanitize_dict(payload))

    def _on_signal_approved(self, payload):
        logger.info("SIGNAL_APPROVED -> %s", sanitize_dict(payload))

    def _on_signal_rejected(self, payload):
        logger.warning("SIGNAL_REJECTED -> %s", sanitize_dict(payload))

    def _on_quick_bet_submitted(self, payload):
        logger.info("QUICK_BET_SUBMITTED -> %s", sanitize_dict(payload))

    def _on_quick_bet_accepted(self, payload):
        logger.info("QUICK_BET_ACCEPTED -> %s", sanitize_dict(payload))

    def _on_quick_bet_partial(self, payload):
        logger.info("QUICK_BET_PARTIAL -> %s", sanitize_dict(payload))

    def _on_quick_bet_filled(self, payload):
        logger.info("QUICK_BET_FILLED -> %s", sanitize_dict(payload))

    def _on_quick_bet_failed(self, payload):
        logger.error("QUICK_BET_FAILED -> %s", sanitize_dict(payload))

    def _on_quick_bet_rollback_done(self, payload):
        logger.warning("QUICK_BET_ROLLBACK_DONE -> %s", sanitize_dict(payload))

    def _on_quick_bet_success(self, payload):
        logger.info("QUICK_BET_SUCCESS -> %s", sanitize_dict(payload))

    def _on_quick_bet_ambiguous(self, payload):
        logger.warning("QUICK_BET_AMBIGUOUS -> %s", sanitize_dict(payload))

    # =========================================================
    # ARGUMENTS
    # =========================================================
    def _parse_args(self) -> dict:
        args = [str(x).strip().lower() for x in sys.argv[1:]]

        execution_mode = "SIMULATION"
        if "--live" in args or "live" in args:
            execution_mode = "LIVE"
        elif (
            "--simulation" in args
            or "simulation" in args
            or "--sim" in args
            or "sim" in args
        ):
            execution_mode = "SIMULATION"
        else:
            try:
                if self.settings_service is not None and hasattr(
                    self.settings_service, "load_execution_mode"
                ):
                    execution_mode = str(self.settings_service.load_execution_mode() or "SIMULATION").upper()
            except Exception:
                execution_mode = "SIMULATION"

        live_enabled = False
        try:
            if self.settings_service is not None and hasattr(self.settings_service, "load_live_enabled"):
                live_enabled = bool(self.settings_service.load_live_enabled())
        except Exception:
            live_enabled = False

        kill_switch = False
        try:
            if self.settings_service is not None and hasattr(self.settings_service, "load_kill_switch"):
                kill_switch = bool(self.settings_service.load_kill_switch())
        except Exception:
            kill_switch = False

        live_readiness_ok = False
        try:
            if self.settings_service is not None and hasattr(
                self.settings_service, "load_live_readiness_ok"
            ):
                live_readiness_ok = bool(self.settings_service.load_live_readiness_ok())
        except Exception:
            live_readiness_ok = False

        if "--live-enabled" in args:
            live_enabled = True
        if "--live-disabled" in args:
            live_enabled = False
        if "--kill-switch-on" in args:
            kill_switch = True
        if "--kill-switch-off" in args:
            kill_switch = False

        if kill_switch:
            live_enabled = False

        password = None
        for item in sys.argv[1:]:
            raw = str(item)
            if raw.startswith("--password="):
                password = raw.split("=", 1)[1]
                break

        emergency_stop = "--emergency-stop" in args

        return {
            "simulation_mode": execution_mode != "LIVE",
            "execution_mode": execution_mode,
            "live_enabled": live_enabled,
            "kill_switch": kill_switch,
            "live_readiness_ok": live_readiness_ok,
            "password": password,
            "emergency_stop": emergency_stop,
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
    # GO-LIVE PREFLIGHT (read-only)
    # =========================================================
    # Mappa blocker-code -> (cosa significa, come si rimedia). I codici sono
    # quelli prodotti da RuntimeController.evaluate_live_readiness (fonte
    # autorevole dei prerequisiti LIVE): il preflight li rende leggibili a
    # schermo invece di lasciarli sepolti nel log.
    _BLOCKER_REMEDIATION = {
        "LIVE_NOT_ENABLED": (
            "live_enabled e' False",
            "Avvia con --live-enabled oppure imposta live_enabled=True nel DB.",
        ),
        "LIVE_READINESS_FLAG_NOT_OK": (
            "Il flag live_readiness_ok non e' confermato",
            "Conferma la readiness LIVE (live_readiness_ok=True nel DB) dopo la checklist go-live.",
        ),
        "LIVE_KEY_SOURCE_UNSAFE": (
            "La chiave segreta non proviene da una sorgente sicura",
            "Usa PICKFAIR_SECRET_KEY (env) o un file chiave valido (~/.pickfair/db.key).",
        ),
        "LIVE_HARD_STOP_CONFIG_MISSING": (
            "Config hard-stop giornaliero mancante",
            "Imposta i campi hard-stop (perdita giornaliera max) nella config LIVE.",
        ),
        "LIVE_HARD_STOP_CONFIG_INVALID": (
            "Config hard-stop giornaliero non valida",
            "Correggi i valori hard-stop (numerici e coerenti) nella config LIVE.",
        ),
        "KILL_SWITCH_ACTIVE": (
            "Kill switch attivo",
            "Disattiva il kill switch (--kill-switch-off o flag DB).",
        ),
        "SAFE_MODE_BLOCKING": (
            "Safe mode sta bloccando il LIVE",
            "Rimuovi la condizione di safe mode (kill switch / emergenza).",
        ),
        "LIVE_DEPENDENCY_MISSING": (
            "Dipendenza LIVE assente o degradata (es. betfair_service disconnesso)",
            "Verifica che betfair_service sia presente e connettibile (credenziali/cert).",
        ),
        "INVALID_EXECUTION_MODE": (
            "execution_mode non valido",
            "Usa SIMULATION o LIVE.",
        ),
        "CONTRADICTORY_STATE": (
            "Stato contraddittorio (LIVE ma simulation_mode/live_enabled incoerenti)",
            "Allinea execution_mode / live_enabled / simulation_mode.",
        ),
        "RUNTIME_NOT_INITIALIZED": (
            "Runtime non inizializzato",
            "Verifica il build del runtime (config/table_manager/...).",
        ),
        "RUNTIME_HALF_STARTED": (
            "Runtime avviato a meta' (servizi non connessi)",
            "Riavvia e verifica la connessione betfair/telegram.",
        ),
        "STARTUP_FAILED": (
            "Errore di startup registrato",
            "Consulta last_error/log e risolvi l'errore di avvio.",
        ),
        "READINESS_SIGNAL_UNKNOWN": (
            "Segnale di readiness sconosciuto",
            "Verifica lo stato del runtime (mode).",
        ),
        "RUNTIME_LOCKDOWN": (
            "Runtime in lockdown",
            "Rimuovi la condizione di lockdown (emergenza / riconciliazione).",
        ),
        # Reason di alto livello del deploy gate (fallback quando non ci sono
        # blocker granulari, es. probe LIVE non OK).
        "DEPLOY_BLOCKED_NOT_READY": (
            "Deploy gate: readiness LIVE non pronta",
            "Risolvi i blocker di readiness elencati o conferma live_readiness_ok.",
        ),
        "DEPLOY_BLOCKED_KILL_SWITCH": (
            "Deploy gate: kill switch attivo",
            "Disattiva il kill switch (--kill-switch-off o flag DB).",
        ),
        "DEPLOY_BLOCKED_BLOCKERS_PRESENT": (
            "Deploy gate: blocker di readiness presenti",
            "Risolvi i blocker elencati sopra.",
        ),
        "DEPLOY_BLOCKED_INVALID_STATE": (
            "Deploy gate: stato non valido",
            "Verifica execution_mode / live_enabled / readiness.",
        ),
        "LIVE_PROBE_NOT_READY": (
            "Probe di readiness LIVE non OK (streaming/bankroll/reconciliation)",
            "Verifica la salute di streaming, bankroll sync e reconciliation.",
        ),
    }

    def _preflight_requested(self) -> bool:
        # Check su argv (non su _parse_args) DELIBERATO: la decisione va presa
        # PRIMA di build()/settings_service, per costruire in modalita'
        # read-only (start_services=False). `--preflight` e' un flag "bare":
        # gli unici argomenti con valore usano la forma inline `--opt=val`
        # (es. --password=...), quindi lo scan e' sicuro.
        return "--preflight" in [str(a).strip().lower() for a in sys.argv[1:]]

    def _telegram_login_requested(self) -> bool:
        # Come --preflight: check su argv, il login e' un'azione a se' che NON
        # avvia il runtime/trading (serve solo il DB per api_id/api_hash e per
        # salvare la session_string generata).
        return "--telegram-login" in [str(a).strip().lower() for a in sys.argv[1:]]

    @staticmethod
    def _sanitize_login_code(raw):
        """Estrae SOLO le cifre dal codice inserito.

        Il messaggio di servizio Telegram (777000) è tipo ``Login code: 12345``:
        se l'utente incolla tutto, ``sign_in`` darebbe "codice non valido". Qui
        teniamo solo le cifre (``12345``). Se non ci sono cifre, ritorna il testo
        originale (strip) così l'errore resta comprensibile.
        """
        text = str(raw or "")
        digits = "".join(ch for ch in text if ch.isdigit())
        return digits or text.strip()

    @staticmethod
    def _request_code_error_message(err):
        """Messaggio per un ``request_code`` fallito.

        Su FloodWait (troppi invii ravvicinati) spiega di ATTENDERE e NON
        rilanciare: rilanciare peggiora il flood e invalida i codici precedenti
        (causa tipica del "codice non valido" dopo molti tentativi).
        """
        text = str(err or "")
        low = text.lower()
        if "wait" in low or "flood" in low:
            return (
                f"⏳ Telegram ha imposto un'attesa (troppi tentativi ravvicinati): {text}\n"
                "   Aspetta i secondi indicati e NON rilanciare il comando finché non scade."
            )
        return f"❌ Invio codice fallito: {text}"

    @staticmethod
    def _signin_error_message(err):
        """Messaggio per un ``sign_in`` fallito, con guida sul caso più comune.

        "codice non valido" quasi sempre = si è usato un codice di una richiesta
        precedente (ogni invio invalida i precedenti) o si è incollato testo extra.
        """
        if str(err) == "invalid_code":
            return (
                "❌ Codice non valido: usa SOLO l'ULTIMO codice ricevuto (ogni nuovo "
                "invio invalida i precedenti) e digita solo le cifre."
            )
        return f"❌ Login non riuscito: {err}"

    @staticmethod
    def _telegram_login_flow(listener, api_id, api_hash, *, prompt, prompt_secret, out):
        """Flusso interattivo di login userbot Telegram (telefono+codice+2FA).

        Puro (I/O iniettato via prompt/prompt_secret/out) per testabilita'.
        Ritorna (exit_code, session_string|None): 0 = login ok, 2 = fallito.
        """
        if not api_id or not api_hash:
            out("❌ api_id/api_hash Telegram non configurati: configurali prima (GUI o DB).")
            return 2, None
        phone = (prompt("Numero di telefono (es. +39...): ") or "").strip()
        if not phone:
            out("❌ Numero di telefono mancante.")
            return 2, None
        res = listener.request_code(phone)
        if not res.get("ok"):
            out(HeadlessApp._request_code_error_message(res.get("error")))
            return 2, None
        out("📩 Codice inviato. Controlla l'app Telegram (chat \"Telegram\", 777000), non l'SMS.")
        code = HeadlessApp._sanitize_login_code(prompt("Codice di verifica (solo le cifre): "))
        res = listener.sign_in(code)
        if res.get("requires_password"):
            out("🔐 Autenticazione a due fattori (2FA) attiva.")
            pwd = (prompt_secret("Password 2FA: ") or "").strip()
            res = listener.sign_in(code, password_2fa=pwd)
        if res.get("ok") and res.get("session_string"):
            out("✅ Login Telegram completato: session_string generata.")
            return 0, res.get("session_string")
        out(HeadlessApp._signin_error_message(res.get("error")))
        return 2, None

    @staticmethod
    def _cli_flag_value(argv, name):
        """Valore di `--name VALUE` o `--name=VALUE` in argv, '' se assente.

        Il valore in token separato è accettato SOLO se non è a sua volta
        un'opzione (`--...`): così `--api-id --altro` non cattura `--altro` come
        valore (che poi fallirebbe la conversione a int).
        """
        prefix = name + "="
        args = [str(x) for x in (argv or [])]
        for i, arg in enumerate(args):
            if arg.startswith(prefix):
                return arg[len(prefix):].strip()
            if arg == name:
                nxt = args[i + 1] if i + 1 < len(args) else ""
                return "" if nxt.startswith("--") else nxt.strip()
        return ""

    @staticmethod
    def _resolve_telegram_credentials(argv, env, settings):
        """Risolve api_id/api_hash per `--telegram-login`, '' se non risolti.

        api_id (non segreto): flag `--api-id` > env `TELEGRAM_API_ID` > DB.
        api_hash (SEGRETO): env `TELEGRAM_API_HASH` > DB — MAI da CLI, per non
        esporlo in `ps`/shell history (se manca, il chiamante lo chiede con input
        nascosto). `from_external` è True se almeno uno arriva da CLI/env: in quel
        caso va persistito nel DB (cifrato) SOLO dopo un login riuscito. NON legge
        `config.json` (evita di incoraggiare segreti in un file committato).
        """
        env = env or {}
        settings = settings or {}
        cli_id = HeadlessApp._cli_flag_value(argv, "--api-id")
        env_id = str(env.get("TELEGRAM_API_ID") or "").strip()
        env_hash = str(env.get("TELEGRAM_API_HASH") or "").strip()
        api_id = (cli_id or env_id or str(settings.get("api_id") or "")).strip()
        api_hash = (env_hash or str(settings.get("api_hash") or "")).strip()
        from_external = bool(cli_id or env_id or env_hash)
        return api_id, api_hash, from_external

    def _telegram_login_db(self):
        """Ritorna il DB per `--telegram-login` (init se serve), o None su errore."""
        db = self.db
        if db is None:
            try:
                db = Database()
            except Exception as exc:
                logger.exception("Errore init DB per --telegram-login: %s", exc)
                print(f"❌ Errore inizializzazione DB: {exc}")
                return None
        return db

    def _telegram_prepare_login(self, db, prompt_secret):
        """Legge i settings, risolve/valida le credenziali e costruisce il listener.

        Ritorna sempre una tupla a 6 elementi il cui primo è l'``exit_code``:
        ``(None, listener, api_id, api_hash, from_external, settings)`` al successo,
        oppure ``(2, None, None, None, None, None)`` se una credenziale manca o è
        invalida. Contratto non ambiguo (exit_code ``None`` vs ``int``: niente
        controllo `isinstance(int)`, che con `bool ⊂ int` sarebbe fragile).
        api_hash è un SEGRETO: se non è in env/DB viene chiesto con input nascosto
        (`prompt_secret`) — mai da CLI (sarebbe visibile in `ps`/history).
        """
        from telegram_listener import TelegramListener

        fail = (2, None, None, None, None, None)
        try:
            settings = db.get_telegram_settings()
        except Exception as exc:
            logger.exception("Lettura settings Telegram per --telegram-login fallita: %s", exc)
            print(f"❌ Errore lettura configurazione Telegram: {exc}")
            return fail
        api_id, api_hash, from_external = self._resolve_telegram_credentials(
            sys.argv[1:], os.environ, settings
        )
        # api_id dev'essere un intero POSITIVO: mancante, "0", negativo o non
        # numerico è config invalida (Telethon fallirebbe dopo con un errore
        # opaco). Errore chiaro invece di istanziare il listener con 0/negativo.
        try:
            api_id_num = int(str(api_id or "").strip() or 0)
        except ValueError:
            api_id_num = 0
        if api_id_num <= 0:
            print("❌ api_id mancante o non valido (dev'essere un intero positivo): "
                  "passalo con --api-id, con l'env TELEGRAM_API_ID, oppure "
                  "configuralo nel DB (via GUI).")
            return fail
        # api_hash mancante: chiedilo con input NASCOSTO. La persistenza avviene
        # SOLO dopo un login riuscito, così creds errate non sovrascrivono il DB.
        if not api_hash:
            try:
                entered = prompt_secret("Telegram api_hash (input nascosto): ").strip()
            except Exception as exc:
                # getpass può sollevare (EOF/stdin chiuso su VPS non interattivo):
                # trattalo come login fallito (exit 2), non far propagare a main()
                # che lo appiattirebbe a exit 1 rompendo il contratto documentato.
                logger.exception("Lettura api_hash (input nascosto) fallita: %s", exc)
                print(f"❌ Errore lettura api_hash: {exc}")
                return fail
            if entered:
                api_hash = entered
                from_external = True
        if not api_hash:
            print("❌ api_hash mancante: forniscilo con l'env TELEGRAM_API_HASH, "
                  "dal DB, o all'input nascosto.")
            return fail
        try:
            listener = TelegramListener(api_id_num, api_hash, db=db)
        except Exception as exc:
            print(f"❌ Config Telegram non valida (api_id/api_hash): {exc}")
            return fail
        return None, listener, api_id, api_hash, from_external, settings

    @staticmethod
    def _telegram_persist_login(db, settings, api_id, api_hash, from_external, session_string):
        """Salva `session_string` (+ eventuali api_id/api_hash esterni) dopo login ok.

        Persiste api_id/api_hash forniti da CLI/env SOLO ora (evita di sovrascrivere
        il DB con creds errate). Best-effort: ritorna True se salvato, False se la
        persistenza fallisce (il chiamante mappa False su exit code 2).
        """
        merged = dict(settings)
        merged["session_string"] = session_string
        merged["enabled"] = True
        if from_external and api_id and api_hash:
            merged["api_id"] = api_id
            merged["api_hash"] = api_hash
        try:
            db.save_telegram_settings(merged)
        except Exception as exc:
            logger.exception("Persistenza configurazione Telegram fallita: %s", exc)
            print(f"⚠️  Login riuscito ma salvataggio fallito: {exc}")
            return False
        extra = " + api_id/api_hash" if from_external else ""
        print(f"💾 Configurazione salvata (session_string{extra}, cifrate). Riavvia in modalità normale.")
        return True

    def _run_telegram_login(self) -> int:
        """Comando `--telegram-login`: autentica un userbot Telegram dal VPS
        (telefono+codice+2FA), genera la session_string e la salva nel DB.

        api_id: da `--api-id` o env `TELEGRAM_API_ID`, altrimenti dal DB.
        api_hash (segreto): da env `TELEGRAM_API_HASH` o dal DB; se manca viene
        chiesto con input nascosto (mai da CLI). Le credenziali fornite da CLI/env
        sono salvate nel DB (cifrate) SOLO dopo un login riuscito.

        NON avvia il runtime/trading. Exit code: 0 login ok (e salvataggio ok),
        2 login fallito, credenziali mancanti, o persistenza post-login fallita.
        """
        import getpass as _getpass

        db = self._telegram_login_db()
        if db is None:
            return 2
        exit_code, listener, api_id, api_hash, from_external, settings = (
            self._telegram_prepare_login(db, _getpass.getpass)
        )
        if exit_code is not None:
            return exit_code  # credenziale mancante/invalida

        try:
            try:
                exit_code, session_string = self._telegram_login_flow(
                    listener, api_id, api_hash,
                    prompt=input, prompt_secret=_getpass.getpass, out=print,
                )
            except Exception as exc:
                # Un'eccezione imprevista nel flusso (rete, Telethon, input) è un
                # login fallito: exit 2 (contratto documentato), non lasciarla
                # propagare a main() che la appiattirebbe a exit 1.
                logger.exception("Flusso di login Telegram fallito: %s", exc)
                print(f"❌ Login Telegram fallito: {exc}")
                return 2
            if exit_code == 0 and session_string and not self._telegram_persist_login(
                db, settings, api_id, api_hash, from_external, session_string
            ):
                return 2
            return exit_code
        finally:
            # Chiudi sempre il client/loop di login, anche sui path di fallimento
            # (es. 2FA errata) dove il listener resta in attesa (rilievo Greptile).
            # try/except: un errore nel cleanup non deve mascherare il return
            # (o l'eccezione) del flusso di login (rilievo Fable).
            try:
                listener._cleanup_login()
            except Exception:
                logger.debug("Errore in cleanup login (finally)", exc_info=True)

    def _run_preflight(self) -> int:
        """Valuta i prerequisiti LIVE e stampa una checklist leggibile.

        [GO-LIVE PREFLIGHT] Read-only: usa lo STESSO gate di start()
        (RuntimeController.get_deploy_gate_status, variante read-only di
        enforce_deploy_gate: nessun side effect di log/attr), cosi' il verdicto
        riflette esattamente cio' che start() deciderebbe. NON si connette a
        Betfair ne' avvia il trading. Exit code: 0 pronto per LIVE, 2 non
        pronto, 3 se non e' stato richiesto LIVE (nulla da valutare), 1 se il
        runtime non e' stato costruito.
        """
        args = self._parse_args()
        execution_mode = str(args.get("execution_mode") or "SIMULATION")
        live_enabled = bool(args.get("live_enabled", False))
        live_readiness_ok = bool(args.get("live_readiness_ok", False))

        if self.runtime is None:
            msg = "[PREFLIGHT] Runtime non disponibile dopo build"
            logger.error(msg)
            print(msg)
            return 1  # errore di bootstrap/build (coerente con ops/preflight.md)

        # boot=True: il preflight e' un check PRE-connessione (build con
        # start_services=False, nessun connect a Betfair) e deve riflettere
        # esattamente cio' che start() deciderebbe al boot. start() usa il
        # gate in fase boot (tollera i soli componenti 'pending connection',
        # es. betfair disconnesso perche' si connette dopo il gate); senza
        # boot=True il preflight sarebbe piu' severo di start() e segnalerebbe
        # un falso blocker per betfair disconnesso.
        status = self.runtime.get_deploy_gate_status(
            execution_mode=execution_mode,
            live_enabled=live_enabled,
            live_readiness_ok=live_readiness_ok,
            boot=True,
        )
        report, exit_code = self._format_preflight_report(
            status, execution_mode, live_enabled, live_readiness_ok
        )
        print(report)
        logger.info("GO-LIVE PREFLIGHT\n%s", report)
        return exit_code

    def _append_blocker(self, lines, code, desc, remedy):
        lines.append(f"  [X] {code}")
        lines.append(f"        cosa   : {desc}")
        lines.append(f"        rimedio: {remedy}")

    def _format_preflight_report(self, status, execution_mode, live_enabled, live_readiness_ok):
        """Costruisce (report_testuale, exit_code) dal deploy-gate status. Il
        messaggio "PRONTO" e l'exit sono guidati da `allowed`, mai dalla sola
        assenza di blocker. Exit: 0 GO, 2 NO-GO, 3 non-LIVE (nulla da valutare
        -> fail-closed per l'uso come gate CI/script)."""
        allowed = bool(status.get("allowed", False))
        payload = (status.get("details") or {}).get("readiness_payload") or {}
        level = str(payload.get("level") or status.get("readiness") or "NOT_READY")
        blockers = [str(b) for b in (payload.get("blockers") or [])]
        probe = (payload.get("details") or {}).get("probe") or {}
        # Fail-closed nel display: probe assente -> NON assumere sano (rilievo Fugu).
        probe_ok = bool(probe.get("ok", False))

        lines = [
            "=" * 64,
            "PICKFAIR — GO-LIVE PREFLIGHT",
            "=" * 64,
            f"execution_mode richiesto : {execution_mode}",
            f"live_enabled             : {live_enabled}",
            f"live_readiness_ok        : {live_readiness_ok}",
            f"readiness level          : {level}",
            "-" * 64,
        ]

        # Non-LIVE: preflight LIVE non applicabile. Exit 3 (non 0): usato come
        # gate CI/script senza --live NON deve passare "verde" (fail-closed).
        if execution_mode.strip().upper() != "LIVE":
            lines.append(
                "execution_mode richiesto NON e' LIVE: preflight LIVE non "
                "applicabile. Rilancia con --live --live-enabled per valutare la "
                "readiness LIVE reale."
            )
            lines.append("=" * 64)
            return "\n".join(lines), 3

        if allowed:
            lines.append("PRONTO PER LIVE — nessun blocker.")
            lines.append("=" * 64)
            return "\n".join(lines), 0

        lines.append("NON PRONTO:")
        shown = False
        for code in blockers:
            desc, remedy = self._BLOCKER_REMEDIATION.get(
                code, ("(blocker non catalogato)", "Verifica lo stato runtime/log.")
            )
            self._append_blocker(lines, code, desc, remedy)
            shown = True
        if not probe_ok:
            desc, remedy = self._BLOCKER_REMEDIATION["LIVE_PROBE_NOT_READY"]
            self._append_blocker(lines, "LIVE_PROBE_NOT_READY", probe.get("reason") or desc, remedy)
            shown = True
        if not shown:
            # Nessun blocker granulare: mostra le reason di alto livello del gate.
            for reason in (status.get("reasons") or [status.get("reason") or "DEPLOY_BLOCKED_INVALID_STATE"]):
                desc, remedy = self._BLOCKER_REMEDIATION.get(
                    str(reason), ("Deploy gate NO-GO", "Consulta il deploy gate/log per il dettaglio.")
                )
                self._append_blocker(lines, reason, desc, remedy)
        lines.append("=" * 64)
        return "\n".join(lines), 2

    def _emit_live_nogo_diagnostics(self, deploy_gate, execution_mode, live_enabled, live_readiness_ok) -> None:
        """Stampa a schermo la checklist NO-GO del deploy gate all'avvio LIVE.

        #358: rende VISIBILE il motivo del NO-GO (blocker + rimedio) invece di
        lasciarlo sepolto nel log. Riusa la stessa checklist del preflight
        (`_format_preflight_report`): cosi' un avvio `--live` senza i
        prerequisiti (es. senza `--live-enabled`) stampa esplicitamente cosa
        manca invece di ripiegare in silenzio. E' puramente diagnostico: NON
        tocca la logica del gate ne' l'esito di `start()` (l'enforcement resta
        in `RuntimeController.start`).
        """
        report, _ = self._format_preflight_report(
            deploy_gate, execution_mode, live_enabled, live_readiness_ok
        )
        print(report)
        # WARNING (non INFO): la diagnostica va preservata nei log anche in
        # produzione dove il livello e' filtrato a WARNING+, coerente col
        # warning "[DEPLOY GATE] NO-GO" che la precede.
        logger.warning("[DEPLOY GATE] Diagnostica NO-GO LIVE:\n%s", report)

    # =========================================================
    # RUN
    # =========================================================
    def start(self) -> int:
        if self._running:
            logger.warning("HeadlessApp già in esecuzione")
            return 0

        # --telegram-login: azione interattiva a se' (login userbot dal VPS).
        # NON avvia runtime/trading; usa solo il DB per api_id/api_hash e per
        # salvare la session_string generata. Deciso PRIMA di build().
        if self._telegram_login_requested():
            return self._run_telegram_login()

        # --preflight va deciso PRIMA di build(): e' read-only e non deve avviare
        # i servizi (watchdog/cleanup). Il check e' su argv perche' avviene prima
        # che settings_service esista.
        preflight = self._preflight_requested()

        try:
            self.build(start_services=not preflight)
            if preflight:
                # Read-only: valuta e stampa i prerequisiti LIVE, poi esce.
                # NON esegue boot recovery ne' avvia il trading.
                return self._run_preflight()
            self._run_boot_recovery()
        except Exception as exc:
            logger.exception("Errore bootstrap headless: %s", exc)
            return 1

        args = self._parse_args()
        simulation_mode = bool(args["simulation_mode"])
        execution_mode = str(args.get("execution_mode") or "SIMULATION")
        live_enabled = bool(args.get("live_enabled", False))
        live_readiness_ok = bool(args.get("live_readiness_ok", False))
        password = args["password"]
        emergency_stop_requested = bool(args.get("emergency_stop", False))

        mode_txt = "SIMULATION" if simulation_mode else "LIVE"
        logger.info("Avvio runtime headless in modalità %s", mode_txt)

        if self.runtime is None:
            logger.error("Runtime non disponibile dopo build")
            self._cleanup_partial_build()
            return 1

        # Operator emergency-stop: immediately halt and cancel open orders before
        # entering the main loop.  --emergency-stop is the headless operator
        # trigger; it is fail-closed: partial cancel failures do not resume trading.
        if emergency_stop_requested:
            logger.warning("EMERGENCY STOP richiesto via --emergency-stop")
            try:
                result = self.runtime.emergency_stop(reason="operator_cli_flag")
                logger.warning("EMERGENCY_STOP result: %s", result)
            except Exception as exc:
                logger.exception("Errore durante emergency_stop: %s", exc)
            return 0

        try:
            if execution_mode == "LIVE":
                # boot=True: questo check gira PRIMA di runtime.start() (dove
                # Betfair si connette), quindi e' una valutazione di boot,
                # pre-connessione. Deve riflettere cio' che start() deciderebbe:
                # senza boot=True loggerebbe un falso "[DEPLOY GATE] NO-GO" per
                # la disconnessione attesa di Betfair. L'enforcement reale resta
                # dentro start() (anch'esso boot=True).
                deploy_gate = self.runtime.enforce_deploy_gate(
                    execution_mode=execution_mode,
                    live_enabled=live_enabled,
                    live_readiness_ok=live_readiness_ok,
                    boot=True,
                )
                if not deploy_gate.get("allowed", False):
                    logger.warning(
                        "[DEPLOY GATE] NO-GO: reason=%s",
                        ",".join(deploy_gate.get("reasons") or [str(deploy_gate.get("reason") or "")]),
                    )
                    # #358: motivo del NO-GO visibile a schermo (blocker+rimedio),
                    # non solo nel log. Es. `--live` senza `--live-enabled` ->
                    # stampa LIVE_NOT_ENABLED + "Avvia con --live-enabled".
                    self._emit_live_nogo_diagnostics(
                        deploy_gate, execution_mode, live_enabled, live_readiness_ok
                    )

            result = self.runtime.start(
                password=password,
                simulation_mode=simulation_mode,
                execution_mode=execution_mode,
                live_enabled=live_enabled,
                live_readiness_ok=live_readiness_ok,
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
