from __future__ import annotations

import logging
import sys

from database import Database
from event_bus import EventBus
from executor_manager import ExecutorManager
from shutdown_manager import ShutdownManager

from core.trading_engine import TradingEngine
from core.runtime_controller import RuntimeController

from services.betfair_service import BetfairService
from services.setting_service import SettingsService
from services.telegram_service import TelegramService


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


# =========================================================
# BUILD APP (BOOTSTRAP COMPLETO)
# =========================================================
def build_app():
    db = Database()
    bus = EventBus()
    executor = ExecutorManager(max_workers=4, default_timeout=30)
    shutdown = ShutdownManager()

    settings_service = SettingsService(db)
    betfair_service = BetfairService(settings_service)
    telegram_service = TelegramService(settings_service, db, bus)

    trading_engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=betfair_service.get_client,
        executor=executor,
    )

    runtime = RuntimeController(
        bus=bus,
        db=db,
        settings_service=settings_service,
        betfair_service=betfair_service,
        telegram_service=telegram_service,
        trading_engine=trading_engine,
    )

    # =========================================================
    # SHUTDOWN ORDER (IMPORTANTE)
    # =========================================================
    shutdown.register("telegram_stop", telegram_service.stop, priority=10)
    shutdown.register("betfair_disconnect", betfair_service.disconnect, priority=20)
    shutdown.register("db_close", db.close_all_connections, priority=30)
    shutdown.register("executor_shutdown", executor.shutdown, priority=40)

    return {
        "db": db,
        "bus": bus,
        "executor": executor,
        "shutdown": shutdown,
        "settings_service": settings_service,
        "betfair_service": betfair_service,
        "telegram_service": telegram_service,
        "trading_engine": trading_engine,
        "runtime": runtime,
    }


# =========================================================
# STATUS PRINT
# =========================================================
def print_status(runtime):
    status = runtime.get_status()

    print("\n=== PICKFAIR HEADLESS STATUS ===")

    for key in [
        "mode",
        "desk_mode",
        "bankroll_current",
        "equity_peak",
        "realized_pnl",
        "total_exposure",
        "total_exposure_pct",
        "drawdown_pct",
        "telegram_connected",
        "betfair_connected",
        "active_tables",
        "recovery_tables",
        "last_error",
        "last_signal_at",
    ]:
        print(f"{key}: {status.get(key)}")

    print("\nTables:")
    for table in status.get("tables", []):
        print(table)

    print("\nDuplication Guard:")
    print(status.get("duplication_guard", {}))


# =========================================================
# CLI LOOP
# =========================================================
def main():
    app = build_app()
    runtime = app["runtime"]
    shutdown = app["shutdown"]

    password = None
    if len(sys.argv) > 1:
        password = sys.argv[1]

    try:
        print("===================================")
        print("   PICKFAIR HEADLESS ENGINE READY  ")
        print("===================================")
        print("Comandi disponibili:")
        print("start | pause | resume | stop | status | reset | exit")

        while True:
            try:
                cmd = input("> ").strip().lower()
            except (KeyboardInterrupt, EOFError):
                print("\nExit...")
                break

            if cmd == "start":
                result = runtime.start(password=password)
                print(result)

            elif cmd == "pause":
                print(runtime.pause())

            elif cmd == "resume":
                print(runtime.resume())

            elif cmd == "stop":
                print(runtime.stop())

            elif cmd == "status":
                print_status(runtime)

            elif cmd == "reset":
                print(runtime.reset_cycle())

            elif cmd in {"exit", "quit"}:
                break

            elif not cmd:
                continue

            else:
                print("Comando non riconosciuto.")

    finally:
        print("Shutting down...")
        shutdown.shutdown()


# =========================================================
# ENTRYPOINT
# =========================================================
if __name__ == "__main__":
    main()