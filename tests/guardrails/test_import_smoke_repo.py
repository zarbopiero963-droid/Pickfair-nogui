import importlib

import pytest


MODULES = [
    "auto_throttle",
    "betfair_client",
    "circuit_breaker",
    "database",
    "dutching",
    "dutching_cache",
    "dutching_state",
    "executor_manager",
    "headless_main",
    "order_manager",
    "pnl_engine",
    "shutdown_manager",
    "simulation_broker",
    "telegram_listener",
    "telegram_module",
    "telegram_sender",
    "tick_dispatcher",
    "tick_storage",
    "core.async_db_writer",
    "core.dutching_batch_manager",
    "core.event_bus",
    "core.execution_guard",
    "core.fast_analytics",
    "core.market_tracker",
    "core.money_management",
    "core.order_router",
    "core.pnl_engine",
    "core.reconciliation_engine",
    "core.risk_desk",
    "core.risk_middleware",
    "core.runtime_controller",
    "core.safety_layer",
    "core.simulation_matching_engine",
    "core.simulation_order_book",
    "core.simulation_state",
    "core.state_recovery",
    "core.system_state",
    "core.table_manager",
    "core.tick_dispatcher",
    "core.tick_ring_buffer",
    "core.trading_engine",
    "services.betfair_service",
    "services.setting_service",
    "services.telegram_bet_resolver",
    "services.telegram_service",
    "services.telegram_signal_processor",
    "controllers.dutching_controller",
    "controllers.telegram_controller",
    "ai.ai_guardrail",
    "ai.ai_pattern_engine",
    "ai.wom_engine",
]


@pytest.mark.guardrail
@pytest.mark.smoke
@pytest.mark.parametrize("module_name", MODULES)
def test_module_import_smoke(module_name):
    mod = importlib.import_module(module_name)
    assert mod is not None