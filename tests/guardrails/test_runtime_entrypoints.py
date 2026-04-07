import pytest
import inspect


@pytest.mark.guardrail
@pytest.mark.smoke
def test_headless_main_imports():
    import headless_main  # noqa: F401


@pytest.mark.guardrail
@pytest.mark.smoke
def test_core_runtime_controller_imports():
    from core.runtime_controller import RuntimeController  # noqa: F401


@pytest.mark.guardrail
def test_trading_engine_has_required_entrypoints():
    from core.trading_engine import TradingEngine

    required_methods = ("submit_quick_bet", "recover_after_restart")
    for method_name in required_methods:
        assert hasattr(TradingEngine, method_name)
        method = getattr(TradingEngine, method_name)
        assert callable(method), f"{method_name} must be callable, not just present"
        params = inspect.signature(method).parameters
        assert "self" in params, f"{method_name} must remain an instance method"


@pytest.mark.guardrail
def test_database_has_required_recovery_methods():
    from database import Database

    required_methods = (
        "create_order_saga",
        "update_order_saga",
        "get_pending_sagas",
        "insert_order",
        "update_order",
        "get_order",
        "order_exists_inflight",
        "find_duplicate_order",
    )
    for method_name in required_methods:
        assert hasattr(Database, method_name)
        method = getattr(Database, method_name)
        assert callable(method), f"{method_name} must be callable, not just present"
