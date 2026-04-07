import pytest
import inspect


def _assert_instance_method(owner, method_name: str) -> None:
    assert hasattr(owner, method_name)
    method = getattr(owner, method_name)
    assert callable(method), f"{method_name} must be callable, not just present"
    params = inspect.signature(method).parameters
    assert "self" in params, f"{method_name} must remain an instance method"


@pytest.mark.guardrail
@pytest.mark.smoke
def test_headless_main_imports():
    import headless_main

    assert hasattr(headless_main, "HeadlessApp")


@pytest.mark.guardrail
@pytest.mark.smoke
def test_core_runtime_controller_imports():
    from core.runtime_controller import RuntimeController

    assert RuntimeController is not None


@pytest.mark.guardrail
def test_trading_engine_has_required_entrypoints():
    from core.trading_engine import TradingEngine

    required_methods = ("submit_quick_bet", "recover_after_restart")
    for method_name in required_methods:
        _assert_instance_method(TradingEngine, method_name)


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
        _assert_instance_method(Database, method_name)
