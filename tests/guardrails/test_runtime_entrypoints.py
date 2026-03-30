import pytest


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

    assert hasattr(TradingEngine, "submit_quick_bet")
    assert hasattr(TradingEngine, "recover_after_restart")


@pytest.mark.guardrail
def test_database_has_required_recovery_methods():
    from database import Database

    assert hasattr(Database, "create_order_saga")
    assert hasattr(Database, "update_order_saga")
    assert hasattr(Database, "get_pending_sagas")