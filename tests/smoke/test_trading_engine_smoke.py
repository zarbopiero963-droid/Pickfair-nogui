import pytest


@pytest.mark.smoke
def test_import_trading_engine_module():
    import core.trading_engine  # noqa: F401


@pytest.mark.smoke
def test_trading_engine_basic_bootstrap():
    from core.trading_engine import TradingEngine

    class Bus:
        def subscribe(self, *_args, **_kwargs):
            return None

        def publish(self, *_args, **_kwargs):
            return None

    class DB:
        pass

    engine = TradingEngine(
        bus=Bus(),
        db=DB(),
        client_getter=lambda: None,
        executor=None,
    )

    assert engine is not None 