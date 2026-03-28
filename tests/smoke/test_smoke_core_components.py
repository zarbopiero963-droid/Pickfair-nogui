import pytest


@pytest.mark.smoke
def test_import_core_modules():
    import core.event_bus  # noqa: F401
    import core.duplication_guard  # noqa: F401
    import core.async_db_writer  # noqa: F401


@pytest.mark.smoke
def test_construct_event_bus():
    from core.event_bus import EventBus

    bus = EventBus()
    assert bus is not None, "EventBus deve costruirsi senza crash"


@pytest.mark.smoke
def test_construct_duplication_guard():
    from core.duplication_guard import DuplicationGuard

    guard = DuplicationGuard()
    assert guard is not None, "DuplicationGuard deve costruirsi senza crash"


@pytest.mark.smoke
def test_construct_async_db_writer():
    from core.async_db_writer import AsyncDBWriter

    class DummyDB:
        def save_bet(self, **payload):
            pass

        def save_cashout_transaction(self, **payload):
            pass

        def save_simulation_bet(self, **payload):
            pass

    writer = AsyncDBWriter(DummyDB())
    assert writer is not None, "AsyncDBWriter deve costruirsi senza crash"


@pytest.mark.smoke
def test_async_db_writer_start_stop():
    from core.async_db_writer import AsyncDBWriter

    class DummyDB:
        def save_bet(self, **payload):
            pass

        def save_cashout_transaction(self, **payload):
            pass

        def save_simulation_bet(self, **payload):
            pass

    writer = AsyncDBWriter(DummyDB())
    writer.start()
    writer.stop()

    assert True, "AsyncDBWriter deve avviarsi e fermarsi senza crash"
