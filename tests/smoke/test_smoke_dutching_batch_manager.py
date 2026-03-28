import pytest


@pytest.mark.smoke
def test_import_dutching_batch_manager():
    import core.dutching_batch_manager  # noqa: F401


@pytest.mark.smoke
def test_construct_dutching_batch_manager():
    from core.dutching_batch_manager import DutchingBatchManager

    class DummyDB:
        def _execute(self, query, params=(), fetch=False, commit=True):
            return []

    mgr = DutchingBatchManager(DummyDB())
    assert mgr is not None