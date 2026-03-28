import tempfile
import threading
from pathlib import Path

import pytest

from database import Database


@pytest.mark.chaos
@pytest.mark.core
@pytest.mark.concurrency
def test_concurrent_writes_do_not_corrupt_settings():
    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))

        def worker(i):
            db._set_setting(f"k{i}", f"v{i}")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(30)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        settings = db.get_settings()
        for i in range(30):
            assert settings[f"k{i}"] == f"v{i}"


@pytest.mark.chaos
@pytest.mark.core
@pytest.mark.recovery
def test_close_and_reopen_multiple_times_keeps_consistency():
    with tempfile.TemporaryDirectory() as td:
        db_path = Path(td) / "db.sqlite"
        db = Database(str(db_path))

        db.save_settings({"a": "1"})
        for _ in range(5):
            db.close_all_connections()
            db.reopen()

        settings = db.get_settings()
        assert settings["a"] == "1"