import tempfile
from pathlib import Path

import pytest

from database import Database


@pytest.mark.unit
@pytest.mark.core
@pytest.mark.invariant
def test_nested_transaction_rolls_back_inner_failure():
    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))

        with pytest.raises(RuntimeError):
            with db.transaction():
                db._set_setting("a", "1")
                with db.transaction():
                    db._set_setting("b", "2")
                    raise RuntimeError("boom")

        settings = db.get_settings()
        assert settings == {}


@pytest.mark.unit
@pytest.mark.core
@pytest.mark.invariant
def test_execute_inside_transaction_does_not_commit_early():
    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))

        with pytest.raises(RuntimeError):
            with db.transaction():
                db._execute(
                    "INSERT INTO settings(key, value) VALUES(?, ?)",
                    ("x", "1"),
                    commit=True,
                )
                raise RuntimeError("rollback me")

        settings = db.get_settings()
        assert "x" not in settings


@pytest.mark.unit
@pytest.mark.core
@pytest.mark.recovery
def test_reopen_keeps_data():
    with tempfile.TemporaryDirectory() as td:
        db_path = Path(td) / "db.sqlite"
        db = Database(str(db_path))
        db.save_settings({"persist": "yes"})
        db.close_all_connections()
        db.reopen()

        settings = db.get_settings()
        assert settings["persist"] == "yes"


@pytest.mark.unit
@pytest.mark.failure
def test_missing_signal_pattern_raises():
    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))

        with pytest.raises(RuntimeError):
            db.update_signal_pattern(999, label="x")