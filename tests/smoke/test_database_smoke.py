import tempfile
from pathlib import Path

import pytest


@pytest.mark.smoke
def test_import_database():
    import database  # noqa: F401


@pytest.mark.smoke
def test_construct_database():
    from database import Database

    with tempfile.TemporaryDirectory() as td:
        db = Database(str(Path(td) / "db.sqlite"))
        assert db is not None