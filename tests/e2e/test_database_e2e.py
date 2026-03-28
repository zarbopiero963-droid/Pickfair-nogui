import tempfile
from pathlib import Path

import pytest

from database import Database


@pytest.mark.e2e
def test_full_reopen_recovery_flow():
    with tempfile.TemporaryDirectory() as td:
        db_path = Path(td) / "db.sqlite"

        db1 = Database(str(db_path))
        db1.save_settings({"mode": "live"})
        db1.save_session("TOK123", "EXP456")
        db1.create_order_saga(
            customer_ref="REF1",
            batch_id="B1",
            event_key="E1",
            table_id=7,
            market_id="1.1",
            selection_id="10",
            bet_type="BACK",
            price=2.0,
            stake=5.0,
            payload={"foo": "bar"},
            status="PENDING",
        )
        db1.close_all_connections()

        db2 = Database(str(db_path))
        settings = db2.get_settings()
        saga = db2.get_order_saga("REF1")

        assert settings["mode"] == "live"
        assert settings["session_token"] == "TOK123"
        assert saga is not None
        assert saga["status"] == "PENDING"
        assert saga["payload"] == {"foo": "bar"}