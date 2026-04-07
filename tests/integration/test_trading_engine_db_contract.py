from __future__ import annotations

from database import Database
from core.trading_engine import TradingEngine, STATUS_SUBMITTED, STATUS_DUPLICATE_BLOCKED


class _Bus:
    def subscribe(self, *_args, **_kwargs):
        return None

    def publish(self, *_args, **_kwargs):
        return None


class _Client:
    def place_bet(self, **_payload):
        return {"bet_id": "BET-DB-CONTRACT"}


class _Executor:
    def is_ready(self):
        return True

    def submit(self, _operation_name, fn):
        return fn()


def _make_engine(db: Database) -> TradingEngine:
    return TradingEngine(
        bus=_Bus(),
        db=db,
        client_getter=lambda: _Client(),
        executor=_Executor(),
    )


def test_real_database_contract_read_write_and_duplicate_flow(tmp_path):
    db = Database(str(tmp_path / "contract.sqlite3"))
    engine = _make_engine(db)

    first = engine.submit_quick_bet(
        {
            "customer_ref": "CREF-DB-1",
            "correlation_id": "CID-DB-1",
            "event_key": "EVT-DB",
            "simulation_mode": False,
        }
    )

    first_order = db.get_order(first["order_id"])
    assert first_order["status"] == STATUS_SUBMITTED
    assert db.order_exists_inflight(customer_ref="CREF-DB-1") is True
    assert db.find_duplicate_order(customer_ref="CREF-DB-1") == first["order_id"]

    second = engine.submit_quick_bet(
        {
            "customer_ref": "CREF-DB-1",
            "correlation_id": "CID-DB-2",
            "event_key": "EVT-DB",
            "simulation_mode": False,
        }
    )

    assert second["status"] == STATUS_DUPLICATE_BLOCKED
    assert db.load_pending_customer_refs() == ["CREF-DB-1"]
    assert db.load_pending_correlation_ids() == ["CID-DB-1"]
