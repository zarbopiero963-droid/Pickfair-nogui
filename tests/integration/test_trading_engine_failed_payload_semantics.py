from __future__ import annotations

from database import Database
from core.trading_engine import TradingEngine, STATUS_FAILED


class _Bus:
    def subscribe(self, *_args, **_kwargs):
        return None

    def publish(self, *_args, **_kwargs):
        return None


class _Executor:
    def is_ready(self):
        return True

    def submit(self, _operation_name, fn):
        return fn()


class _FailedClient:
    def place_bet(self, **_payload):
        return {
            "ok": False,
            "status": "FAILED",
            "error": "BROKER_REJECTED",
            "reason_code": "BROKER_REJECTED",
        }


def test_failed_payload_semantics_are_not_acked_and_not_fake_success(tmp_path):
    db = Database(str(tmp_path / "failed-semantics.sqlite3"))
    engine = TradingEngine(
        bus=_Bus(),
        db=db,
        client_getter=lambda: _FailedClient(),
        executor=_Executor(),
    )

    result = engine.submit_quick_bet(
        {
            "customer_ref": "CREF-SEM-1",
            "correlation_id": "CID-SEM-1",
            "event_key": "EVT-SEM",
            "simulation_mode": False,
        }
    )

    assert result["status"] == STATUS_FAILED
    assert result["outcome"] == "FAILURE"
    assert result["is_terminal"] is True
    assert result["finalization_persisted"] is True
    assert "DOWNSTREAM_SEMANTIC_FAILURE" in (result["error"] or "")

    order = db.get_order(result["order_id"])
    assert order["status"] == STATUS_FAILED
    assert order["finalized"] is True
