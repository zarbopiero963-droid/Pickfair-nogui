import pytest

from core.dutching_batch_manager import DutchingBatchManager


class Bus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload):
        self.events.append((name, payload))


class DB:
    def __init__(self):
        self.calls = []

    def _execute(self, query, params=(), fetch=False, commit=True):
        self.calls.append((" ".join(query.split()), params, fetch, commit))
        if "SELECT * FROM dutching_batches WHERE batch_id =" in query:
            return [{
                "id": 1,
                "batch_id": params[0],
                "event_key": "EK",
                "market_id": "1.1",
                "event_name": "",
                "market_name": "",
                "table_id": None,
                "strategy": "DUTCHING",
                "status": "PENDING",
                "total_legs": 0,
                "placed_legs": 0,
                "matched_legs": 0,
                "failed_legs": 0,
                "cancelled_legs": 0,
                "batch_exposure": 0.0,
                "avg_profit": 0.0,
                "book_pct": 0.0,
                "payload_json": "{}",
                "notes": "",
                "created_at": "x",
                "updated_at": "x",
                "closed_at": None,
            }]
        if "SELECT * FROM dutching_batch_legs" in query:
            return []
        return []


@pytest.mark.integration
def test_create_batch_hits_db_and_bus():
    db = DB()
    bus = Bus()
    mgr = DutchingBatchManager(db, bus=bus)

    batch = mgr.create_batch(batch_id="B10", event_key="EK", market_id="1.1")

    assert batch["batch_id"] == "B10"
    assert any("INSERT INTO dutching_batches" in call[0] for call in db.calls)
    assert bus.events[-1][0] == "DUTCHING_BATCH_CREATED"