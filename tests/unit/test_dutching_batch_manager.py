import pytest

from core.dutching_batch_manager import DutchingBatchManager


class FakeBus:
    def __init__(self):
        self.events = []

    def publish(self, event_name, payload):
        self.events.append((event_name, payload))


class FakeDB:
    def __init__(self):
        self.batches = {}
        self.legs = []

    def _execute(self, query, params=(), fetch=False, commit=True):
        q = " ".join(query.split())

        if q.startswith("CREATE TABLE") or q.startswith("CREATE INDEX"):
            return []

        if "INSERT INTO dutching_batches" in q:
            (
                batch_id, event_key, market_id, event_name, market_name,
                table_id, strategy, status, total_legs,
                batch_exposure, avg_profit, book_pct,
                payload_json, created_at, updated_at
            ) = params
            existing = self.batches.get(batch_id, {})
            self.batches[batch_id] = {
                **existing,
                "id": existing.get("id", len(self.batches) + 1),
                "batch_id": batch_id,
                "event_key": event_key,
                "market_id": market_id,
                "event_name": event_name,
                "market_name": market_name,
                "table_id": table_id,
                "strategy": strategy,
                "status": existing.get("status", status),
                "total_legs": total_legs,
                "placed_legs": existing.get("placed_legs", 0),
                "matched_legs": existing.get("matched_legs", 0),
                "failed_legs": existing.get("failed_legs", 0),
                "cancelled_legs": existing.get("cancelled_legs", 0),
                "batch_exposure": batch_exposure,
                "avg_profit": avg_profit,
                "book_pct": book_pct,
                "payload_json": payload_json,
                "notes": existing.get("notes", ""),
                "created_at": existing.get("created_at", created_at),
                "updated_at": updated_at,
                "closed_at": existing.get("closed_at"),
            }
            return []

        if "SELECT * FROM dutching_batches WHERE batch_id =" in q:
            batch_id = params[0]
            row = self.batches.get(batch_id)
            return [row] if row else []

        if "SELECT * FROM dutching_batches WHERE status IN ('PENDING', 'SUBMITTING', 'LIVE', 'PARTIAL', 'ROLLBACK_PENDING')" in q:
            return [b for b in self.batches.values() if b["status"] in {"PENDING", "SUBMITTING", "LIVE", "PARTIAL", "ROLLBACK_PENDING"}]

        if "SELECT * FROM dutching_batches WHERE status IN ('EXECUTED', 'ROLLED_BACK', 'FAILED', 'CANCELLED')" in q:
            limit = params[0]
            rows = [b for b in self.batches.values() if b["status"] in {"EXECUTED", "ROLLED_BACK", "FAILED", "CANCELLED"}]
            return rows[:limit]

        if "SELECT * FROM dutching_batches ORDER BY created_at DESC, id DESC LIMIT ?" in q:
            limit = params[0]
            return list(self.batches.values())[:limit]

        if "UPDATE dutching_batches SET status =" in q and "closed_at" in q:
            status, notes, updated_at, closed_at, batch_id = params
            b = self.batches[batch_id]
            b["status"] = status
            b["notes"] = notes
            b["updated_at"] = updated_at
            b["closed_at"] = closed_at
            return []

        if "UPDATE dutching_batches SET status =" in q and "closed_at" not in q:
            status, notes, updated_at, batch_id = params
            b = self.batches[batch_id]
            b["status"] = status
            b["notes"] = notes
            b["updated_at"] = updated_at
            return []

        if "UPDATE dutching_batches SET payload_json =" in q:
            payload_json, updated_at, batch_id = params
            if batch_id in self.batches:
                self.batches[batch_id]["payload_json"] = payload_json
                self.batches[batch_id]["updated_at"] = updated_at
            return []

        if "UPDATE dutching_batches SET total_legs =" in q:
            total_legs, placed_legs, matched_legs, failed_legs, cancelled_legs, updated_at, batch_id = params
            b = self.batches[batch_id]
            b["total_legs"] = total_legs
            b["placed_legs"] = placed_legs
            b["matched_legs"] = matched_legs
            b["failed_legs"] = failed_legs
            b["cancelled_legs"] = cancelled_legs
            b["updated_at"] = updated_at
            return []

        if "INSERT INTO dutching_batch_legs" in q:
            (
                batch_id, leg_index, customer_ref, market_id, selection_id,
                side, price, stake, liability, status, created_at, updated_at
            ) = params
            existing = next((x for x in self.legs if x["batch_id"] == batch_id and x["leg_index"] == leg_index), None)
            row = {
                "id": existing["id"] if existing else len(self.legs) + 1,
                "batch_id": batch_id,
                "leg_index": leg_index,
                "customer_ref": customer_ref,
                "market_id": market_id,
                "selection_id": str(selection_id),
                "side": side,
                "price": price,
                "stake": stake,
                "liability": liability,
                "bet_id": existing["bet_id"] if existing else "",
                "status": status,
                "error_text": existing["error_text"] if existing else "",
                "raw_response_json": existing["raw_response_json"] if existing else "{}",
                "created_at": existing["created_at"] if existing else created_at,
                "updated_at": updated_at,
            }
            if existing:
                self.legs = [row if x["batch_id"] == batch_id and x["leg_index"] == leg_index else x for x in self.legs]
            else:
                self.legs.append(row)
            return []

        if "SELECT * FROM dutching_batch_legs WHERE batch_id =" in q:
            batch_id = params[0]
            return [x for x in self.legs if x["batch_id"] == batch_id]

        if "SELECT * FROM dutching_batch_legs WHERE customer_ref =" in q:
            customer_ref = params[0]
            rows = [x for x in self.legs if x["customer_ref"] == customer_ref]
            return rows[-1:] if rows else []

        if "UPDATE dutching_batch_legs SET customer_ref =" in q:
            customer_ref, raw_response_json, updated_at, batch_id, leg_index = params
            for leg in self.legs:
                if leg["batch_id"] == batch_id and leg["leg_index"] == leg_index:
                    leg["customer_ref"] = customer_ref
                    leg["status"] = "SUBMITTED"
                    leg["raw_response_json"] = raw_response_json
                    leg["updated_at"] = updated_at
            return []

        if "UPDATE dutching_batch_legs SET status =" in q:
            status, bet_id, error_text, raw_response_json, updated_at, batch_id, leg_index = params
            for leg in self.legs:
                if leg["batch_id"] == batch_id and leg["leg_index"] == leg_index:
                    leg["status"] = status
                    leg["bet_id"] = bet_id
                    leg["error_text"] = error_text
                    leg["raw_response_json"] = raw_response_json
                    leg["updated_at"] = updated_at
            return []

        raise AssertionError(f"Query non gestita nel fake DB: {q}")


@pytest.mark.unit
@pytest.mark.guardrail
def test_create_batch_and_publish_created_event():
    db = FakeDB()
    bus = FakeBus()
    mgr = DutchingBatchManager(db, bus=bus)

    batch = mgr.create_batch(
        batch_id="B1",
        event_key="E1",
        market_id="1.111",
        event_name="A v B",
        total_legs=2,
        payload={"x": 1},
    )

    assert batch["batch_id"] == "B1"
    assert batch["payload"] == {"x": 1}
    assert bus.events[0][0] == "DUTCHING_BATCH_CREATED"


@pytest.mark.unit
@pytest.mark.guardrail
def test_create_batch_with_legs_sets_real_total_legs():
    db = FakeDB()
    mgr = DutchingBatchManager(db)

    batch = mgr.create_batch(
        batch_id="B2",
        event_key="E2",
        market_id="1.222",
        total_legs=99,
        legs=[
            {"selectionId": 1, "price": 2.0, "stake": 10},
            {"selectionId": 2, "price": 3.0, "stake": 5},
        ],
    )

    assert batch["total_legs"] == 2


@pytest.mark.unit
@pytest.mark.guardrail
def test_recompute_batch_status_executed_when_all_placed_or_matched():
    db = FakeDB()
    mgr = DutchingBatchManager(db)

    mgr.create_batch(batch_id="B3", event_key="E3", market_id="1.333")
    mgr.create_leg(batch_id="B3", leg_index=1, market_id="1.333", selection_id=1, side="BACK", price=2, stake=10, status="PLACED")
    mgr.create_leg(batch_id="B3", leg_index=2, market_id="1.333", selection_id=2, side="BACK", price=3, stake=10, status="MATCHED")

    batch = mgr.recompute_batch_status("B3")
    assert batch["status"] == "EXECUTED"


@pytest.mark.unit
@pytest.mark.guardrail
def test_recompute_batch_status_partial_on_mixed_success_and_failure():
    db = FakeDB()
    mgr = DutchingBatchManager(db)

    mgr.create_batch(batch_id="B4", event_key="E4", market_id="1.444")
    mgr.create_leg(batch_id="B4", leg_index=1, market_id="1.444", selection_id=1, side="BACK", price=2, stake=10, status="MATCHED")
    mgr.create_leg(batch_id="B4", leg_index=2, market_id="1.444", selection_id=2, side="BACK", price=3, stake=10, status="FAILED")

    batch = mgr.recompute_batch_status("B4")
    assert batch["status"] == "ROLLBACK_PENDING"
    assert batch["payload"]["emergency_hedge"]["triggered"] is True


@pytest.mark.unit
@pytest.mark.guardrail
def test_release_runtime_artifacts_uses_table_id_zero_too():
    db = FakeDB()
    mgr = DutchingBatchManager(db)

    mgr.create_batch(batch_id="B5", event_key="EK5", market_id="1.555", table_id=0)

    class FakeGuard:
        def __init__(self):
            self.released = []

        def release(self, key):
            self.released.append(key)

    class FakeTableManager:
        def __init__(self):
            self.unlocked = []

        def force_unlock(self, table_id):
            self.unlocked.append(table_id)

    g = FakeGuard()
    t = FakeTableManager()

    mgr.release_runtime_artifacts(batch_id="B5", duplication_guard=g, table_manager=t)
    assert g.released == ["EK5"]
    assert t.unlocked == [0]
