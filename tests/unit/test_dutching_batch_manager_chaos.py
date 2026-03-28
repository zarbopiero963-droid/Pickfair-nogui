import pytest

from core.dutching_batch_manager import DutchingBatchManager


class FakeBus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload):
        self.events.append((name, payload))


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
            row = self.batches.get(params[0])
            return [row] if row else []

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
            rows = [x for x in self.legs if x["batch_id"] == params[0]]
            rows.sort(key=lambda x: (x["leg_index"], x["id"]))
            return rows

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

        return []


@pytest.mark.core
@pytest.mark.chaos
@pytest.mark.invariant
def test_dutching_batch_manager_mixed_illegalish_states_fall_back_to_live():
    db = FakeDB()
    mgr = DutchingBatchManager(db)

    mgr.create_batch(batch_id="BX", event_key="EX", market_id="1.X")
    mgr.create_leg(batch_id="BX", leg_index=1, market_id="1.X", selection_id=1, side="BACK", price=2, stake=10, status="SUBMITTED")
    mgr.create_leg(batch_id="BX", leg_index=2, market_id="1.X", selection_id=2, side="BACK", price=3, stake=9, status="MATCHED")
    mgr.create_leg(batch_id="BX", leg_index=3, market_id="1.X", selection_id=3, side="BACK", price=4, stake=8, status="WEIRD_STATUS")

    batch = mgr.recompute_batch_status("BX")
    assert batch["status"] == "LIVE", "Stati misti non riconosciuti devono degradare in LIVE e non rompere il manager"


@pytest.mark.core
@pytest.mark.chaos
@pytest.mark.failure
def test_dutching_batch_manager_repeated_status_updates_do_not_crash():
    db = FakeDB()
    bus = FakeBus()
    mgr = DutchingBatchManager(db, bus=bus)

    mgr.create_batch(batch_id="BY", event_key="EY", market_id="1.Y")
    mgr.create_leg(batch_id="BY", leg_index=1, market_id="1.Y", selection_id=1, side="BACK", price=2, stake=10)

    for _ in range(20):
        mgr.update_leg_status(batch_id="BY", leg_index=1, status="MATCHED", bet_id="BET-Y")

    batch = mgr.get_batch("BY")
    assert batch["status"] == "EXECUTED"
    assert any(name == "DUTCHING_LEG_STATUS_UPDATED" for name, _ in bus.events)


@pytest.mark.core
@pytest.mark.chaos
@pytest.mark.invariant
def test_dutching_batch_manager_terminal_batches_query_only_returns_terminal():
    db = FakeDB()
    mgr = DutchingBatchManager(db)

    mgr.create_batch(batch_id="T1", event_key="E1", market_id="1.1")
    mgr.create_batch(batch_id="T2", event_key="E2", market_id="1.2")
    mgr.update_batch_status("T1", "FAILED", notes="x")
    mgr.update_batch_status("T2", "LIVE", notes="y")

    terminal = mgr.get_terminal_batches(limit=10)
    ids = {x["batch_id"] for x in terminal}
    assert "T1" in ids
    assert "T2" not in ids