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
            self.batches[batch_id] = {
                "id": len(self.batches) + 1,
                "batch_id": batch_id,
                "event_key": event_key,
                "market_id": market_id,
                "event_name": event_name,
                "market_name": market_name,
                "table_id": table_id,
                "strategy": strategy,
                "status": status,
                "total_legs": total_legs,
                "placed_legs": 0,
                "matched_legs": 0,
                "failed_legs": 0,
                "cancelled_legs": 0,
                "batch_exposure": batch_exposure,
                "avg_profit": avg_profit,
                "book_pct": book_pct,
                "payload_json": payload_json,
                "notes": "",
                "created_at": created_at,
                "updated_at": updated_at,
                "closed_at": None,
            }
            return []

        if "SELECT * FROM dutching_batches WHERE batch_id =" in q:
            row = self.batches.get(params[0])
            return [row] if row else []

        if "UPDATE dutching_batches SET payload_json =" in q:
            payload_json, updated_at, batch_id = params
            self.batches[batch_id]["payload_json"] = payload_json
            self.batches[batch_id]["updated_at"] = updated_at
            return []

        if "INSERT INTO dutching_batch_legs" in q:
            (
                batch_id, leg_index, customer_ref, market_id, selection_id,
                side, price, stake, liability, status, created_at, updated_at
            ) = params
            self.legs.append({
                "id": len(self.legs) + 1,
                "batch_id": batch_id,
                "leg_index": leg_index,
                "customer_ref": customer_ref,
                "market_id": market_id,
                "selection_id": str(selection_id),
                "side": side,
                "price": price,
                "stake": stake,
                "liability": liability,
                "bet_id": "",
                "status": status,
                "error_text": "",
                "raw_response_json": "{}",
                "created_at": created_at,
                "updated_at": updated_at,
            })
            return []

        if "SELECT * FROM dutching_batch_legs WHERE batch_id =" in q:
            return [x for x in self.legs if x["batch_id"] == params[0]]

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

        if "UPDATE dutching_batches SET status =" in q and "closed_at" not in q:
            status, notes, updated_at, batch_id = params
            self.batches[batch_id]["status"] = status
            self.batches[batch_id]["notes"] = notes
            self.batches[batch_id]["updated_at"] = updated_at
            return []

        if "UPDATE dutching_batches SET status =" in q and "closed_at" in q:
            status, notes, updated_at, closed_at, batch_id = params
            self.batches[batch_id]["status"] = status
            self.batches[batch_id]["notes"] = notes
            self.batches[batch_id]["updated_at"] = updated_at
            self.batches[batch_id]["closed_at"] = closed_at
            return []

        return []


def _new_manager():
    db = FakeDB()
    bus = FakeBus()
    mgr = DutchingBatchManager(db, bus=bus)
    mgr.create_batch(
        batch_id="B100",
        event_key="EK100",
        market_id="1.100",
        legs=[
            {"selectionId": 1, "price": 2.0, "stake": 10},
            {"selectionId": 2, "price": 3.0, "stake": 8},
        ],
    )
    return mgr, bus


@pytest.mark.chaos
def test_partial_fill_triggers_emergency_hedge_once():
    mgr, bus = _new_manager()

    mgr.update_leg_status(batch_id="B100", leg_index=1, status="MATCHED", bet_id="BET1")
    mgr.update_leg_status(batch_id="B100", leg_index=2, status="FAILED", error_text="timeout")

    batch = mgr.get_batch("B100")
    assert batch["status"] == "ROLLBACK_PENDING"
    assert batch["payload"]["emergency_hedge"]["triggered"] is True

    command_events = [name for name, _ in bus.events if name == "CMD_DUTCHING_EMERGENCY_HEDGE"]
    assert len(command_events) == 1


@pytest.mark.chaos
def test_full_success_batch_does_not_trigger_hedge():
    mgr, bus = _new_manager()

    mgr.update_leg_status(batch_id="B100", leg_index=1, status="MATCHED", bet_id="BET1")
    mgr.update_leg_status(batch_id="B100", leg_index=2, status="PLACED", bet_id="BET2")

    batch = mgr.get_batch("B100")
    assert batch["status"] == "EXECUTED"
    assert all(name != "CMD_DUTCHING_EMERGENCY_HEDGE" for name, _ in bus.events)


@pytest.mark.chaos
def test_failure_before_live_leg_does_not_trigger_hedge():
    mgr, bus = _new_manager()

    mgr.update_leg_status(batch_id="B100", leg_index=1, status="FAILED", error_text="rejected")
    mgr.update_leg_status(batch_id="B100", leg_index=2, status="FAILED", error_text="rejected")

    batch = mgr.get_batch("B100")
    assert batch["status"] == "FAILED"
    assert all(name != "CMD_DUTCHING_EMERGENCY_HEDGE" for name, _ in bus.events)


@pytest.mark.chaos
def test_repeated_partial_state_is_idempotent_for_hedge_trigger():
    mgr, bus = _new_manager()

    mgr.update_leg_status(batch_id="B100", leg_index=1, status="MATCHED", bet_id="BET1")
    mgr.update_leg_status(batch_id="B100", leg_index=2, status="FAILED", error_text="timeout")
    mgr.recompute_batch_status("B100")

    command_events = [name for name, _ in bus.events if name == "CMD_DUTCHING_EMERGENCY_HEDGE"]
    assert len(command_events) == 1
