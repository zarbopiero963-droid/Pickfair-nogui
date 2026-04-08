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

    def _normalize_query(self, query):
        return " ".join(str(query).split()).strip().lower()

    def _execute(self, query, params=(), fetch=False, commit=True):
        normalized = self._normalize_query(query)

        if normalized.startswith("create table") or normalized.startswith("create index"):
            return []

        if normalized.startswith("insert into dutching_batches"):
            return self._handle_insert_dutching_batches(params)

        if normalized.startswith("update dutching_batches"):
            return self._handle_update_dutching_batches(normalized, params)

        if "from dutching_batches" in normalized:
            return self._handle_select_dutching_batches(normalized, params)

        if normalized.startswith("insert into dutching_batch_legs"):
            return self._handle_insert_dutching_batch_legs(params)

        if normalized.startswith("update dutching_batch_legs"):
            return self._handle_update_dutching_batch_legs(normalized, params)

        if "from dutching_batch_legs" in normalized:
            return self._handle_select_dutching_batch_legs(normalized, params)

        raise AssertionError(f"Unsupported fake DB query (normalized): {normalized}")

    def _handle_insert_dutching_batches(self, params):
        (
            batch_id,
            event_key,
            market_id,
            event_name,
            market_name,
            table_id,
            strategy,
            status,
            total_legs,
            batch_exposure,
            avg_profit,
            book_pct,
            payload_json,
            created_at,
            updated_at,
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

    def _handle_update_dutching_batches(self, normalized, params):
        if "set status =" in normalized and "closed_at =" in normalized:
            status, notes, updated_at, closed_at, batch_id = params
            batch = self.batches[batch_id]
            batch["status"] = status
            batch["notes"] = notes
            batch["updated_at"] = updated_at
            batch["closed_at"] = closed_at
            return []

        if "set status =" in normalized:
            status, notes, updated_at, batch_id = params
            batch = self.batches[batch_id]
            batch["status"] = status
            batch["notes"] = notes
            batch["updated_at"] = updated_at
            return []

        if "set payload_json =" in normalized and "where batch_id = ?" in normalized:
            payload_json, updated_at, batch_id = params
            if batch_id in self.batches:
                self.batches[batch_id]["payload_json"] = payload_json
                self.batches[batch_id]["updated_at"] = updated_at
            return []

        if "set total_legs =" in normalized:
            total_legs, placed_legs, matched_legs, failed_legs, cancelled_legs, updated_at, batch_id = params
            batch = self.batches[batch_id]
            batch["total_legs"] = total_legs
            batch["placed_legs"] = placed_legs
            batch["matched_legs"] = matched_legs
            batch["failed_legs"] = failed_legs
            batch["cancelled_legs"] = cancelled_legs
            batch["updated_at"] = updated_at
            return []

        if "set updated_at = ?" in normalized and "where batch_id = ?" in normalized:
            updated_at, batch_id = params
            if batch_id in self.batches:
                self.batches[batch_id]["updated_at"] = updated_at
            return []

        raise AssertionError(f"Unsupported dutching_batches UPDATE query (normalized): {normalized}")

    def _handle_select_dutching_batches(self, normalized, params):
        if "where batch_id = ?" in normalized:
            batch_id = params[0]
            row = self.batches.get(batch_id)
            return [row] if row else []

        if "where status in ('pending', 'submitting', 'live', 'partial', 'rollback_pending')" in normalized:
            return [
                batch
                for batch in self.batches.values()
                if batch["status"] in {"PENDING", "SUBMITTING", "LIVE", "PARTIAL", "ROLLBACK_PENDING"}
            ]

        if "where status in ('executed', 'rolled_back', 'failed', 'cancelled')" in normalized:
            limit = int(params[0])
            rows = [
                batch
                for batch in self.batches.values()
                if batch["status"] in {"EXECUTED", "ROLLED_BACK", "FAILED", "CANCELLED"}
            ]
            return rows[:limit]

        if "order by created_at desc, id desc limit ?" in normalized:
            limit = int(params[0])
            rows = list(self.batches.values())
            rows.sort(key=lambda x: ((x.get("created_at") or ""), (x.get("id") or 0)), reverse=True)
            return rows[:limit]

        raise AssertionError(f"Unsupported dutching_batches SELECT query (normalized): {normalized}")

    def _handle_insert_dutching_batch_legs(self, params):
        (
            batch_id,
            leg_index,
            customer_ref,
            market_id,
            selection_id,
            side,
            price,
            stake,
            liability,
            status,
            created_at,
            updated_at,
        ) = params
        existing = next(
            (item for item in self.legs if item["batch_id"] == batch_id and item["leg_index"] == leg_index),
            None,
        )
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
            self.legs = [
                row if item["batch_id"] == batch_id and item["leg_index"] == leg_index else item
                for item in self.legs
            ]
        else:
            self.legs.append(row)
        return []

    def _handle_update_dutching_batch_legs(self, normalized, params):
        if "set customer_ref =" in normalized:
            customer_ref, raw_response_json, updated_at, batch_id, leg_index = params
            for leg in self.legs:
                if leg["batch_id"] == batch_id and leg["leg_index"] == leg_index:
                    leg["customer_ref"] = customer_ref
                    leg["status"] = "SUBMITTED"
                    leg["raw_response_json"] = raw_response_json
                    leg["updated_at"] = updated_at
            return []

        if "set status =" in normalized and "bet_id =" in normalized:
            status, bet_id, error_text, raw_response_json, updated_at, batch_id, leg_index = params
            for leg in self.legs:
                if leg["batch_id"] == batch_id and leg["leg_index"] == leg_index:
                    leg["status"] = status
                    leg["bet_id"] = bet_id
                    leg["error_text"] = error_text
                    leg["raw_response_json"] = raw_response_json
                    leg["updated_at"] = updated_at
            return []

        raise AssertionError(f"Unsupported dutching_batch_legs UPDATE query (normalized): {normalized}")

    def _handle_select_dutching_batch_legs(self, normalized, params):
        if "where batch_id = ?" in normalized:
            batch_id = params[0]
            rows = [item for item in self.legs if item["batch_id"] == batch_id]
            rows.sort(key=lambda x: (x.get("leg_index") or 0, x.get("id") or 0))
            return rows

        if "where customer_ref = ?" in normalized:
            customer_ref = params[0]
            rows = [item for item in self.legs if item["customer_ref"] == customer_ref]
            rows.sort(key=lambda x: x.get("id") or 0, reverse=True)
            return rows[:1]

        raise AssertionError(f"Unsupported dutching_batch_legs SELECT query (normalized): {normalized}")


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


@pytest.mark.unit
def test_fakedb_normalizes_payload_update_query_shape():
    db = FakeDB()
    mgr = DutchingBatchManager(db)
    mgr.create_batch(batch_id="B6", event_key="E6", market_id="1.666")

    db._execute(
        """
        UPDATE dutching_batches
        SET payload_json = ?, updated_at = ?
        WHERE batch_id = ?
        """,
        ('{"foo": 1}', "2024-01-01T00:00:00", "B6"),
    )

    assert db.batches["B6"]["payload_json"] == '{"foo": 1}'
    assert db.batches["B6"]["updated_at"] == "2024-01-01T00:00:00"


@pytest.mark.unit
def test_fakedb_select_after_status_update_consistent():
    db = FakeDB()
    mgr = DutchingBatchManager(db)
    mgr.create_batch(batch_id="B7", event_key="E7", market_id="1.777")

    db._execute(
        """
        UPDATE dutching_batches
        SET status = ?, notes = ?, updated_at = ?
        WHERE batch_id = ?
        """,
        ("LIVE", "ok", "2024-02-01T00:00:00", "B7"),
    )

    rows = db._execute("SELECT * FROM dutching_batches WHERE batch_id = ? LIMIT 1", ("B7",), fetch=True, commit=False)
    assert rows[0]["status"] == "LIVE"
    assert rows[0]["notes"] == "ok"


@pytest.mark.unit
def test_fakedb_unknown_query_fails_loudly_with_normalized_shape():
    db = FakeDB()
    with pytest.raises(AssertionError, match="normalized"):
        db._execute("SELECT count(*) FROM imaginary_table")
