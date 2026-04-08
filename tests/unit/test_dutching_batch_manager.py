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
            return self._handle_insert_dutching_batches(normalized, params)

        if normalized.startswith("update dutching_batches"):
            return self._handle_update_dutching_batches(normalized, params)

        if "from dutching_batches" in normalized:
            return self._handle_select_dutching_batches(normalized, params)

        if normalized.startswith("insert into dutching_batch_legs"):
            return self._handle_insert_dutching_batch_legs(normalized, params)

        if normalized.startswith("update dutching_batch_legs"):
            return self._handle_update_dutching_batch_legs(normalized, params)

        if "from dutching_batch_legs" in normalized:
            return self._handle_select_dutching_batch_legs(normalized, params)

        raise AssertionError(f"Unsupported fake DB query (normalized): {normalized}")

    @staticmethod
    def _extract_insert_columns(normalized):
        if "(" not in normalized or ")" not in normalized:
            raise AssertionError(f"Unsupported INSERT query shape (normalized): {normalized}")
        first_open = normalized.find("(")
        first_close = normalized.find(")", first_open + 1)
        if first_close <= first_open:
            raise AssertionError(f"Unsupported INSERT query shape (normalized): {normalized}")
        cols = normalized[first_open + 1 : first_close]
        return [col.strip() for col in cols.split(",") if col.strip()]

    @staticmethod
    def _extract_set_columns(normalized):
        marker = " set "
        where_marker = " where "
        set_start = normalized.find(marker)
        if set_start < 0:
            raise AssertionError(f"Unsupported UPDATE query shape (normalized): {normalized}")
        where_start = normalized.find(where_marker, set_start + len(marker))
        set_clause = normalized[set_start + len(marker) : where_start if where_start > 0 else len(normalized)]
        updates = []
        for part in set_clause.split(","):
            left = part.split("=", 1)[0].strip()
            if left:
                updates.append(left)
        return updates

    def _handle_insert_dutching_batches(self, normalized, params):
        columns = self._extract_insert_columns(normalized)
        if len(columns) != len(params):
            raise AssertionError(
                f"INSERT dutching_batches columns/params mismatch (normalized): {normalized} columns={len(columns)} params={len(params)}"
            )
        incoming = dict(zip(columns, params))
        batch_id = incoming.get("batch_id")
        if not batch_id:
            raise AssertionError(f"INSERT dutching_batches missing batch_id (normalized): {normalized}")
        existing = self.batches.get(batch_id, {})
        self.batches[batch_id] = {
            **existing,
            "id": existing.get("id", len(self.batches) + 1),
            "batch_id": batch_id,
            "event_key": incoming.get("event_key", existing.get("event_key", "")),
            "market_id": incoming.get("market_id", existing.get("market_id", "")),
            "event_name": incoming.get("event_name", existing.get("event_name", "")),
            "market_name": incoming.get("market_name", existing.get("market_name", "")),
            "table_id": incoming.get("table_id", existing.get("table_id", -1)),
            "strategy": incoming.get("strategy", existing.get("strategy", "equal_profit")),
            "status": existing.get("status", incoming.get("status", "PENDING")),
            "total_legs": incoming.get("total_legs", existing.get("total_legs", 0)),
            "placed_legs": existing.get("placed_legs", 0),
            "matched_legs": existing.get("matched_legs", 0),
            "failed_legs": existing.get("failed_legs", 0),
            "cancelled_legs": existing.get("cancelled_legs", 0),
            "batch_exposure": incoming.get("batch_exposure", existing.get("batch_exposure", 0.0)),
            "avg_profit": incoming.get("avg_profit", existing.get("avg_profit", 0.0)),
            "book_pct": incoming.get("book_pct", existing.get("book_pct", 0.0)),
            "payload_json": incoming.get("payload_json", existing.get("payload_json", "{}")),
            "notes": existing.get("notes", ""),
            "created_at": existing.get("created_at", incoming.get("created_at", "")),
            "updated_at": incoming.get("updated_at", existing.get("updated_at", "")),
            "closed_at": existing.get("closed_at"),
        }
        return []

    def _handle_update_dutching_batches(self, normalized, params):
        if "where batch_id = ?" not in normalized:
            raise AssertionError(f"Unsupported dutching_batches UPDATE query (normalized): {normalized}")
        if len(params) < 2:
            raise AssertionError(
                f"Unsupported dutching_batches UPDATE params (normalized): {normalized} params={len(params)}"
            )
        update_columns = self._extract_set_columns(normalized)
        expected_params = len(update_columns) + 1
        if len(params) != expected_params:
            raise AssertionError(
                f"Unsupported dutching_batches UPDATE shape (normalized): {normalized} expected_params={expected_params} actual={len(params)}"
            )

        batch_id = params[-1]
        batch = self.batches.get(batch_id)
        if not batch:
            raise AssertionError(
                f"Cannot UPDATE dutching_batches missing batch_id={batch_id} (normalized): {normalized}"
            )

        for col, val in zip(update_columns, params[:-1]):
            batch[col] = val
        return []

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

    def _handle_insert_dutching_batch_legs(self, normalized, params):
        columns = self._extract_insert_columns(normalized)
        if len(columns) != len(params):
            raise AssertionError(
                f"INSERT dutching_batch_legs columns/params mismatch (normalized): {normalized} columns={len(columns)} params={len(params)}"
            )
        incoming = dict(zip(columns, params))
        batch_id = incoming.get("batch_id")
        leg_index = incoming.get("leg_index")
        if batch_id is None or leg_index is None:
            raise AssertionError(f"INSERT dutching_batch_legs missing batch_id/leg_index (normalized): {normalized}")
        existing = next(
            (item for item in self.legs if item["batch_id"] == batch_id and item["leg_index"] == leg_index),
            None,
        )
        row = {
            "id": existing["id"] if existing else len(self.legs) + 1,
            "batch_id": batch_id,
            "leg_index": leg_index,
            "customer_ref": incoming.get("customer_ref", existing["customer_ref"] if existing else ""),
            "market_id": incoming.get("market_id", existing["market_id"] if existing else ""),
            "selection_id": str(incoming.get("selection_id", existing["selection_id"] if existing else "")),
            "side": incoming.get("side", existing["side"] if existing else ""),
            "price": incoming.get("price", existing["price"] if existing else 0.0),
            "stake": incoming.get("stake", existing["stake"] if existing else 0.0),
            "liability": incoming.get("liability", existing["liability"] if existing else 0.0),
            "bet_id": existing["bet_id"] if existing else "",
            "status": incoming.get("status", existing["status"] if existing else "CREATED"),
            "error_text": existing["error_text"] if existing else "",
            "raw_response_json": existing["raw_response_json"] if existing else "{}",
            "created_at": existing["created_at"] if existing else incoming.get("created_at", ""),
            "updated_at": incoming.get("updated_at", existing["updated_at"] if existing else ""),
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
        if "where batch_id = ?" not in normalized or "leg_index = ?" not in normalized:
            raise AssertionError(f"Unsupported dutching_batch_legs UPDATE query (normalized): {normalized}")
        update_columns = self._extract_set_columns(normalized)
        expected_params = len(update_columns) + 2
        if len(params) != expected_params:
            raise AssertionError(
                f"Unsupported dutching_batch_legs UPDATE shape (normalized): {normalized} expected_params={expected_params} actual={len(params)}"
            )
        batch_id, leg_index = params[-2], params[-1]
        for leg in self.legs:
            if leg["batch_id"] == batch_id and leg["leg_index"] == leg_index:
                for col, val in zip(update_columns, params[:-2]):
                    leg[col] = val
                if "customer_ref" in update_columns and "status" not in update_columns:
                    leg["status"] = "SUBMITTED"
                return []
        raise AssertionError(
            f"Cannot UPDATE dutching_batch_legs missing row batch_id={batch_id} leg_index={leg_index} (normalized): {normalized}"
        )

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


@pytest.mark.unit
def test_fakedb_update_updated_at_only_is_supported():
    db = FakeDB()
    mgr = DutchingBatchManager(db)
    mgr.create_batch(batch_id="B8", event_key="E8", market_id="1.888")

    db._execute(
        """
        UPDATE dutching_batches
        SET updated_at = ?
        WHERE batch_id = ?
        """,
        ("2024-03-01T00:00:00", "B8"),
    )

    row = db._execute("SELECT * FROM dutching_batches WHERE batch_id = ?", ("B8",), fetch=True, commit=False)[0]
    assert row["updated_at"] == "2024-03-01T00:00:00"


@pytest.mark.unit
def test_fakedb_leg_insert_with_column_list_and_update_roundtrip():
    db = FakeDB()
    db._execute(
        """
        INSERT INTO dutching_batch_legs (
            batch_id, leg_index, market_id, selection_id, side,
            price, stake, liability, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("B9", 1, "1.999", 123, "BACK", 2.5, 10.0, 0.0, "CREATED", "t0", "t0"),
    )
    db._execute(
        """
        UPDATE dutching_batch_legs
        SET status = ?, bet_id = ?, error_text = ?, raw_response_json = ?, updated_at = ?
        WHERE batch_id = ? AND leg_index = ?
        """,
        ("MATCHED", "BET-1", "", '{"ok": true}', "t1", "B9", 1),
    )

    row = db._execute(
        "SELECT * FROM dutching_batch_legs WHERE batch_id = ? ORDER BY leg_index ASC",
        ("B9",),
        fetch=True,
        commit=False,
    )[0]
    assert row["status"] == "MATCHED"
    assert row["bet_id"] == "BET-1"
    assert row["selection_id"] == "123"
