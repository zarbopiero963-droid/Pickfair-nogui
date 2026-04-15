import pytest

from core.async_db_writer import AsyncDBWriter


class _AuditDB:
    def __init__(self):
        self.events = []

    def insert_audit_event(self, event):
        self.events.append(dict(event))


class _NoAuditDB:
    pass


def test_async_db_writer_exposes_runtime_write_contract_and_persists_event():
    db = _AuditDB()
    writer = AsyncDBWriter(db)
    writer.start()
    try:
        assert callable(getattr(writer, "write", None))

        event = {
            "event_id": "evt-1",
            "type": "order_submitted",
            "payload": {"order_id": "o-1"},
        }
        assert writer.write(event) is True

        writer.queue.join()
        assert db.events and db.events[0]["event_id"] == "evt-1"
    finally:
        writer.stop()


def test_async_db_writer_write_fails_fast_on_contract_mismatch():
    writer = AsyncDBWriter(_NoAuditDB())
    with pytest.raises(AttributeError, match="audit contract mismatch"):
        writer.write({"event_id": "evt-2"})


def test_async_db_writer_write_rejects_non_mapping_payload():
    writer = AsyncDBWriter(_AuditDB())
    with pytest.raises(TypeError, match="expects a dict"):
        writer.write("not-an-event")


def test_async_db_writer_pressure_snapshot_exposes_backpressure_counters():
    writer = AsyncDBWriter(_AuditDB())
    snap = writer.pressure_snapshot()
    assert "submitted" in snap
    assert "queue_high_watermark" in snap
    assert "seconds_since_last_submit" in snap
