import pytest


class FakeBus:
    def __init__(self):
        self.subscriptions = {}

    def subscribe(self, topic, handler):
        self.subscriptions[topic] = handler

    def publish(self, _name, _payload):
        pass


class FakeDB:
    def __init__(self, duplicate=False):
        self.duplicate = duplicate

    def is_ready(self):
        return True

    def order_exists_inflight(self, *, customer_ref, correlation_id):
        return self.duplicate

    def load_pending_customer_refs(self):
        return []

    def load_pending_correlation_ids(self):
        return []


class InlineExecutor:
    def is_ready(self):
        return True

    def submit(self, _name, fn):
        return fn()


@pytest.mark.invariant
def test_dedup_property_same_customer_ref_is_blocked():
    from core.trading_engine import TradingEngine, _ExecutionContext

    engine = TradingEngine(
        bus=FakeBus(),
        db=FakeDB(duplicate=False),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    with engine._lock:
        engine._inflight_keys.add("CR-A")

    ctx = _ExecutionContext(
        correlation_id="CID-A",
        customer_ref="CR-A",
        created_at=0.0,
    )

    assert engine._dedup_allow(ctx) is False


@pytest.mark.invariant
def test_dedup_property_same_correlation_id_is_blocked():
    from core.trading_engine import TradingEngine, _ExecutionContext

    engine = TradingEngine(
        bus=FakeBus(),
        db=FakeDB(duplicate=False),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    with engine._lock:
        engine._seen_correlation_ids.add("CID-X")
        engine._seen_cid_order.append("CID-X")

    ctx = _ExecutionContext(
        correlation_id="CID-X",
        customer_ref="CR-X",
        created_at=0.0,
    )

    assert engine._dedup_allow(ctx) is False


@pytest.mark.invariant
def test_dedup_property_db_duplicate_is_blocked_even_if_ram_is_empty():
    from core.trading_engine import TradingEngine, _ExecutionContext

    engine = TradingEngine(
        bus=FakeBus(),
        db=FakeDB(duplicate=True),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    ctx = _ExecutionContext(
        correlation_id="CID-DB",
        customer_ref="CR-DB",
        created_at=0.0,
    )

    assert engine._dedup_allow(ctx) is False


@pytest.mark.invariant
def test_dedup_property_new_request_is_admitted_and_registered():
    from core.trading_engine import TradingEngine, _ExecutionContext

    engine = TradingEngine(
        bus=FakeBus(),
        db=FakeDB(duplicate=False),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )

    ctx = _ExecutionContext(
        correlation_id="CID-NEW",
        customer_ref="CR-NEW",
        created_at=0.0,
    )

    assert engine._dedup_allow(ctx) is True
    assert "CR-NEW" in engine._inflight_keys
    assert "CID-NEW" in engine._seen_correlation_ids