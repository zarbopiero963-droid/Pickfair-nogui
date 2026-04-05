import pytest

from core.trading_engine import TradingEngine
from observability.metrics_registry import MetricsRegistry


class DummyBus:
    def __init__(self):
        self.subscriptions = {}

    def subscribe(self, topic, handler):
        self.subscriptions[topic] = handler

    def publish(self, *_args, **_kwargs):
        return None


class DummyDB:
    def __init__(self):
        self.orders = {}
        self.seq = 0

    def is_ready(self):
        return True

    def insert_order(self, payload):
        self.seq += 1
        oid = f"OID-{self.seq}"
        self.orders[oid] = dict(payload)
        return oid

    def update_order(self, order_id, update):
        self.orders[order_id].update(dict(update))

    def get_order(self, order_id):
        return dict(self.orders[order_id])

    def insert_audit_event(self, *_args, **_kwargs):
        return None

    def load_pending_customer_refs(self):
        return []

    def load_pending_correlation_ids(self):
        return []

    def order_exists_inflight(self, **_kwargs):
        return False

    def find_duplicate_order(self, **_kwargs):
        return None


class InlineExecutor:
    def is_ready(self):
        return True

    def submit(self, _name, fn):
        return fn()


class OkOrderManager:
    def submit(self, payload):
        _ = payload
        return {"ok": True, "bet_id": "BET-1"}


@pytest.mark.smoke
def test_trading_engine_metrics_increment_on_submit_path():
    bus = DummyBus()
    db = DummyDB()

    engine = TradingEngine(
        bus=bus,
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    engine.order_manager = OkOrderManager()
    engine.metrics_registry = MetricsRegistry()

    result = engine.submit_quick_bet(
        {
            "market_id": "1.100",
            "selection_id": 11,
            "price": 2.0,
            "size": 5.0,
            "side": "BACK",
            "customer_ref": "METRIC-SMOKE-1",
        }
    )

    assert result["ok"] is True
    snap = engine.metrics_registry.snapshot()
    assert snap["counters"].get("quick_bet_requests_total", 0) >= 1
    assert snap["counters"].get("quick_bet_accepted_total", 0) >= 1
