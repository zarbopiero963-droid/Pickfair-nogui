from __future__ import annotations

from copy import deepcopy

from core.duplication_guard import DuplicationGuard
from core.state_recovery import StateRecovery


class FakeBus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload):
        self.events.append((name, deepcopy(payload)))


class FakeDB:
    def __init__(self):
        self._startup_orders = {}
        self.acceptance_opened = False

    def startup_order_exists(self, order):
        order_id = str(order.get("order_id") or "")
        return order_id in self._startup_orders

    def upsert_startup_order(self, order):
        order_id = str(order.get("order_id") or "")
        self._startup_orders[order_id] = deepcopy(order)

    def load_startup_orders(self):
        return list(self._startup_orders.values())


class FakeReconcile:
    def __init__(self, orders):
        self.orders = deepcopy(orders)
        self.fetch_calls = 0

    def fetch_startup_active_orders(self):
        self.fetch_calls += 1
        return deepcopy(self.orders)

    def merge_startup_active_orders(self, remote_orders):
        return {"orders": deepcopy(remote_orders), "count": len(remote_orders)}


def _ghost_order():
    return {
        "order_id": "BET-001",
        "market_id": "1.234",
        "selection_id": "55",
        "bet_type": "BACK",
        "source": "pattern",
    }


def test_startup_recovers_remote_missing_db_before_acceptance_path():
    db = FakeDB()
    bus = FakeBus()
    guard = DuplicationGuard(ttl_seconds=999999)
    reconcile = FakeReconcile([_ghost_order()])

    recovery = StateRecovery(db=db, bus=bus, reconciliation_engine=reconcile, duplication_guard=guard)
    result = recovery.recover()

    assert reconcile.fetch_calls == 1
    assert result["ok"] is True
    assert result["ghost_orders_recovered"] == 1
    assert result["startup_mode"] == "LIVE_WITH_RECOVERED_GHOSTS"

    persisted = db.load_startup_orders()
    assert len(persisted) == 1
    assert persisted[0]["order_id"] == "BET-001"



def test_duplicate_after_restart_is_blocked_by_recovered_ghost_order():
    db = FakeDB()
    guard = DuplicationGuard(ttl_seconds=999999)
    reconcile = FakeReconcile([_ghost_order()])

    recovery = StateRecovery(db=db, bus=FakeBus(), reconciliation_engine=reconcile, duplication_guard=guard)
    recovery.recover()

    duplicate_key = guard.build_event_key(_ghost_order())
    assert guard.acquire(duplicate_key) is False



def test_startup_is_not_flat_when_ghost_order_exists():
    recovery = StateRecovery(
        db=FakeDB(),
        bus=FakeBus(),
        reconciliation_engine=FakeReconcile([_ghost_order()]),
        duplication_guard=DuplicationGuard(ttl_seconds=999999),
    )

    result = recovery.recover()

    assert result["ok"] is True
    assert result["startup_mode"] != "LIVE_FLAT"
    assert result["startup_mode"] == "LIVE_WITH_RECOVERED_GHOSTS"
from tests.helpers.fake_exchange import FakeExchange


def test_startup_reconcile_merges_remote_ghost_before_new_submit() -> None:
    exchange = FakeExchange(duplicate_mode="single_exposure")
    exchange.force_timeout_on_next_submit()

    try:
        exchange.place_order(
            {
                "market_id": "1.200",
                "selection_id": 99,
                "price": 3.1,
                "size": 4.0,
                "side": "BACK",
                "customer_ref": "BOOT-GHOST-1",
            }
        )
    except TimeoutError:
        pass

    local_orders: dict[str, dict[str, object]] = {}

    def startup_reconcile() -> None:
        for row in exchange.get_current_orders(statuses=["EXECUTABLE", "PARTIALLY_MATCHED", "MATCHED"]):
            local_orders[row["customer_ref"]] = {
                "status": "RECOVERED",
                "remote_bet_id": row["bet_id"],
                "size": row["size"],
            }

    startup_reconcile()

    assert "BOOT-GHOST-1" in local_orders
    assert local_orders["BOOT-GHOST-1"]["status"] == "RECOVERED"

    duplicate = exchange.place_order(
        {
            "market_id": "1.200",
            "selection_id": 99,
            "price": 3.1,
            "size": 4.0,
            "side": "BACK",
            "customer_ref": "BOOT-GHOST-1",
        }
    )

    assert duplicate["bet_id"] == local_orders["BOOT-GHOST-1"]["remote_bet_id"]
    assert len(exchange.get_current_orders(customer_ref="BOOT-GHOST-1")) == 1
