from __future__ import annotations

from tests.helpers.fake_exchange import FakeExchange


def test_startup_reconcile_merges_remote_ghost_before_new_submit() -> None:
    exchange = FakeExchange(duplicate_mode="single_exposure")

    remote = exchange.place_order(
        {
            "market_id": "1.200",
            "selection_id": 99,
            "price": 3.1,
            "size": 4.0,
            "side": "BACK",
            "customer_ref": "BOOT-GHOST-1",
        }
    )

    local_orders: dict[str, dict[str, object]] = {}

    def startup_reconcile() -> None:
        for row in exchange.get_current_orders():
            local_orders[row["customer_ref"]] = {
                "status": "RECOVERED",
                "remote_bet_id": row["bet_id"],
                "size": row["size"],
            }

    startup_reconcile()

    assert "BOOT-GHOST-1" in local_orders
    assert local_orders["BOOT-GHOST-1"]["remote_bet_id"] == remote["bet_id"]

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

    assert duplicate["bet_id"] == remote["bet_id"]
    assert len(exchange.get_current_orders(customer_ref="BOOT-GHOST-1")) == 1
