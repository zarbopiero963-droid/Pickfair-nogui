from __future__ import annotations

from tests.helpers.fake_exchange import FakeExchange


def test_reconcile_converges_over_multiple_passes_with_explicit_state_advances() -> None:
    exchange = FakeExchange(duplicate_mode="return_existing")
    remote = exchange.place_order(
        {
            "market_id": "1.300",
            "selection_id": 7,
            "price": 1.9,
            "size": 10.0,
            "side": "LAY",
            "customer_ref": "RECON-PASS-1",
        }
    )

    # duplicate customer_ref returns the same remote order and keeps single exposure
    duplicate = exchange.place_order(
        {
            "market_id": "1.300",
            "selection_id": 7,
            "price": 2.0,
            "size": 10.0,
            "side": "LAY",
            "customer_ref": "RECON-PASS-1",
        }
    )
    assert duplicate["bet_id"] == remote["bet_id"]

    local_state = {"status": "AMBIGUOUS", "matched_size": 0.0, "remote_bet_id": None}

    def reconcile_pass() -> None:
        rows = exchange.get_current_orders(customer_ref="RECON-PASS-1")
        if not rows:
            return
        row = rows[0]
        local_state["matched_size"] = row["matched_size"]
        local_state["remote_bet_id"] = row["bet_id"]
        local_state["status"] = row["status"]

    reconcile_pass()
    assert local_state["status"] == "EXECUTABLE"
    assert local_state["remote_bet_id"] == remote["bet_id"]

    exchange.advance_fill(remote["order_id"], new_status="PARTIALLY_MATCHED", matched_size=3.0)
    reconcile_pass()
    assert local_state["status"] == "PARTIALLY_MATCHED"
    assert local_state["matched_size"] == 3.0

    exchange.advance_fill(remote["order_id"], new_status="MATCHED")
    reconcile_pass()
    assert local_state["status"] == "MATCHED"
    assert local_state["matched_size"] == 10.0
