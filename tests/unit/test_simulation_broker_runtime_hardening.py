"""Static-analysis friendly runtime hardening tests for simulation broker."""

import unittest

from simulation_broker import SimulationBroker, SimulationState


class TestSimBrokerRuntime(unittest.TestCase):
    """Covers malformed runtime input hardening scenarios."""

    @staticmethod
    def make_book(selection_id=10, lay_size=10.0):
        """Build a minimal market book fixture."""
        return {
            "marketId": "1.100",
            "runners": [{"selectionId": selection_id, "ex": {
                "availableToBack": [{"price": 1.99, "size": 10.0}],
                "availableToLay": [{"price": 2.0, "size": lay_size}],
            }}],
        }

    def make_broker(self, lay_size=10.0):
        """Create a broker with seeded liquidity."""
        broker = SimulationBroker(starting_balance=1000.0, partial_fill_enabled=True, consume_liquidity=True)
        broker.update_market_book(self.make_book(lay_size=lay_size))
        return broker

    @staticmethod
    def first_report(output):
        """Return first instruction report from place_orders/place_bet output."""
        return output["instructionReports"][0]

    @staticmethod
    def stored_order(broker, report):
        """Resolve stored order from an instruction report."""
        return broker.state.orders[report["betId"]]

    def assert_invalid_order_unchanged(self, broker, report):
        """Validate deterministic invalid-order outcome."""
        order = self.stored_order(broker, report)
        self.assertEqual(report["status"], "FAILURE")
        self.assertEqual(order.matched_size, 0.0)
        self.assertEqual(order.status, "EXECUTABLE")

    def test_load_invalid_numbers(self):
        """Malformed state restore values should not crash."""
        state = SimulationState(starting_balance=1000.0, commission_pct=4.5)
        payload = {
            "starting_balance": float("nan"),
            "balance": float("inf"),
            "orders": {"b1": {"selection_id": "bad", "price": "nan?", "size": None}},
            "position_ledgers": {"1.1::x": {"market_id": "1.1", "runner_id": "bad"}},
        }
        state.load_from_dict(payload)
        self.assertEqual(state.balance, state.starting_balance)
        self.assertEqual(state.orders["b1"].selection_id, 0)
        self.assertEqual(state.orders["b1"].size, 0.0)
        self.assertNotIn("1.1::x", state.position_ledgers)

    def test_partial_cancel_state(self):
        """Partial fill then cancel should keep coherent state."""
        broker = self.make_broker(lay_size=2.0)
        out = broker.place_bet(market_id="1.100", selection_id=10, side="BACK", price=2.0, size=5.0)
        report = self.first_report(out)
        order = self.stored_order(broker, report)
        self.assertEqual(order.matched_size, 2.0)
        self.assertEqual(order.status, "EXECUTABLE")
        cancel = broker.cancel_orders(market_id="1.100", instructions=[{"betId": report["betId"]}])
        self.assertEqual(cancel["instructionReports"][0]["status"], "SUCCESS")
        current = broker.list_current_orders(market_ids=["1.100"])["currentOrders"][0]
        self.assertEqual(current["status"], "CANCELLED")
        self.assertEqual(current["sizeMatched"], 2.0)
        self.assertEqual(current["sizeRemaining"], 3.0)

    def test_none_selection_fails(self):
        """None selection should fail match without crashing."""
        broker = self.make_broker()
        out = broker.place_orders(
            market_id="1.100",
            instructions=[{"selectionId": None, "side": "BACK", "price": 2.0, "size": 1.0}],
        )
        report = self.first_report(out)
        self.assert_invalid_order_unchanged(broker, report)
        self.assertEqual(self.stored_order(broker, report).selection_id, 0)

    def test_bad_selection_fails(self):
        """String selection id should fail match without crashing."""
        broker = self.make_broker()
        out = broker.place_orders(
            market_id="1.100",
            instructions=[{"selectionId": "bad", "side": "BACK", "price": 2.0, "size": 1.0}],
        )
        report = self.first_report(out)
        self.assert_invalid_order_unchanged(broker, report)
        self.assertEqual(self.stored_order(broker, report).selection_id, 0)

    def test_conflict_selection_fails(self):
        """Conflicting/invalid selection keys should fail deterministically."""
        broker = self.make_broker()
        out = broker.place_orders(
            market_id="1.100",
            instructions=[{"selection_id": None, "selectionId": "bad", "side": "BACK", "price": 2.0, "size": 1.0}],
        )
        self.assert_invalid_order_unchanged(broker, self.first_report(out))

    def test_bad_price_size_fails(self):
        """Bad price/size/stake should not crash or move funds."""
        broker = self.make_broker()
        before = broker.get_account_funds()
        out = broker.place_orders(market_id="1.100", instructions=[
            {"selectionId": 10, "side": "BACK", "price": "bad", "size": 1.0},
            {"selectionId": 10, "side": "BACK", "price": 2.0, "size": "bad"},
            {"selectionId": 10, "side": "BACK", "price": 2.0, "stake": "bad"},
        ])
        for report in out["instructionReports"]:
            self.assert_invalid_order_unchanged(broker, report)
        after = broker.get_account_funds()
        self.assertEqual(after["available"], before["available"])
        self.assertEqual(after["exposure"], before["exposure"])


if __name__ == "__main__":
    unittest.main()
