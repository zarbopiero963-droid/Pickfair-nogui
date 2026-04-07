import pytest

from order_manager import OrderManager, OrderStatus, DuplicateOrderError


class _DB:
    def __init__(self):
        self.sagas = {}
        self.by_logical = {}
        self.transitions = []

    def create_order_saga(self, **kwargs):
        ref = kwargs["customer_ref"]
        self.sagas[ref] = dict(kwargs)
        self.by_logical[kwargs["logical_key"]] = self.sagas[ref]

    def get_order_saga(self, customer_ref):
        return self.sagas.get(customer_ref)

    def get_order_saga_by_logical_key(self, logical_key):
        return self.by_logical.get(logical_key)

    def update_order_saga(self, **kwargs):
        ref = kwargs["customer_ref"]
        row = self.sagas[ref]
        row.update(kwargs)
        self.transitions.append((ref, row.get("status")))


class _Client:
    def __init__(self, leg_status="SUCCESS", matched=0.0, cancel_ok=True, replace_ok=True):
        self.leg_status = leg_status
        self.matched = matched
        self.cancel_ok = cancel_ok
        self.replace_ok = replace_ok

    def place_bet(self, **_kwargs):
        return {
            "status": "SUCCESS",
            "instructionReports": [{"status": self.leg_status, "betId": "B1", "sizeMatched": self.matched}],
        }

    def cancel_orders(self, **_kwargs):
        return {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS" if self.cancel_ok else "FAILURE", "sizeCancelled": 1.0}]}

    def replace_orders(self, **_kwargs):
        return {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS" if self.replace_ok else "FAILURE", "betId": "B2"}]}


@pytest.mark.integration
@pytest.mark.parametrize("simulation_mode", [False, True])
def test_order_manager_lifecycle_and_terminal_parity(simulation_mode):
    db = _DB()
    om = OrderManager(db=db, client_getter=lambda: _Client(leg_status="SUCCESS", matched=2.0), sleep_fn=lambda *_: None)

    payload = {"market_id": "1.1", "selection_id": 7, "price": 2.0, "stake": 5.0, "simulation_mode": simulation_mode, "customer_ref": f"REF-{simulation_mode}"}
    placed = om.place_order(payload)
    cancelled = om.cancel_order(customer_ref=placed["customer_ref"], bet_id="B1", market_id="1.1")

    assert placed["status"] == OrderStatus.PARTIALLY_MATCHED.value
    assert cancelled["status"] == OrderStatus.CANCELLED.value
    assert db.get_order_saga(placed["customer_ref"])["status"] == OrderStatus.CANCELLED.value


@pytest.mark.integration
def test_order_manager_replace_and_idempotency_same_live_sim_contract():
    payload = {"market_id": "1.1", "selection_id": 7, "price": 2.0, "stake": 5.0, "customer_ref": "DEDUP"}

    outcomes = []
    for sim in (False, True):
        db = _DB()
        om = OrderManager(db=db, client_getter=lambda: _Client(), sleep_fn=lambda *_: None)
        placed = om.place_order({**payload, "simulation_mode": sim})
        replaced = om.replace_order(customer_ref=placed["customer_ref"], bet_id="B1", market_id="1.1", new_price=2.2)
        with pytest.raises(DuplicateOrderError):
            om.place_order({**payload, "simulation_mode": sim, "customer_ref": "DEDUP-2"})
        outcomes.append((placed["status"], replaced["status"], list(db.transitions)))

    assert outcomes[0][0:2] == outcomes[1][0:2] == (OrderStatus.PLACED.value, OrderStatus.PLACED.value)
    assert outcomes[0][2] == outcomes[1][2]
