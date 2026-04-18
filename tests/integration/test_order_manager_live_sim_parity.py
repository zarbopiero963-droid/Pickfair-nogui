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


class _TransientThenSuccessClient:
    def __init__(self):
        self.place_attempts = 0

    def place_bet(self, **_kwargs):
        self.place_attempts += 1
        if self.place_attempts == 1:
            raise TimeoutError("TIMEOUT")
        return {
            "status": "SUCCESS",
            "instructionReports": [{"status": "SUCCESS", "betId": "B1", "sizeMatched": 5.0}],
        }

    def cancel_orders(self, **_kwargs):
        return {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS", "sizeCancelled": 1.0}]}

    def replace_orders(self, **_kwargs):
        return {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS", "betId": "B2"}]}


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


@pytest.mark.integration
def test_order_manager_reject_then_retry_and_retry_policy_contract_parity():
    payload = {"market_id": "1.1", "selection_id": 7, "price": 2.0, "stake": 5.0}
    outcomes = []

    for sim in (False, True):
        db = _DB()
        om_fail = OrderManager(db=db, client_getter=lambda: _Client(leg_status="FAILURE"), sleep_fn=lambda *_: None)
        failed = om_fail.place_order({**payload, "simulation_mode": sim, "customer_ref": f"RETRY-{sim}-1"})

        om_retry = OrderManager(db=db, client_getter=lambda: _Client(leg_status="SUCCESS", matched=5.0), sleep_fn=lambda *_: None)
        retried = om_retry.place_order({**payload, "simulation_mode": sim, "customer_ref": f"RETRY-{sim}-2"})

        om_transient = OrderManager(db=db, client_getter=lambda: _TransientThenSuccessClient(), sleep_fn=lambda *_: None)
        transient = om_transient.place_order({**payload, "simulation_mode": sim, "customer_ref": f"RETRY-{sim}-3", "price": 2.2})
        outcomes.append((failed, retried, transient))

    for ix in (0, 1):
        failed, retried, transient = outcomes[ix]
        assert failed["status"] == OrderStatus.FAILED.value
        assert failed["reason_code"] == "BROKER_REJECTED"
        assert retried["status"] == OrderStatus.MATCHED.value
        assert retried["reason_code"] == "FULLY_MATCHED"
        assert transient["status"] == OrderStatus.MATCHED.value
        assert transient["reason_code"] == "FULLY_MATCHED"

    assert outcomes[0][0]["status"] == outcomes[1][0]["status"]
    assert outcomes[0][1]["status"] == outcomes[1][1]["status"]
    assert outcomes[0][2]["status"] == outcomes[1][2]["status"]


@pytest.mark.integration
@pytest.mark.parametrize("simulation_mode", [False, True])
def test_order_manager_partial_replace_cancel_multi_step_sequence_parity(simulation_mode):
    db = _DB()
    om = OrderManager(
        db=db,
        client_getter=lambda: _Client(leg_status="SUCCESS", matched=2.0, cancel_ok=True, replace_ok=True),
        sleep_fn=lambda *_: None,
    )
    payload = {
        "market_id": "1.2",
        "selection_id": 8,
        "price": 2.1,
        "stake": 6.0,
        "simulation_mode": simulation_mode,
        "customer_ref": f"MULTI-{simulation_mode}",
    }

    placed = om.place_order(payload)
    replaced = om.replace_order(
        customer_ref=placed["customer_ref"], bet_id="B1", market_id="1.2", new_price=2.3
    )
    cancelled = om.cancel_order(customer_ref=placed["customer_ref"], bet_id="B2", market_id="1.2")

    assert placed["status"] == OrderStatus.PARTIALLY_MATCHED.value
    assert replaced["status"] == OrderStatus.PLACED.value
    assert cancelled["status"] == OrderStatus.CANCELLED.value
    assert db.get_order_saga(placed["customer_ref"])["status"] == OrderStatus.CANCELLED.value

@pytest.mark.integration
@pytest.mark.parametrize(
    "origin,payload_meta_key,payload_meta",
    [
        ("PATTERN", "pattern_meta", {"pattern_id": "PT-01", "pattern_version": 2}),
        ("COPY", "copy_meta", {"master_id": "M-1", "copy_group_id": "CG-1", "copy_mode": "mirror"}),
    ],
)
def test_order_manager_persists_origin_metadata_in_saga_payload(origin, payload_meta_key, payload_meta):
    payload = {
        "market_id": "1.1",
        "selection_id": 7,
        "price": 2.0,
        "stake": 5.0,
        "customer_ref": "META-PARITY",
        "order_origin": origin,
        payload_meta_key: payload_meta,
    }

    for sim in (False, True):
        db = _DB()
        om = OrderManager(db=db, client_getter=lambda: _Client(), sleep_fn=lambda *_: None)
        placed = om.place_order({**payload, "simulation_mode": sim})
        saga = db.get_order_saga(placed["customer_ref"])

        assert saga is not None
        persisted_payload = saga.get("payload") or {}
        assert persisted_payload.get("order_origin") == origin
        assert persisted_payload.get(payload_meta_key) == payload_meta
