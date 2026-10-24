from decimal import Decimal

import pytest

from core.microstake_safe import (
    ExecutionKind,
    MicrostakeRequest,
    MicrostakeCoordinator,
    MicrostakePolicy,
    NativeMicrostakeExecutor,
    JsonSagaStore,
    Side,
    SmartExecutionEngine,
    StatefulExchangeSimulator,
)


D = Decimal


def request(*, side=Side.BACK, stake="0.10", price="2.00", ref="native-1"):
    return MicrostakeRequest(
        market_id="1.100",
        selection_id=7,
        side=side,
        economic_stake=D(stake),
        target_price=D(price),
        customer_ref=ref,
    )


@pytest.mark.parametrize("stake", ["0.10", "0.50", "1.00", "1.99"])
def test_red_microstake_is_selected_automatically_without_feature_flag(stake):
    engine = SmartExecutionEngine(exchange_minimum=D("2.00"))

    plan = engine.plan(
        request(stake=stake),
        passive_stub_price=D("3.00"),
        best_counterparty_price=None,
        transient_exposure_cap=D("10.00"),
    )

    assert plan.kind is ExecutionKind.MICROSTAKE
    assert plan.api_size == D("2.00")
    assert plan.economic_size == D(stake)


@pytest.mark.parametrize(
    "side,stub,best",
    [
        (Side.BACK, "1.01", "2.00"),
        (Side.LAY, "1000", "2.00"),
    ],
)
def test_red_crossing_historical_stub_is_blocked_before_exchange(side, stub, best):
    engine = SmartExecutionEngine(exchange_minimum=D("2.00"))

    plan = engine.plan(
        request(side=side),
        passive_stub_price=D(stub),
        best_counterparty_price=D(best),
        transient_exposure_cap=D("5000"),
    )

    assert plan.kind is ExecutionKind.BLOCKED
    assert plan.reason == "stub_price_would_cross_book"


def test_red_lay_transient_liability_is_checked_not_economic_stake():
    engine = SmartExecutionEngine(exchange_minimum=D("2.00"))

    plan = engine.plan(
        request(side=Side.LAY, stake="0.10", price="3.00"),
        passive_stub_price=D("3.00"),
        best_counterparty_price=None,
        transient_exposure_cap=D("3.99"),
    )

    assert plan.kind is ExecutionKind.BLOCKED
    assert plan.reason == "transient_exposure_cap"
    assert plan.transient_exposure == D("4.00")


def test_red_missing_book_proof_blocks_native_microstake():
    engine = SmartExecutionEngine(exchange_minimum=D("2.00"))

    plan = engine.plan(
        request(),
        passive_stub_price=None,
        best_counterparty_price=None,
        transient_exposure_cap=D("10.00"),
    )

    assert plan.kind is ExecutionKind.BLOCKED
    assert plan.reason == "passive_stub_price_unverified"


def test_red_normal_order_remains_direct():
    engine = SmartExecutionEngine(exchange_minimum=D("2.00"))

    plan = engine.plan(
        request(stake="2.00"),
        passive_stub_price=None,
        best_counterparty_price=None,
        transient_exposure_cap=D("10.00"),
    )

    assert plan.kind is ExecutionKind.DIRECT
    assert plan.api_size == D("2.00")


class _Bus:
    def subscribe(self, *_args):
        pass

    def publish(self, *_args):
        pass


class _DB:
    def __init__(self):
        self.rows = {}

    def is_ready(self):
        return True

    def insert_order(self, payload):
        key = len(self.rows) + 1
        self.rows[key] = dict(payload)
        return key

    def update_order(self, key, update):
        self.rows[key].update(update)

    def get_order(self, key):
        return self.rows.get(key)

    def insert_audit_event(self, _event):
        pass

    def load_pending_customer_refs(self):
        return []

    def load_pending_correlation_ids(self):
        return []

    def order_exists_inflight(self, **_kwargs):
        return False


class _Inline:
    def is_ready(self):
        return True

    def submit(self, _name, fn):
        return fn()


class _DirectClient:
    def __init__(self):
        self.calls = []

    def place_bet(self, **payload):
        self.calls.append(payload)
        return {
            "status": "SUCCESS",
            "instructionReports": [{"status": "SUCCESS", "betId": "DIRECT-1"}],
        }


class _MicroExecutor:
    def __init__(self):
        self.calls = []

    def execute(self, payload):
        self.calls.append(dict(payload))
        return {
            "status": "SUCCESS",
            "instructionReports": [{"status": "SUCCESS", "betId": "MICRO-1"}],
            "microstake": True,
        }


def _engine(micro_executor):
    from core.trading_engine import TradingEngine

    direct = _DirectClient()
    engine = TradingEngine(
        bus=_Bus(), db=_DB(), client_getter=lambda: direct,
        executor=_Inline(), microstake_executor=micro_executor,
    )
    return engine, direct


def test_red_trading_engine_automatically_routes_small_amount_to_microstake():
    micro = _MicroExecutor()
    engine, direct = _engine(micro)

    result = engine.submit_quick_bet({
        "market_id": "1.100", "selection_id": 7, "side": "BACK",
        "price": 2.0, "size": 0.10, "customer_ref": "native-small",
    })

    assert result["ok"] is True
    assert len(micro.calls) == 1
    assert direct.calls == []


def test_red_trading_engine_keeps_exchange_minimum_and_above_on_direct_path():
    micro = _MicroExecutor()
    engine, direct = _engine(micro)

    result = engine.submit_quick_bet({
        "market_id": "1.100", "selection_id": 7, "side": "BACK",
        "price": 2.0, "size": 2.0, "customer_ref": "native-direct",
    })

    assert result["ok"] is True
    assert micro.calls == []
    assert len(direct.calls) == 1


def test_red_small_amount_fails_closed_when_native_executor_is_missing():
    engine, direct = _engine(None)

    result = engine.submit_quick_bet({
        "market_id": "1.100", "selection_id": 7, "side": "BACK",
        "price": 2.0, "size": 0.10, "customer_ref": "native-unavailable",
    })

    assert result["ok"] is False
    assert result["status"] == "FAILED"
    assert "MICROSTAKE_EXECUTOR_UNAVAILABLE" in result["error"]
    assert direct.calls == []


def _native(tmp_path, *, cap="10", quote=None):
    exchange = StatefulExchangeSimulator(balance=D("100"))
    coordinator = MicrostakeCoordinator(
        exchange,
        JsonSagaStore(tmp_path / "native-sagas.json"),
        MicrostakePolicy(max_transient_exposure=D(cap)),
    )
    quote = quote or {
        "passive_stub_price": D("3.00"),
        "best_counterparty_price": None,
    }
    return NativeMicrostakeExecutor(
        coordinator,
        quote_provider=lambda _payload: quote,
        transient_exposure_cap=D(cap),
    ), exchange


def test_native_executor_runs_recovered_saga_and_returns_engine_contract(tmp_path):
    native, exchange = _native(tmp_path)

    result = native.execute({
        "market_id": "1.100", "selection_id": 7, "side": "BACK",
        "price": 2.0, "size": 0.10, "customer_ref": "native-e2e",
    })

    assert result["status"] == "SUCCESS"
    assert result["phase"] == "ACTIVE"
    assert result["instructionReports"][0]["betId"].startswith("SIM-")
    assert [op[0] for op in exchange.operation_log] == ["PLACE", "CANCEL", "REPLACE"]


def test_native_executor_blocks_owner_cap_conflict_before_place(tmp_path):
    native, exchange = _native(tmp_path, cap="0.99")                     # stub da 1 EUR oltre il cap

    result = native.execute({
        "market_id": "1.100", "selection_id": 7, "side": "BACK",
        "price": 2.0, "size": 0.10, "customer_ref": "native-cap",
    })

    assert result["status"] == "FAILURE"
    assert result["error"] == "transient_exposure_cap"
    assert exchange.operation_log == []


def test_native_executor_blocks_unproved_or_crossing_quote_before_place(tmp_path):
    native, exchange = _native(
        tmp_path,
        cap="5000",
        quote={
            "passive_stub_price": D("1.01"),
            "best_counterparty_price": D("2.00"),
        },
    )

    result = native.execute({
        "market_id": "1.100", "selection_id": 7, "side": "BACK",
        "price": 2.0, "size": 0.10, "customer_ref": "native-cross",
    })

    assert result["error"] == "stub_price_would_cross_book"
    assert exchange.operation_log == []
