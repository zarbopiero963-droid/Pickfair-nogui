import math
import pytest

from pnl_engine import PnLEngine

_REQUIRED_LEGACY_API = ("_calc", "_on_filled", "_on_market", "snapshot")
pytestmark = pytest.mark.skipif(
    any(not hasattr(PnLEngine, name) for name in _REQUIRED_LEGACY_API),
    reason="legacy event-driven pnl_engine API not available",
)

class FakeBus:
    def __init__(self):
        self.subscribers = {}
        self.events = []

    def subscribe(self, event_name, handler):
        self.subscribers.setdefault(event_name, []).append(handler)

    def publish(self, event_name, payload):
        self.events.append((event_name, payload))


@pytest.mark.invariant
def test_root_pnl_engine_repeated_calc_is_stable():
    from pnl_engine import PnLEngine

    engine = PnLEngine(bus=None, commission_pct=4.5)

    pos = {
        "event_key": "E1",
        "market_id": "1.100",
        "selection_id": 10,
        "side": "BACK",
        "price": 2.5,
        "stake": 100.0,
        "table_id": 1,
        "batch_id": "B1",
    }
    market_book = {
        "marketId": "1.100",
        "runners": [
            {
                "selectionId": 10,
                "ex": {
                    "availableToBack": [{"price": 2.48}],
                    "availableToLay": [{"price": 2.52}],
                },
            }
        ],
    }

    values = [engine._calc(pos, market_book) for _ in range(5000)]

    assert all(math.isfinite(v) for v in values)
    first = values[0]
    assert all(abs(v - first) < 1e-12 for v in values)


@pytest.mark.chaos
def test_root_pnl_engine_numeric_stress_extreme_prices_and_stakes():
    from pnl_engine import PnLEngine

    engine = PnLEngine(bus=None, commission_pct=4.5)

    cases = [
        ("BACK", 1.01, 1e-6, 1.02, 1.03),
        ("BACK", 1000.0, 1e6, 999.0, 1000.0),
        ("LAY", 1.01, 1e6, 1.02, 1.03),
        ("LAY", 1000.0, 1e-3, 999.0, 1000.0),
    ]

    for side, entry, stake, best_back, best_lay in cases:
        pos = {
            "event_key": "E1",
            "market_id": "1.200",
            "selection_id": 11,
            "side": side,
            "price": entry,
            "stake": stake,
            "table_id": 1,
            "batch_id": "B1",
        }
        market_book = {
            "marketId": "1.200",
            "runners": [
                {
                    "selectionId": 11,
                    "ex": {
                        "availableToBack": [{"price": best_back}],
                        "availableToLay": [{"price": best_lay}],
                    },
                }
            ],
        }

        pnl = engine._calc(pos, market_book)
        assert math.isfinite(pnl)


@pytest.mark.chaos
def test_root_pnl_engine_missing_quotes_is_safe_zero():
    from pnl_engine import PnLEngine

    engine = PnLEngine(bus=None)

    pos = {
        "event_key": "E2",
        "market_id": "1.201",
        "selection_id": 12,
        "side": "BACK",
        "price": 2.0,
        "stake": 50.0,
        "table_id": 1,
        "batch_id": "B2",
    }
    market_book = {
        "marketId": "1.201",
        "runners": [
            {"selectionId": 12, "ex": {}}
        ],
    }

    pnl = engine._calc(pos, market_book)
    assert pnl == 0.0


@pytest.mark.invariant
def test_root_pnl_engine_close_trigger_is_deterministic():
    from pnl_engine import PnLEngine

    bus = FakeBus()
    engine = PnLEngine(bus=bus, commission_pct=4.5)

    fill = {
        "event_key": "E3",
        "market_id": "1.202",
        "selection_id": 13,
        "bet_type": "BACK",
        "price": 3.0,
        "stake": 100.0,
        "table_id": 2,
        "batch_id": "B3",
    }
    engine._on_filled(fill)

    market_book = {
        "marketId": "1.202",
        "runners": [
            {
                "selectionId": 13,
                "ex": {
                    "availableToBack": [{"price": 2.0}],
                    "availableToLay": [{"price": 2.0}],
                },
            }
        ],
    }

    engine._on_market(market_book)

    close_events = [e for e in bus.events if e[0] == "RUNTIME_CLOSE_POSITION"]
    assert len(close_events) == 1
    payload = close_events[0][1]
    assert payload["event_key"] == "E3"
    assert math.isfinite(payload["pnl"])


@pytest.mark.invariant
def test_root_pnl_engine_snapshot_is_stable_under_many_positions():
    from pnl_engine import PnLEngine

    engine = PnLEngine(bus=None)

    for i in range(1000):
        engine._on_filled(
            {
                "event_key": f"E{i}",
                "market_id": f"1.{i}",
                "selection_id": i,
                "bet_type": "BACK" if i % 2 == 0 else "LAY",
                "price": 2.0 + (i % 10) * 0.1,
                "stake": 10.0 + i,
                "table_id": i,
                "batch_id": f"B{i}",
            }
        )

    snap = engine.snapshot()
    assert snap["open_positions"] == 1000
    assert len(snap["positions"]) == 1000