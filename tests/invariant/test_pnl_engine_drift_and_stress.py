import math
import pytest

from core.pnl_engine import PnLEngine

_REQUIRED_EVENT_DRIVEN_API = ("_calc", "_on_filled", "_on_market", "snapshot")

# Fail-closed: if the required event-driven API is absent, fail at collection
# time instead of silently skipping.  A supported implementation must pass its
# invariant suite; missing API → broken contract → CI failure.
_missing_api = [name for name in _REQUIRED_EVENT_DRIVEN_API if not hasattr(PnLEngine, name)]
if _missing_api:
    pytest.fail(
        f"core.pnl_engine.PnLEngine is missing required methods: {_missing_api}. "
        "This invariant suite is fail-closed and cannot silently skip for a "
        "supported PnL implementation."
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
    from core.pnl_engine import PnLEngine

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


@pytest.mark.invariant
def test_root_pnl_engine_commission_applies_only_to_positive_pnl():
    from core.pnl_engine import PnLEngine

    engine = PnLEngine(bus=None, commission_pct=4.5)
    base_pos = {
        "event_key": "E-COMM",
        "market_id": "1.250",
        "selection_id": 99,
        "side": "BACK",
        "price": 2.0,
        "stake": 100.0,
        "table_id": 1,
        "batch_id": "BC",
    }

    positive_book = {
        "marketId": "1.250",
        "runners": [{"selectionId": 99, "ex": {"availableToBack": [{"price": 2.0}], "availableToLay": [{"price": 1.5}]}}],
    }
    zero_book = {
        "marketId": "1.250",
        "runners": [{"selectionId": 99, "ex": {"availableToBack": [{"price": 2.0}], "availableToLay": [{"price": 2.0}]}}],
    }
    negative_book = {
        "marketId": "1.250",
        "runners": [{"selectionId": 99, "ex": {"availableToBack": [{"price": 2.0}], "availableToLay": [{"price": 2.5}]}}],
    }

    pnl_pos = engine._calc(dict(base_pos), positive_book)
    pnl_zero = engine._calc(dict(base_pos), zero_book)
    pnl_neg = engine._calc(dict(base_pos), negative_book)

    assert math.isfinite(pnl_pos)
    assert math.isfinite(pnl_zero)
    assert math.isfinite(pnl_neg)

    # Gross +50 => net 50 - 4.5%
    assert abs(pnl_pos - 47.75) < 1e-12
    # Zero remains zero
    assert pnl_zero == 0.0
    # Gross -50 should not be further reduced by commission
    assert abs(pnl_neg - (-50.0)) < 1e-12


@pytest.mark.invariant
def test_root_pnl_engine_commission_contract_is_finite_and_single_applied():
    from core.pnl_engine import PnLEngine

    engine = PnLEngine(bus=None, commission_pct=4.5)
    pos = {
        "event_key": "E-COMM-2",
        "market_id": "1.251",
        "selection_id": 101,
        "side": "BACK",
        "price": 2.0,
        "stake": 100.0,
        "table_id": 1,
        "batch_id": "BC2",
    }

    winning_book = {
        "marketId": "1.251",
        "runners": [{"selectionId": 101, "ex": {"availableToBack": [{"price": 2.0}], "availableToLay": [{"price": 1.5}]}}],
    }

    pnl_net = engine._calc(dict(pos), winning_book)
    assert math.isfinite(pnl_net)
    # Single-application 4.5% reference: +50 gross -> +47.75 net
    assert abs(pnl_net - 47.75) < 1e-12


@pytest.mark.invariant
def test_root_event_driven_calc_surface_is_mark_to_market_not_realized_settlement():
    from core.pnl_engine import PnLEngine

    engine = PnLEngine(bus=None, commission_pct=4.5)
    pos = {
        "event_key": "E-KIND",
        "market_id": "1.252",
        "selection_id": 102,
        "side": "BACK",
        "price": 2.0,
        "stake": 100.0,
        "table_id": 1,
        "batch_id": "BK",
    }
    winning_book = {
        "marketId": "1.252",
        "runners": [{"selectionId": 102, "ex": {"availableToBack": [{"price": 2.0}], "availableToLay": [{"price": 1.5}]}}],
    }

    settlement = engine._calc_settlement(dict(pos), winning_book)
    assert settlement["settlement_kind"] == "mark_to_market_estimate"
    assert settlement["settlement_source"] == "core_pnl_engine"


@pytest.mark.chaos
def test_root_pnl_engine_numeric_stress_extreme_prices_and_stakes():
    from core.pnl_engine import PnLEngine

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
    from core.pnl_engine import PnLEngine

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
    from core.pnl_engine import PnLEngine

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
    assert math.isfinite(payload["gross_pnl"])
    assert math.isfinite(payload["commission_amount"])
    assert math.isfinite(payload["net_pnl"])
    assert payload["pnl"] == payload["net_pnl"]
    assert payload["commission_pct"] == 4.5
    assert payload["settlement_basis"] == "market_net_realized"
    assert payload["settlement_source"] == "core_pnl_engine"
    assert payload["settlement_kind"] == "realized_settlement"


@pytest.mark.invariant
def test_root_pnl_engine_snapshot_is_stable_under_many_positions():
    from core.pnl_engine import PnLEngine

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


@pytest.mark.invariant
def test_runtime_authoritative_settlement_acceptance_remains_centralized_not_helper_derived():
    from core.runtime_controller import RuntimeController
    from core.pnl_engine import PnLEngine as EventPnLEngine
    from pnl_engine import PnLEngine as HelperPnLEngine

    helper = HelperPnLEngine(commission_pct=4.5)
    helper_payload = helper.calculate_settlement_pnl(
        side="BACK",
        price=2.0,
        size=100.0,
        won=True,
    )
    extracted_helper = RuntimeController._extract_settlement_contract(
        {
            "event_key": "evt-helper-centralized",
            "batch_id": "batch-helper-centralized",
            "table_id": 1,
            "correlation_id": "corr-helper-centralized",
            **helper_payload,
        }
    )
    assert extracted_helper["settlement_validation"] == "rejected_ambiguous"
    assert extracted_helper["settlement_acceptance"] == "REJECT_AMBIGUOUS_SETTLEMENT"

    class _Bus:
        def __init__(self):
            self.events = []

        def subscribe(self, *_args, **_kwargs):
            return None

        def publish(self, topic, payload):
            self.events.append((topic, dict(payload or {})))

    bus = _Bus()
    engine = EventPnLEngine(bus=bus, commission_pct=4.5)
    engine._on_filled(
        {
            "event_key": "evt-authoritative-centralized",
            "market_id": "1.333",
            "selection_id": 123,
            "bet_type": "BACK",
            "price": 2.0,
            "stake": 100.0,
            "table_id": 1,
            "batch_id": "batch-authoritative-centralized",
        }
    )
    engine._on_market(
        {
            "marketId": "1.333",
            "runners": [
                {
                    "selectionId": 123,
                    "ex": {
                        "availableToBack": [{"price": 2.0}],
                        "availableToLay": [{"price": 1.5}],
                    },
                }
            ],
        }
    )
    close_payload = [payload for topic, payload in bus.events if topic == "RUNTIME_CLOSE_POSITION"][-1]
    extracted_authoritative = RuntimeController._extract_settlement_contract(dict(close_payload))
    assert extracted_authoritative["settlement_validation"] == "accepted"
    assert extracted_authoritative["settlement_acceptance"] == "ACCEPT_REALIZED_SETTLEMENT"


@pytest.mark.invariant
def test_core_event_pnl_weighted_average_multi_fill_same_event_key_is_authoritative():
    from core.pnl_engine import PnLEngine

    engine = PnLEngine(bus=None, commission_pct=4.5)
    engine._on_filled(
        {
            "event_key": "E-WAVG",
            "market_id": "1.610",
            "selection_id": 10,
            "bet_type": "BACK",
            "price": 2.0,
            "stake": 100.0,
            "fill_id": "f1",
            "table_id": 1,
            "batch_id": "BWAVG",
        }
    )
    engine._on_filled(
        {
            "event_key": "E-WAVG",
            "market_id": "1.610",
            "selection_id": 10,
            "bet_type": "BACK",
            "price": 3.0,
            "stake": 50.0,
            "fill_id": "f2",
            "table_id": 1,
            "batch_id": "BWAVG",
        }
    )

    pos = engine.snapshot()["positions"][0]["ledger"]
    assert pos["open_side"] == "BACK"
    assert pos["open_size"] == pytest.approx(150.0)
    assert pos["avg_entry_price"] == pytest.approx((2.0 * 100.0 + 3.0 * 50.0) / 150.0)


@pytest.mark.invariant
def test_core_event_pnl_partial_close_same_event_key_updates_residual_and_realized():
    from core.pnl_engine import PnLEngine

    engine = PnLEngine(bus=None, commission_pct=4.5)
    engine._on_filled(
        {
            "event_key": "E-PCLOSE",
            "market_id": "1.611",
            "selection_id": 10,
            "bet_type": "BACK",
            "price": 2.0,
            "stake": 100.0,
            "fill_id": "f1",
            "table_id": 1,
            "batch_id": "BPCLOSE",
        }
    )
    engine._on_filled(
        {
            "event_key": "E-PCLOSE",
            "market_id": "1.611",
            "selection_id": 10,
            "bet_type": "LAY",
            "price": 1.5,
            "stake": 40.0,
            "fill_id": "f2",
            "table_id": 1,
            "batch_id": "BPCLOSE",
        }
    )

    pos = engine.snapshot()["positions"][0]["ledger"]
    assert pos["open_side"] == "BACK"
    assert pos["open_size"] == pytest.approx(60.0)
    assert pos["realized_pnl"] == pytest.approx((2.0 - 1.5) * 40.0)
    assert pos["unrealized_pnl"] == pytest.approx(0.0)


@pytest.mark.invariant
def test_core_event_pnl_duplicate_fill_id_no_duplicate_realization():
    from core.pnl_engine import PnLEngine

    engine = PnLEngine(bus=None, commission_pct=4.5)
    payload = {
        "event_key": "E-DUP",
        "market_id": "1.612",
        "selection_id": 10,
        "bet_type": "BACK",
        "price": 2.0,
        "stake": 100.0,
        "fill_id": "dup-fill",
        "table_id": 1,
        "batch_id": "BDUP",
    }
    engine._on_filled(dict(payload))
    engine._on_filled(dict(payload))

    pos = engine.snapshot()["positions"][0]["ledger"]
    assert pos["open_size"] == pytest.approx(100.0)
    assert pos["realized_pnl"] == pytest.approx(0.0)
