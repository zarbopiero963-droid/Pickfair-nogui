"""Gate A batch C: core/simulation_matching_engine.py pessimistic slippage."""
from __future__ import annotations

import pytest

from core.simulation_matching_engine import SimulationMatchingEngine
from core.simulation_order_book import SimulationOrderBook
from core.simulation_state import SimulationState


def _engine(ticks):
    return SimulationMatchingEngine(order_book=SimulationOrderBook(), state=SimulationState(),
                                    queue_ahead_ratio=0.0, slippage_ticks=ticks)


def test_trg_628_b_1014():
    """ID: TRG-628-B-1014 / PF-B1-R2-MATCH-SLIPPAGE

    Pessimistic slippage worsens BACK by lowering the price and LAY by raising
    it, one Betfair tick per slippage tick; never below the 1.01 floor.
    """
    e2 = _engine(2)
    assert e2._apply_pessimistic_slippage("BACK", 2.0) == pytest.approx(1.97)   # 2.00 -(0.02)-> 1.98 -(0.01, band <2)-> 1.97
    assert e2._apply_pessimistic_slippage("LAY", 2.0) == pytest.approx(2.04)    # 2.00 -> 2.02 -> 2.04
    assert e2._apply_pessimistic_slippage("BACK", 5.0) == pytest.approx(4.8)    # 0.1 ticks
    assert e2._apply_pessimistic_slippage("LAY", 9.8) == pytest.approx(10.5)    # 9.8 -> 10.0 -> 10.5 (tick per band)
    # floor
    assert e2._apply_pessimistic_slippage("BACK", 1.01) == pytest.approx(1.01)
    assert _engine(5)._apply_pessimistic_slippage("BACK", 1.03) == pytest.approx(1.01)
    # zero ticks = book price
    assert _engine(0)._apply_pessimistic_slippage("BACK", 3.0) == pytest.approx(3.0)
    assert _engine(0)._apply_pessimistic_slippage("LAY", 3.0) == pytest.approx(3.0)
