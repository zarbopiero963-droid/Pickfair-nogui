import math
import random
import pytest


@pytest.mark.invariant
def test_fast_analytics_balanced_long_run_no_drift():
    from core.fast_analytics import FastWoMState

    state = FastWoMState(max_ticks=256)

    for _ in range(50000):
        state.push({"back_volume": 100.0, "lay_volume": 100.0})

    wom = state.wom()
    imbalance = state.imbalance()
    snap = state.snapshot()

    assert math.isfinite(wom)
    assert math.isfinite(imbalance)
    assert abs(wom - 0.5) < 1e-12
    assert abs(imbalance - 0.0) < 1e-12
    assert snap["ticks"] == 256
    assert math.isfinite(snap["sum_back"])
    assert math.isfinite(snap["sum_lay"])


@pytest.mark.chaos
def test_fast_analytics_extreme_ranges_no_nan_inf():
    from core.fast_analytics import FastWoMState

    state = FastWoMState(max_ticks=512)

    extremes = [
        {"back_volume": 1e-12, "lay_volume": 1e12},
        {"back_volume": 1e12, "lay_volume": 1e-12},
        {"back_volume": 1e-6, "lay_volume": 1e6},
        {"back_volume": 1e6, "lay_volume": 1e-6},
        {"back_volume": 0.0, "lay_volume": 1000.0},
        {"back_volume": 1000.0, "lay_volume": 0.0},
    ]

    for _ in range(10000):
        for tick in extremes:
            state.push(tick)
            wom = state.wom()
            imbalance = state.imbalance()
            assert math.isfinite(wom)
            assert math.isfinite(imbalance)
            assert 0.0 <= wom <= 1.0
            assert -1.0 <= imbalance <= 1.0

    snap = state.snapshot()
    assert math.isfinite(snap["sum_back"])
    assert math.isfinite(snap["sum_lay"])


@pytest.mark.invariant
def test_fast_analytics_periodic_rebase_keeps_balanced_window_stable():
    from core.fast_analytics import FastWoMState

    state = FastWoMState(max_ticks=128)

    # forza molte eviction + rebase
    for _ in range(20000):
        state.push({"back_volume": 250.0, "lay_volume": 250.0})

    assert abs(state.wom() - 0.5) < 1e-12
    assert abs(state.imbalance()) < 1e-12


@pytest.mark.chaos
def test_fast_analytics_randomized_stress_is_bounded():
    from core.fast_analytics import FastWoMState

    rng = random.Random(1337)
    state = FastWoMState(max_ticks=256)

    for _ in range(100000):
        state.push(
            {
                "back_volume": rng.uniform(0.0, 100000.0),
                "lay_volume": rng.uniform(0.0, 100000.0),
            }
        )

    wom = state.wom()
    imbalance = state.imbalance()

    assert math.isfinite(wom)
    assert math.isfinite(imbalance)
    assert 0.0 <= wom <= 1.0
    assert -1.0 <= imbalance <= 1.0


@pytest.mark.invariant
def test_fast_analytics_repeated_snapshot_reads_are_stable():
    from core.fast_analytics import FastWoMState

    state = FastWoMState(max_ticks=64)

    for _ in range(5000):
        state.push({"back_volume": 10.0, "lay_volume": 20.0})

    a = state.snapshot()
    b = state.snapshot()
    c = state.snapshot()

    assert a == b == c
    assert math.isfinite(a["wom"])