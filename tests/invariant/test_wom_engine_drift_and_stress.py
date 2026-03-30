import math
import pytest


class FakeClock:
    def __init__(self, start: float = 1_700_000_000.0):
        self.now = float(start)

    def time(self) -> float:
        return self.now

    def step(self, seconds: float = 0.01) -> float:
        self.now += float(seconds)
        return self.now


@pytest.mark.invariant
def test_wom_engine_balanced_flow_has_no_meaningful_drift(monkeypatch):
    import ai.wom_engine as wom_module
    from ai.wom_engine import WoMEngine

    clock = FakeClock()
    monkeypatch.setattr(wom_module.time, "time", clock.time)

    engine = WoMEngine(window_size=5000, time_window=120.0)

    for _ in range(4000):
        engine.record_tick(
            selection_id=101,
            back_price=2.0,
            back_volume=100.0,
            lay_price=2.02,
            lay_volume=100.0,
        )
        clock.step(0.01)

    result = engine.calculate_wom(101)
    assert result is not None
    assert math.isfinite(result.wom)
    assert abs(result.wom - 0.5) < 1e-12
    assert math.isfinite(result.edge_score)
    assert math.isfinite(result.confidence)
    assert result.tick_count >= 2


@pytest.mark.invariant
def test_wom_engine_multi_window_balanced_flow_is_stable(monkeypatch):
    import ai.wom_engine as wom_module
    from ai.wom_engine import WoMEngine

    clock = FakeClock()
    monkeypatch.setattr(wom_module.time, "time", clock.time)

    engine = WoMEngine(window_size=6000, time_window=120.0)

    for _ in range(5000):
        engine.record_tick(
            selection_id=7,
            back_price=1.99,
            back_volume=250.0,
            lay_price=2.0,
            lay_volume=250.0,
        )
        clock.step(0.01)

    multi = engine.calculate_multi_window_wom(7)

    assert set(multi.keys()) == {"wom_5s", "wom_15s", "wom_30s", "wom_60s"}
    for value in multi.values():
        assert math.isfinite(value)
        assert abs(value - 0.5) < 1e-12


@pytest.mark.chaos
def test_wom_engine_numeric_stress_extreme_volumes_stays_bounded(monkeypatch):
    import ai.wom_engine as wom_module
    from ai.wom_engine import WoMEngine

    clock = FakeClock()
    monkeypatch.setattr(wom_module.time, "time", clock.time)

    engine = WoMEngine(window_size=8000, time_window=120.0)

    extremes = [
        (1e-9, 1e9),
        (1e9, 1e-9),
        (1e-6, 1e6),
        (1e6, 1e-6),
        (0.01, 1000.0),
        (1000.0, 0.01),
    ]

    for _ in range(3000):
        for back_vol, lay_vol in extremes:
            engine.record_tick(
                selection_id=55,
                back_price=2.0,
                back_volume=back_vol,
                lay_price=2.02,
                lay_volume=lay_vol,
            )
            clock.step(0.002)

    enhanced = engine.calculate_enhanced_wom(55)
    assert enhanced is not None

    for value in [
        enhanced.wom,
        enhanced.wom_5s,
        enhanced.wom_15s,
        enhanced.wom_30s,
        enhanced.wom_60s,
        enhanced.edge_score,
        enhanced.confidence,
        enhanced.delta_pressure,
        enhanced.momentum,
        enhanced.volatility,
    ]:
        assert math.isfinite(value)

    assert 0.0 <= enhanced.wom <= 1.0
    assert 0.0 <= enhanced.wom_5s <= 1.0
    assert 0.0 <= enhanced.wom_15s <= 1.0
    assert 0.0 <= enhanced.wom_30s <= 1.0
    assert 0.0 <= enhanced.wom_60s <= 1.0
    assert -1.0 <= enhanced.edge_score <= 1.0
    assert 0.0 <= enhanced.confidence <= 1.0
    assert -1.0 <= enhanced.delta_pressure <= 1.0
    assert -1.0 <= enhanced.momentum <= 1.0
    assert 0.0 <= enhanced.volatility <= 1.0


@pytest.mark.chaos
def test_wom_engine_alternating_pressure_never_leaks_outside_bounds(monkeypatch):
    import ai.wom_engine as wom_module
    from ai.wom_engine import WoMEngine

    clock = FakeClock()
    monkeypatch.setattr(wom_module.time, "time", clock.time)

    engine = WoMEngine(window_size=2000, time_window=90.0)

    for i in range(12000):
        if i % 2 == 0:
            engine.record_tick(900, 2.0, 500.0, 2.02, 50.0)
        else:
            engine.record_tick(900, 2.0, 50.0, 2.02, 500.0)
        clock.step(0.005)

    signal = engine.get_time_window_signal(900)

    assert signal["signal"] in {
        "NO_DATA", "STRONG_BACK", "STRONG_LAY", "BACK", "LAY", "NEUTRAL"
    }
    assert signal["side"] in {"BACK", "LAY", "NEUTRAL"}
    assert math.isfinite(signal["strength"])
    assert 0.0 <= signal["strength"] <= 1.0


@pytest.mark.invariant
def test_wom_engine_dirty_inputs_do_not_create_non_finite_output(monkeypatch):
    import ai.wom_engine as wom_module
    from ai.wom_engine import WoMEngine

    clock = FakeClock()
    monkeypatch.setattr(wom_module.time, "time", clock.time)

    engine = WoMEngine(window_size=256, time_window=60.0)

    dirty_ticks = [
        {"selection_id": 1, "back_price": None, "back_volume": None, "lay_price": "", "lay_volume": ""},
        {"selection_id": 1, "back_price": "2.0", "back_volume": "100", "lay_price": "2.02", "lay_volume": "50"},
        {"selection_id": 1, "back_price": -1, "back_volume": -100, "lay_price": -1, "lay_volume": -50},
        {"selection_id": 1, "back_price": 2.0, "back_volume": 0.0, "lay_price": 2.02, "lay_volume": 0.0},
    ]

    for tick in dirty_ticks * 200:
        engine.record_tick(
            selection_id=tick["selection_id"],
            back_price=tick["back_price"],
            back_volume=tick["back_volume"],
            lay_price=tick["lay_price"],
            lay_volume=tick["lay_volume"],
        )
        clock.step(0.01)

    multi = engine.calculate_multi_window_wom(1)
    for value in multi.values():
        assert math.isfinite(value)
        assert 0.0 <= value <= 1.0