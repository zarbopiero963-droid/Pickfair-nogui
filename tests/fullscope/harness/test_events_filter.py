"""F-33 — gli eventi cercati dalle sonde devono sopravvivere al filtro degli artifact (rilievo Codex/Sol su d46e6da)."""
import pytest

from tests.fullscope.harness import events as E

pytestmark = [pytest.mark.unit, pytest.mark.fullscope]


@pytest.mark.parametrize("name", ["SL_TRIGGERED", "STOP_LOSS_TRIGGERED", "TP_TRIGGERED", "TAKE_PROFIT_HIT",
                                  "TRAILING_STOP_TRIGGERED", "CASHOUT_SUCCESS", "RUNTIME_CLOSE_POSITION",
                                  "SIGNAL_REJECTED", "AUTO_TRADE_MM_RESULT"])
def test_probe_events_are_kept(name):
    assert E.keep(name)


def test_noise_is_dropped():
    assert not E.keep("MARKET_TICK")
