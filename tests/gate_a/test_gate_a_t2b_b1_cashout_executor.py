"""Gate A — T2b batch b1, componente cashout_executor (CashoutExecutor reale).

Bus e OrderRouter sono doppi deterministici che registrano publish/place: nessun broker, nessuna rete.
"""
from __future__ import annotations

from typing import Any, Dict, List

import pytest

from cashout_executor import CashoutExecutor
from core.trading_constants import CASHOUT_FAILED, CASHOUT_SUCCESS


class _Bus:
    def __init__(self) -> None:
        self.events: List[tuple] = []

    def subscribe(self, topic, cb):  # pragma: no cover - non usato
        pass

    def publish(self, topic, payload=None):
        self.events.append((topic, payload))


class _Router:
    def __init__(self, result: Any) -> None:
        self.result = result
        self.calls: List[Dict[str, Any]] = []

    def place(self, payload):
        self.calls.append(dict(payload))
        return self.result


def _valid() -> Dict[str, Any]:
    return {"market_id": "1.1", "selection_id": 10, "side": "LAY", "price": 2.5, "stake": 12.0,
            "green_up": 2.0, "original_pos": {"side": "BACK", "stake": 10.0, "price": 3.0}}


@pytest.mark.parametrize("patch", [
    {"green_up": None}, {"market_id": ""}, {"selection_id": None}, {"side": None}, {"price": None},
    {"stake": None}, {"side": "BETWEEN"}, {"price": float("nan")}, {"stake": float("inf")},
    {"green_up": float("-inf")}, {"price": 1.0}, {"price": 0.5}, {"stake": 0.0}, {"stake": -1.0},
])
def test_trg_628_b_0780(patch):
    """ID: TRG-628-B-0780 / PF-B1-CASH-EXEC-VALIDATE"""
    bus, router = _Bus(), _Router({"placed": True, "matched": 12.0, "bet_id": "H1"})
    payload = _valid()
    payload.update(patch)
    CashoutExecutor(bus, router).on_cmd_execute_cashout(payload)
    assert router.calls == []
    assert [t for t, _ in bus.events] == [CASHOUT_FAILED]
    out = bus.events[0][1]
    assert out["status"] == "REJECTED" and out["reason"].startswith("validation:")


def test_trg_628_b_0780_valid_control():
    """ID: TRG-628-B-0780 / PF-B1-CASH-EXEC-VALIDATE (controllo positivo: payload valido piazza)"""
    bus, router = _Bus(), _Router({"placed": True, "matched": 12.0, "bet_id": "H1"})
    CashoutExecutor(bus, router).on_cmd_execute_cashout(_valid())
    assert len(router.calls) == 1 and router.calls[0]["bet_type"] == "LAY"
    assert [t for t, _ in bus.events] == [CASHOUT_SUCCESS]


def test_trg_628_b_0781():
    """ID: TRG-628-B-0781 / PF-B1-CASH-EXEC-AMBIGUOUS"""
    bus, router = _Bus(), _Router({"order_unknown": True, "placed": False, "bet_id": "H1", "matched": 0.0})
    CashoutExecutor(bus, router).on_cmd_execute_cashout(_valid())
    assert len(router.calls) == 1          # mai retry
    assert [t for t, _ in bus.events] == [CASHOUT_FAILED]
    out = bus.events[0][1]
    assert out["status"] == "AMBIGUOUS" and out["reason"] == "order_unknown" and out["bet_id"] == "H1"


def test_trg_628_b_0782():
    """ID: TRG-628-B-0782 / PF-B1-CASH-EXEC-UNMATCHED"""
    for matched in (0.0, -1.0, None):
        bus, router = _Bus(), _Router({"placed": True, "matched": matched, "bet_id": "H7"})
        CashoutExecutor(bus, router).on_cmd_execute_cashout(_valid())
        assert len(router.calls) == 1
        assert [t for t, _ in bus.events] == [CASHOUT_FAILED], matched
        out = bus.events[0][1]
        assert out["status"] == "UNMATCHED" and out["bet_id"] == "H7"
