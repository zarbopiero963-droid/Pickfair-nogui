"""Gate A — T2b batch b1, componente cashout_cancel_adapter (CashoutCancelAdapter reale)."""
from __future__ import annotations

from typing import Any, List

from cashout_cancel_adapter import CashoutCancelAdapter, response_confirms_cancel

_OK = {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}]}


class _Spy:
    def __init__(self, result: Any = _OK) -> None:
        self.calls: List[tuple] = []
        self.result = result

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.result


def test_trg_628_b_1023():
    """ID: TRG-628-B-1023 / PF-B1-R2-CANCEL-EMPTY"""
    for sim in (True, False):
        live, simc = _Spy(), _Spy()
        adapter = CashoutCancelAdapter(is_simulation=lambda s=sim: s, live_cancel=live, sim_cancel=simc)
        for empty in ([], None, ["", "  "]):
            assert adapter.cancel("1.1", empty) is True
        assert live.calls == [] and simc.calls == []   # mai "cancella TUTTO il mercato"


def test_trg_628_b_1024():
    """ID: TRG-628-B-1024 / PF-B1-R2-CANCEL-MODE"""
    live, simc = _Spy(), _Spy()
    sim_adapter = CashoutCancelAdapter(is_simulation=lambda: True, live_cancel=live, sim_cancel=simc)
    assert sim_adapter.cancel("1.1", ["B1", " B2 "]) is True
    assert simc.calls == [((), {"market_id": "1.1", "instructions": [{"betId": "B1"}, {"betId": "B2"}]})]
    assert live.calls == []
    live2, simc2 = _Spy({"ok": True, "result": _OK}), _Spy()
    live_adapter = CashoutCancelAdapter(is_simulation=lambda: False, live_cancel=live2, sim_cancel=simc2)
    assert live_adapter.cancel("1.1", ["B1"]) is True
    assert live2.calls == [((), {"market_id": "1.1", "bet_ids": ["B1"]})]
    assert simc2.calls == []


def test_trg_628_b_1025():
    """ID: TRG-628-B-1025 / PF-B1-R2-CANCEL-CONFIRM"""
    ok = response_confirms_cancel
    assert ok(_OK) is True
    assert ok({"ok": True, "status": "SUCCESS", "result": _OK}) is True
    # envelope di successo SENZA report: nessuna prova positiva => non confermato
    assert ok({"ok": True, "status": "SUCCESS"}) is False
    assert ok({"status": "SUCCESS", "instructionReports": []}) is False
    # qualsiasi failure prevale, top o annidata
    assert ok({"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}, {"status": "FAILURE"}]}) is False
    assert ok({"ok": True, "result": {"instructionReports": [{"status": "SUCCESS"}, {}]}}) is False
    assert ok({"ok": True, "status": "FAILURE", "result": _OK}) is False
    assert ok({"ok": False, "result": _OK}) is False
    for shape in (True, None, [], "SUCCESS", ["B1"]):
        assert ok(shape) is False
    # via adapter: eccezione del broker => False
    def boom(**kw):
        raise TimeoutError("x")
    adapter = CashoutCancelAdapter(is_simulation=lambda: False, live_cancel=boom, sim_cancel=_Spy())
    assert adapter.cancel("1.1", ["B1"]) is False
