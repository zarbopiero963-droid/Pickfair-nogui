"""Gate A batch C: core/runtime_controller.py RuntimeController.reset_risk_stop."""
from __future__ import annotations

import threading
from types import SimpleNamespace

from core import runtime_controller as rc_mod
from core.runtime_controller import RuntimeController


class _Bus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload=None):
        self.events.append((name, payload))


def _fake_rc(reason="exposure_stop_reached:x"):
    return SimpleNamespace(
        _risk_stop_reason=reason,
        _risk_stop_lock=threading.RLock(),
        db=object(),
        bus=_Bus(),
    )


def test_trg_628_b_0989(monkeypatch):
    """ID: TRG-628-B-0989 / PF-B1-R2-RUNTIME-RESET

    reset_risk_stop persists the reset BEFORE clearing the in-memory reason and
    publishes RISK_STOP_RESET only after a successful persist; a failed persist
    keeps the reason and publishes nothing.
    """
    seen = []
    monkeypatch.setattr(rc_mod.stop_rules, "reset_blocker", lambda rc, at_start: "")

    # Failure: persist returns False -> reason retained, no RISK_STOP_RESET.
    fake = _fake_rc()

    def _persist_fail(db, reason):
        seen.append(("fail", reason, fake._risk_stop_reason, list(fake.bus.events)))
        return False

    monkeypatch.setattr(rc_mod.stop_rules, "persist_risk_stop", _persist_fail)
    out = RuntimeController.reset_risk_stop(fake)
    assert out == {"risk_stop_reset": False, "reason": "risk_stop_reset_non_persistito"}
    assert fake._risk_stop_reason == "exposure_stop_reached:x"
    assert [e for e in fake.bus.events if e[0] == "RISK_STOP_RESET"] == []
    assert seen[-1][1] == ""                              # persisted the reset (empty reason)

    # Success: at persist time the in-memory reason is still set and nothing
    # is published yet; afterwards cleared + exactly one RISK_STOP_RESET.
    fake = _fake_rc()

    def _persist_ok(db, reason):
        seen.append(("ok", reason, fake._risk_stop_reason, list(fake.bus.events)))
        return True

    monkeypatch.setattr(rc_mod.stop_rules, "persist_risk_stop", _persist_ok)
    out = RuntimeController.reset_risk_stop(fake)
    assert out == {"risk_stop_reset": True, "reason": ""}
    _, persisted_reason, reason_at_persist, events_at_persist = seen[-1]
    assert persisted_reason == ""
    assert reason_at_persist == "exposure_stop_reached:x"
    assert events_at_persist == []
    assert fake._risk_stop_reason == ""
    assert [e[0] for e in fake.bus.events] == ["RISK_STOP_RESET"]
