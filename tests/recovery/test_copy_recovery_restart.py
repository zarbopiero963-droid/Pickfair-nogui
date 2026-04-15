from __future__ import annotations

from copy import deepcopy

from core.state_recovery import StateRecovery


class FakeDB:
    def __init__(self, sagas):
        self._sagas = [deepcopy(sagas), []]
        self.calls = 0

    def get_pending_sagas(self):
        idx = min(self.calls, len(self._sagas) - 1)
        self.calls += 1
        return deepcopy(self._sagas[idx])


class FakeBus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload):
        self.events.append((name, deepcopy(payload)))


def test_copy_recovery_rebuilds_state_without_duplicate_actions_after_restart():
    initial_sagas = [
        {
            "status": "PENDING",
            "payload": {
                "copy_group_id": "CG-1",
                "order_origin": "COPY",
                "copy_meta": {"master_id": "M-1", "copy_group_id": "CG-1"},
                "position_id": "POS-1",
                "action_id": "A-1",
                "action_seq": 10,
                "state": "OPEN",
            },
        },
        {
            "status": "PENDING",
            "payload": {
                "copy_group_id": "CG-1",
                "order_origin": "COPY",
                "copy_meta": {"master_id": "M-1", "copy_group_id": "CG-1"},
                "position_id": "POS-2",
                "action_id": "A-2",
                "action_seq": 11,
                "state": "OPEN",
            },
        },
    ]
    db = FakeDB(initial_sagas)
    bus = FakeBus()

    pre_restart = StateRecovery(db=db, bus=bus).recover_pending_orders()
    post_restart = StateRecovery(db=db, bus=bus).recover_pending_orders()

    assert len(pre_restart) == 2
    assert len(post_restart) == 0

    recovered_payloads = [payload for name, payload in bus.events if name == "RECOVER_ORDER"]
    assert len(recovered_payloads) == 2

    action_ids = [p["action_id"] for p in recovered_payloads]
    action_seq = [p["action_seq"] for p in recovered_payloads]
    order_origins = [p.get("order_origin") for p in recovered_payloads]
    copy_meta_group_ids = [((p.get("copy_meta") or {}).get("copy_group_id")) for p in recovered_payloads]
    assert action_ids == ["A-1", "A-2"]
    assert action_seq == [10, 11]
    assert order_origins == ["COPY", "COPY"]
    assert copy_meta_group_ids == ["CG-1", "CG-1"]
    assert len(set(action_ids)) == len(action_ids)
    assert action_seq == sorted(action_seq)
