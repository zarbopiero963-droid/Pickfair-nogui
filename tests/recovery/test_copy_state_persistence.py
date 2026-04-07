from __future__ import annotations

from copy import deepcopy


class FakeCopyStateRepo:
    def __init__(self):
        self._saved = None

    def save(self, state):
        self._saved = deepcopy(state)

    def load(self):
        return deepcopy(self._saved)


def test_copy_state_persists_and_reloads_identically():
    state = {
        "copy_group_id": "CG-55",
        "action_seq": 987,
        "positions": [
            {"position_id": "P-1", "status": "OPEN", "action_id": "A-1"},
            {"position_id": "P-2", "status": "PARTIAL", "action_id": "A-2"},
        ],
        "meta": {"source": "copy", "version": 1},
    }

    repo = FakeCopyStateRepo()
    repo.save(state)
    restored = repo.load()

    assert restored == state
    assert restored["copy_group_id"] == "CG-55"
    assert restored["action_seq"] == 987
    assert [p["position_id"] for p in restored["positions"]] == ["P-1", "P-2"]
