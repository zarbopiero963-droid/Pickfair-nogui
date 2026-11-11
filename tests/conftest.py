import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class _TestRuntimeBox:
    """BB-1G: runtime di test con una scatola nera sana, per i test che esercitano ordini, avvii e riprese senza
    riguardare la scatola nera. Accetta solo i tipi del vocabolario reale (un tipo inventato resta un errore) e non
    scrive nulla. Il comportamento vero del gate (assente, non runtime, guasta, uscite protettive) e' verificato con la
    `BlackBox` reale nei test marcati `real_blackbox` e in `tests/blackbox`, `tests/packaging`."""

    def __init__(self):
        self.events = []

    def is_runtime(self):
        return True

    def record(self, event_type, **fields):
        from core import event_journal
        if event_type not in event_journal.EVENT_TYPES:
            raise ValueError(f"tipo evento sconosciuto: {event_type!r}")
        self.events.append((event_type, fields))
        return uuid.uuid4().hex

    def end_session(self, reason="STOP", **data):
        return self.record("SHUTDOWN", reason_code=reason)

    def close(self):
        pass


# Moduli che gestiscono da se' la scatola nera del processo: la fixture non deve fingere un runtime installato.
_OWN_BLACKBOX_DIRS = ("blackbox", "packaging")


@pytest.fixture(autouse=True)
def _bb1g_test_runtime(request, monkeypatch):
    path = Path(str(request.node.fspath))
    if request.node.get_closest_marker("real_blackbox") or any(part in _OWN_BLACKBOX_DIRS for part in path.parts[-3:-1]):
        yield None
        return
    from core import blackbox_runtime
    if blackbox_runtime.get() is not None:      # un test che ha installato una scatola nera vera la usa
        yield None
        return
    box = _TestRuntimeBox()
    monkeypatch.setattr(blackbox_runtime, "_box", box)
    yield box
