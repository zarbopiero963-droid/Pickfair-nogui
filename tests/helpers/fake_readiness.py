from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FakeLiveReadinessReport:
    ready: bool
    level: str
    blockers: list[str]
    details: dict[str, Any]


class FakeReadinessProvider:
    def __init__(self, report: Any = None, error: Exception | None = None) -> None:
        self._report = report
        self._error = error

    def __call__(self):
        if self._error is not None:
            raise self._error
        return self._report
