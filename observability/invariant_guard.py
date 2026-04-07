"""Pure invariant guard helpers.

The guard is intentionally callable-only and disabled by default.
It does not integrate with runtime services and has no side effects.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

InvariantCheck = Callable[[Mapping[str, Any]], bool]


@dataclass(frozen=True)
class InvariantViolation:
    code: str
    message: str


_DEFAULT_CHECKS: tuple[tuple[str, str, InvariantCheck], ...] = (
    (
        "runtime_status_known",
        "runtime.status must be one of READY, DEGRADED, NOT_READY",
        lambda state: state.get("runtime", {}).get("status") in {"READY", "DEGRADED", "NOT_READY"},
    ),
    (
        "metrics_non_negative",
        "metrics.inflight_count must be >= 0 when present",
        lambda state: state.get("metrics", {}).get("inflight_count", 0) >= 0,
    ),
)


def evaluate_invariants(
    state: Mapping[str, Any],
    *,
    enabled: bool = False,
    checks: Iterable[tuple[str, str, InvariantCheck]] | None = None,
) -> list[InvariantViolation]:
    """Evaluate invariants against a state snapshot.

    The guard is disabled by default. When disabled, it always returns an
    empty list and performs no checks.
    """
    if not enabled:
        return []

    selected_checks = tuple(checks) if checks is not None else _DEFAULT_CHECKS
    violations: list[InvariantViolation] = []

    for code, message, check in selected_checks:
        if not check(state):
            violations.append(InvariantViolation(code=code, message=message))

    return violations


def has_invariant_violations(state: Mapping[str, Any], *, enabled: bool = False) -> bool:
    """Convenience wrapper returning whether any violation exists."""
    return bool(evaluate_invariants(state, enabled=enabled))
