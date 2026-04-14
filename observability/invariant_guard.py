"""Pure invariant guard helpers.

These helpers are side-effect free and can be called directly in tests.
Runtime services (for example WatchdogService) may still invoke them.
Evaluation remains opt-in via ``enabled=True``.
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
    (
        "failed_local_remote_exists",
        "An order marked FAILED locally has a remote (exchange) bet ID — ghost order risk",
        lambda state: not any(
            str(o.get("status", "")).upper() in {"FAILED", "ERROR", "REJECTED"}
            and (o.get("remote_bet_id") or o.get("exchange_order_id"))
            for o in (state.get("recent_orders") or [])
        ),
    ),
    (
        "terminal_to_nonterminal_regression",
        "An order transitioned from a terminal status back to a non-terminal status",
        lambda state: not any(
            str(o.get("prev_status", "")).upper() in {"COMPLETED", "FAILED", "CANCELLED"}
            and str(o.get("status", "")).upper() not in {"COMPLETED", "FAILED", "CANCELLED"}
            for o in (state.get("recent_orders") or [])
        ),
    ),
    (
        "inflight_too_old",
        "Inflight orders exist that are older than the maximum allowed age",
        lambda state: not any(
            str(o.get("status", "")).upper() == "INFLIGHT"
            and float(o.get("age_sec", 0) or 0) > float(state.get("max_inflight_age_sec", 300) or 300)
            for o in (state.get("recent_orders") or [])
        ),
    ),
    (
        "ambiguous_local_remote_inconsistency",
        "Locally ambiguous order has a definitive remote state (win/loss) — inconsistency",
        lambda state: not any(
            str(o.get("status", "")).upper() == "AMBIGUOUS"
            and o.get("remote_final_status") in {"SETTLED_WIN", "SETTLED_LOSS", "CANCELLED"}
            for o in (state.get("recent_orders") or [])
        ),
    ),
    (
        "local_exposure_remote_exposure_mismatch",
        "Local computed exposure differs significantly from remote reported exposure",
        lambda state: abs(
            float((state.get("risk") or {}).get("local_exposure", 0) or 0)
            - float((state.get("risk") or {}).get("remote_exposure", 0) or 0)
        ) <= float((state.get("risk") or {}).get("exposure_tolerance", 0.01) or 0.01),
    ),
    (
        "duplicate_blocked_but_remote_executed",
        "An order was blocked as duplicate locally but the exchange executed a matching order",
        lambda state: not any(
            str(o.get("status", "")).upper() == "DUPLICATE_BLOCKED"
            and o.get("remote_bet_id")
            for o in (state.get("recent_orders") or [])
        ),
    ),
    (
        "finalized_state_inconsistent_with_audit_or_exchange_evidence",
        "A finalized order's local record conflicts with audit log or exchange evidence",
        lambda state: not any(
            str(o.get("status", "")).upper() in {"COMPLETED", "FAILED"}
            and str(o.get("audit_status", "") or "").upper()
            and str(o.get("audit_status", "") or "").upper() != str(o.get("status", "") or "").upper()
            for o in (state.get("recent_orders") or [])
        ),
    ),
    # ── Required runtime-reviewer invariant codes ──────────────────────────
    # These use the canonical uppercase code names required by the runtime
    # reviewer stack audit. They evaluate the same conditions as the aliased
    # checks above but are exposed under the exact codes the reviewer depends on.
    (
        "FAILED_LOCAL_REMOTE_EXISTS",
        "FAILED_LOCAL_REMOTE_EXISTS: An order marked FAILED locally has a remote bet ID — ghost order risk",
        lambda state: not any(
            str(o.get("status", "")).upper() in {"FAILED", "ERROR", "REJECTED"}
            and (o.get("remote_bet_id") or o.get("exchange_order_id"))
            for o in (state.get("recent_orders") or [])
        ),
    ),
    (
        "STATE_REGRESSION",
        "STATE_REGRESSION: An order transitioned from a terminal state back to a non-terminal state",
        lambda state: not any(
            str(o.get("prev_status", "")).upper() in {"COMPLETED", "FAILED", "CANCELLED"}
            and str(o.get("status", "")).upper() not in {"COMPLETED", "FAILED", "CANCELLED"}
            for o in (state.get("recent_orders") or [])
        ),
    ),
    (
        "INFLIGHT_STUCK",
        "INFLIGHT_STUCK: Inflight orders are stuck and exceed the maximum allowed age",
        lambda state: not any(
            str(o.get("status", "")).upper() == "INFLIGHT"
            and float(o.get("age_sec", 0) or 0) > float(state.get("max_inflight_age_sec", 300) or 300)
            for o in (state.get("recent_orders") or [])
        ),
    ),
    (
        "EXPOSURE_MISMATCH",
        "EXPOSURE_MISMATCH: Local computed exposure differs significantly from remote reported exposure",
        lambda state: abs(
            float((state.get("risk") or {}).get("local_exposure", 0) or 0)
            - float((state.get("risk") or {}).get("remote_exposure", 0) or 0)
        ) <= float((state.get("risk") or {}).get("exposure_tolerance", 0.01) or 0.01),
    ),
)

DEFAULT_INVARIANT_CHECKS = _DEFAULT_CHECKS


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

    misconfig_code = "INVARIANT_CHECKS_MISCONFIGURED"
    if checks is None:
        selected_checks = _DEFAULT_CHECKS
    else:
        try:
            selected_checks = tuple(checks)
        except Exception as exc:
            return [
                InvariantViolation(
                    code=misconfig_code,
                    message=f"invariant checks configuration is unreadable: {type(exc).__name__}",
                )
            ]

    if len(selected_checks) == 0:
        return [
            InvariantViolation(
                code=misconfig_code,
                message="invariant checks configuration is empty",
            )
        ]

    violations: list[InvariantViolation] = []

    for idx, row in enumerate(selected_checks):
        if not isinstance(row, tuple) or len(row) != 3:
            violations.append(
                InvariantViolation(
                    code=misconfig_code,
                    message=f"invariant check #{idx} has invalid shape",
                )
            )
            continue
        code, message, check = row
        if not isinstance(code, str) or not code.strip():
            violations.append(
                InvariantViolation(
                    code=misconfig_code,
                    message=f"invariant check #{idx} has invalid code",
                )
            )
            continue
        if not isinstance(message, str) or not message.strip():
            violations.append(
                InvariantViolation(
                    code=misconfig_code,
                    message=f"invariant check {code} has invalid message",
                )
            )
            continue
        if not callable(check):
            violations.append(
                InvariantViolation(
                    code=misconfig_code,
                    message=f"invariant check {code} is not callable",
                )
            )
            continue
        if not check(state):
            violations.append(InvariantViolation(code=code, message=message))

    return violations


def has_invariant_violations(state: Mapping[str, Any], *, enabled: bool = False) -> bool:
    """Convenience wrapper returning whether any violation exists."""
    return bool(evaluate_invariants(state, enabled=enabled))
