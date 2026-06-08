# [TASK: claude_bug_pr8c_deepsource_advisory_context_autoderive] PR256 current-head Codacy/review follow-up coverage.
"""Focused DeepSource advisory context auto-derive tests."""

from typing import Any
from unittest import TestCase

import scripts.pr_flow_automation as flow

ASSERTIONS = TestCase()


def _chk(name: str, state: str, url: str = "") -> dict[str, str]:
    return {"name": name, "conclusion": state, "detailsUrl": url}


def _check_run(name: str, conclusion: str, url: str = "") -> dict[str, str]:
    return {"__typename": "CheckRun", "name": name, "conclusion": conclusion, "detailsUrl": url}


def _status_context(context: str, state: str, url: str = "", **extra: Any) -> dict[str, Any]:
    check: dict[str, Any] = {
        "__typename": "StatusContext",
        "context": context,
        "state": state,
        "targetUrl": url,
    }
    check.update(extra)
    return check


def _ds_fail(**extra: Any) -> dict[str, Any]:
    check: dict[str, Any] = {
        "name": "DeepSource: Python",
        "provider": "deepsource",
        "status": "COMPLETED",
        "conclusion": "FAILURE",
    }
    check.update(extra)
    return check


def _ds_adv(**extra: Any) -> dict[str, Any]:
    check = _ds_fail(
        headSha="abc",
        summary="Cyclomatic complexity readability advisory",
    )
    check.update(extra)
    return check


def _pr_payload(
    checks: list[Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload = {
        "state": "OPEN",
        "isDraft": False,
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
        "headRefOid": "abc",
        "statusCheckRollup": checks
        or [
            _ds_adv(),
            _chk("Codacy Static Code Analysis", "SUCCESS"),
        ],
    }
    payload.update(extra)
    return payload


def _decision(
    monkeypatch,
    checks: list[Any] | None = None,
    reviews: list[dict[str, Any]] | None = None,
    **pr_extra: Any,
) -> dict[str, Any]:
    monkeypatch.setattr(
        flow,
        "pr_view",
        lambda _repo, _pr: _pr_payload(checks, **pr_extra),
    )
    return flow.build_decision("owner/repo", "256", ignore_self=True, review_threads=reviews or [])


def _blocked(decision: dict[str, Any]) -> None:
    ASSERTIONS.assertFalse(decision["can_merge"])
    ASSERTIONS.assertGreaterEqual(len(decision["blockers"]), 1)


def test_bad_payloads_empty_text():
    """Malformed check payload evidence normalizes to empty strings."""
    checks = flow.split_checks(
        {
            "statusCheckRollup": [
                None,
                "not-a-check",
                {},
                {
                    "name": "DeepSource: Python",
                    "conclusion": "FAILURE",
                    "headSha": None,
                    "summary": None,
                },
            ]
        }
    )

    ASSERTIONS.assertEqual(checks["blockers"][-1]["head_sha"], "")
    ASSERTIONS.assertEqual(checks["blockers"][-1]["advisory_evidence"], "")


def test_none_payload_blocks(monkeypatch):
    """Malformed rollup items do not crash and cannot provide advisory evidence."""
    decision = _decision(
        monkeypatch,
        [None, _ds_adv(), _chk("Codacy Static Code Analysis", "SUCCESS")],
    )

    _blocked(decision)


def test_missing_group_head_blocks(monkeypatch):
    """Every advisory check in a group must carry matching head evidence."""
    decision = _decision(
        monkeypatch,
        [
            _ds_adv(name="DeepSource: Python"),
            _ds_adv(name="DeepSource: Python / complexity", headSha=""),
            _chk("Codacy Static Code Analysis", "SUCCESS"),
        ],
    )

    _blocked(decision)


def test_mismatched_heads_block(monkeypatch):
    """Advisory checks from different heads cannot be filtered together."""
    decision = _decision(
        monkeypatch,
        [
            _ds_adv(name="DeepSource: Python"),
            _ds_adv(name="DeepSource: Python / complexity", headSha="old"),
            _chk("Codacy Static Code Analysis", "SUCCESS"),
        ],
    )

    _blocked(decision)


def test_live_missing_evidence(monkeypatch):
    """Live gh StatusContext shape does not infer advisory evidence from name or URL."""
    decision = _decision(
        monkeypatch,
        [
            _status_context(
                "DeepSource: Python / readability advisory",
                "FAILURE",
                "https://example.test/complexity-advisory",
            ),
            _check_run("Codacy Static Code Analysis", "SUCCESS"),
        ],
    )

    _blocked(decision)


def test_live_top_evidence_pass(monkeypatch):
    """Top-level explicit evidence can pair with live gh rollup head derivation."""
    decision = _decision(
        monkeypatch,
        [
            _status_context("DeepSource: Python", "FAILURE", "https://example.test/deepsource"),
            _check_run("Codacy Static Code Analysis", "SUCCESS"),
        ],
        deepsource_advisory_evidence="Cyclomatic complexity readability advisory",
    )

    ASSERTIONS.assertTrue(decision["can_merge"])
    ASSERTIONS.assertEqual(decision["blockers"], [])


def test_required_live_deepsource_without_head_blocks(monkeypatch):
    """Required live DeepSource rollup failure without headSha fails closed."""
    decision = _decision(
        monkeypatch,
        [
            _status_context(
                "DeepSource: Python",
                "FAILURE",
                "https://example.test/deepsource",
                required=True,
            ),
            _check_run("Codacy Static Code Analysis", "SUCCESS"),
        ],
        deepsource_advisory_evidence="Cyclomatic complexity readability advisory",
    )

    _blocked(decision)


def test_blocking_live_deepsource_without_head_blocks(monkeypatch):
    """Blocking live DeepSource rollup failure without headSha fails closed."""
    decision = _decision(
        monkeypatch,
        [
            _status_context(
                "DeepSource: Python",
                "FAILURE",
                "https://example.test/deepsource",
                blocking=True,
            ),
            _check_run("Codacy Static Code Analysis", "SUCCESS"),
        ],
        deepsource_advisory_evidence="Cyclomatic complexity readability advisory",
    )

    _blocked(decision)


def test_non_required_live_deepsource_advisory_without_head_passes(monkeypatch):
    """Non-required live advisory evidence remains nonblocking."""
    decision = _decision(
        monkeypatch,
        [
            _status_context("DeepSource: Python", "FAILURE", "https://example.test/deepsource"),
            _check_run("Codacy Static Code Analysis", "SUCCESS"),
        ],
        deepsource_advisory_evidence="Cyclomatic complexity readability advisory",
    )

    ASSERTIONS.assertTrue(decision["can_merge"])
    ASSERTIONS.assertEqual(decision["blockers"], [])


def test_head_mismatch_blocks(monkeypatch):
    """Explicit mismatching DeepSource head evidence wins over live rollup derivation."""
    decision = _decision(
        monkeypatch,
        [
            _ds_adv(__typename="CheckRun", headSha="old"),
            _check_run("Codacy Static Code Analysis", "SUCCESS"),
        ],
        deepsource_advisory_evidence="Cyclomatic complexity readability advisory",
    )

    _blocked(decision)


def test_matching_advisories_pass(monkeypatch):
    """Multiple current-head DeepSource Python advisory failures are filtered together."""
    decision = _decision(
        monkeypatch,
        [
            _ds_adv(name="DeepSource: Python"),
            _ds_adv(name="DeepSource: Python / complexity"),
            _chk("Codacy Static Code Analysis", "SUCCESS"),
        ],
    )

    ASSERTIONS.assertTrue(decision["can_merge"])
    ASSERTIONS.assertEqual(decision["blockers"], [])
    ASSERTIONS.assertEqual(decision["reasons"], [])


def test_codacy_success_zero(monkeypatch):
    """Live Codacy SUCCESS check can prove annotations clear when no explicit count exists."""
    decision = _decision(monkeypatch)

    ASSERTIONS.assertTrue(decision["can_merge"])
    ASSERTIONS.assertEqual(decision["blockers"], [])


def test_codacy_bad_states_block(monkeypatch):
    """Codacy failure states keep advisory failures blocking."""
    for checks in (
        [_ds_adv(), _chk("Codacy Static Code Analysis", "ACTION_REQUIRED")],
        [_ds_adv(), _chk("Codacy Static Code Analysis", "FAILURE")],
        [_ds_adv()],
    ):
        decision = _decision(monkeypatch, checks)
        _blocked(decision)


def test_codacy_counts_block(monkeypatch):
    """Explicit non-zero or malformed annotation counts fail closed."""
    cases: list[dict[str, Any]] = [
        {"github_annotations_count": 1},
        {"github_annotations_count": "unknown"},
        {"codacy_annotations_count": 2},
    ]
    for pr_extra in cases:
        decision = _decision(monkeypatch, **pr_extra)
        _blocked(decision)


def test_required_deepsource_blocks(monkeypatch):
    """Required or failing current-head DeepSource evidence remains blocking."""
    cases: list[dict[str, Any]] = [
        {"deepsource_required_current_head_check_failing": True},
        {
            "required_failing_checks": [
                {
                    "name": "DeepSource: Python",
                    "provider": "deepsource",
                    "state": "FAILURE",
                    "head_sha": "abc",
                }
            ]
        },
        {"required_checks": ["DeepSource: Python"]},
        {"deepsource_required_current_head_check_failing": None},
    ]
    for pr_extra in cases:
        decision = _decision(monkeypatch, **pr_extra)
        _blocked(decision)


def test_missing_evidence_blocks(monkeypatch):
    """URL/name-only advisory wording is not enough to auto-derive advisory evidence."""
    decision = _decision(
        monkeypatch,
        [
            _ds_fail(
                headSha="abc",
                name="DeepSource: Python / readability advisory",
                detailsUrl="https://example.test/complexity-advisory",
            ),
            _chk("Codacy Static Code Analysis", "SUCCESS"),
        ],
    )

    _blocked(decision)


def test_blocking_wording_blocks(monkeypatch):
    """Safety, fail-open, correctness, regression, and bypass wording cannot auto-derive."""
    for signal in ("security", "fail-open", "correctness", "regression", "bypass"):
        decision = _decision(
            monkeypatch,
            [
                _ds_adv(
                    summary=f"Cyclomatic complexity readability advisory with {signal} concern"
                ),
                _chk("Codacy Static Code Analysis", "SUCCESS"),
            ],
        )
        _blocked(decision)


def test_pending_check_blocks(monkeypatch):
    """Pending non-DeepSource checks are not bypassed."""
    decision = _decision(
        monkeypatch,
        [
            _ds_adv(),
            _chk("Codacy Static Code Analysis", "SUCCESS"),
            _chk("Unit tests", "PENDING"),
        ],
    )

    _blocked(decision)


def test_other_failure_blocks(monkeypatch):
    """Non-DeepSource failures are not bypassed."""
    decision = _decision(
        monkeypatch,
        [
            _ds_adv(),
            _chk("Codacy Static Code Analysis", "SUCCESS"),
            _chk("Unit tests", "FAILURE"),
        ],
    )

    _blocked(decision)


def test_active_review_blocks(monkeypatch):
    """Active review threads are not bypassed."""
    decision = _decision(monkeypatch, reviews=[{"id": "thread-1", "isResolved": False}])

    _blocked(decision)


def test_pr254_context_still_passes(monkeypatch):
    """Explicit PR254 context still feeds the existing advisory evidence gate."""
    context = {
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "evidence_present": True,
        "deepsource_advisory_evidence": "Cyclomatic complexity readability advisory",
        "codacy_state": "SUCCESS",
        "codacy_annotations_count": 0,
        "unresolved_active": 0,
        "pending_checks_count": 0,
        "checks_green": True,
        "deepsource_required_current_head_check_failing": False,
    }
    decision = _decision(
        monkeypatch,
        [_ds_fail(), _chk("Codacy Static Code Analysis", "SUCCESS")],
        deepsource_advisory_context=context,
    )

    ASSERTIONS.assertTrue(decision["can_merge"])
    ASSERTIONS.assertEqual(decision["blockers"], [])
