# [TASK: claude_bug_pr8c_deepsource_advisory_context_autoderive] PR256 current-head Codacy/review follow-up coverage.
"""Focused DeepSource advisory context auto-derive tests."""

from typing import Any
from unittest import TestCase

import scripts.pr_flow_automation as flow

ASSERTIONS = TestCase()


def _check(name: str, state: str, url: str = "") -> dict[str, str]:
    return {"name": name, "conclusion": state, "detailsUrl": url}


def _deepsource_python_failure_check(**extra: Any) -> dict[str, Any]:
    check: dict[str, Any] = {
        "name": "DeepSource: Python",
        "provider": "deepsource",
        "status": "COMPLETED",
        "conclusion": "FAILURE",
    }
    check.update(extra)
    return check


def _deepsource_python_advisory_failure(**extra: Any) -> dict[str, Any]:
    check = _deepsource_python_failure_check(
        headSha="abc",
        summary="Cyclomatic complexity readability advisory",
    )
    check.update(extra)
    return check


def _deepsource_autoderive_pr(
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
            _deepsource_python_advisory_failure(),
            _check("Codacy Static Code Analysis", "SUCCESS"),
        ],
    }
    payload.update(extra)
    return payload


def _build_deepsource_autoderive_decision(
    monkeypatch,
    checks: list[Any] | None = None,
    review_threads: list[dict[str, Any]] | None = None,
    **pr_extra: Any,
) -> dict[str, Any]:
    monkeypatch.setattr(
        flow,
        "pr_view",
        lambda _repo, _pr: _deepsource_autoderive_pr(checks, **pr_extra),
    )
    return flow.build_decision("owner/repo", "254", ignore_self=True, review_threads=review_threads or [])


def _assert_blocked(decision: dict[str, Any]) -> None:
    ASSERTIONS.assertFalse(decision["can_merge"])
    ASSERTIONS.assertGreaterEqual(len(decision["blockers"]), 1)


def test_check_payload_helpers_handle_none_and_missing_evidence():
    """Malformed check payload evidence normalizes to empty strings."""
    ASSERTIONS.assertEqual(flow._check_head_sha(None), "")
    ASSERTIONS.assertEqual(flow._check_head_sha("not-a-check"), "")
    ASSERTIONS.assertEqual(flow._check_head_sha({}), "")
    ASSERTIONS.assertEqual(flow._check_head_sha({"headSha": None}), "")
    ASSERTIONS.assertEqual(flow._check_advisory_evidence(None), "")
    ASSERTIONS.assertEqual(flow._check_advisory_evidence("not-a-check"), "")
    ASSERTIONS.assertEqual(flow._check_advisory_evidence({}), "")
    ASSERTIONS.assertEqual(flow._check_advisory_evidence({"summary": None}), "")


def test_deepsource_advisory_status_autoderive_none_check_payload_blocks(monkeypatch):
    """Malformed rollup items do not crash and cannot provide advisory evidence."""
    decision = _build_deepsource_autoderive_decision(
        monkeypatch,
        [None, _deepsource_python_advisory_failure(), _check("Codacy Static Code Analysis", "SUCCESS")],
    )

    _assert_blocked(decision)


def test_deepsource_advisory_status_autoderive_one_missing_group_head_blocks(monkeypatch):
    """Every advisory check in a group must carry matching head evidence."""
    decision = _build_deepsource_autoderive_decision(
        monkeypatch,
        [
            _deepsource_python_advisory_failure(name="DeepSource: Python"),
            _deepsource_python_advisory_failure(name="DeepSource: Python / complexity", headSha=""),
            _check("Codacy Static Code Analysis", "SUCCESS"),
        ],
    )

    _assert_blocked(decision)


def test_deepsource_advisory_status_autoderive_mismatched_group_heads_blocks(monkeypatch):
    """Advisory checks from different heads cannot be filtered together."""
    decision = _build_deepsource_autoderive_decision(
        monkeypatch,
        [
            _deepsource_python_advisory_failure(name="DeepSource: Python"),
            _deepsource_python_advisory_failure(name="DeepSource: Python / complexity", headSha="old"),
            _check("Codacy Static Code Analysis", "SUCCESS"),
        ],
    )

    _assert_blocked(decision)


def test_deepsource_advisory_status_autoderives_matching_python_advisories(monkeypatch):
    """Multiple current-head DeepSource Python advisory failures are filtered together."""
    decision = _build_deepsource_autoderive_decision(
        monkeypatch,
        [
            _deepsource_python_advisory_failure(name="DeepSource: Python"),
            _deepsource_python_advisory_failure(name="DeepSource: Python / complexity"),
            _check("Codacy Static Code Analysis", "SUCCESS"),
        ],
    )

    ASSERTIONS.assertTrue(decision["can_merge"])
    ASSERTIONS.assertEqual(decision["blockers"], [])
    ASSERTIONS.assertEqual(decision["reasons"], [])


def test_deepsource_advisory_status_autoderive_codacy_success_rollup_proves_clear(monkeypatch):
    """Live Codacy SUCCESS check can prove annotations clear when no explicit count exists."""
    decision = _build_deepsource_autoderive_decision(monkeypatch)

    ASSERTIONS.assertTrue(decision["can_merge"])
    ASSERTIONS.assertEqual(decision["blockers"], [])


def test_deepsource_advisory_status_autoderive_codacy_failure_or_annotations_block(monkeypatch):
    """Codacy failure states and explicit annotations keep advisory failures blocking."""
    cases = (
        ([_deepsource_python_advisory_failure(), _check("Codacy Static Code Analysis", "ACTION_REQUIRED")], {}),
        ([_deepsource_python_advisory_failure(), _check("Codacy Static Code Analysis", "FAILURE")], {}),
        (None, {"github_annotations_count": 1}),
        (None, {"github_annotations_count": "unknown"}),
    )
    for checks, pr_extra in cases:
        decision = _build_deepsource_autoderive_decision(monkeypatch, checks, **pr_extra)
        _assert_blocked(decision)


def test_deepsource_advisory_status_autoderive_required_deepsource_blocks(monkeypatch):
    """Required or failing current-head DeepSource evidence remains blocking."""
    for pr_extra in (
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
    ):
        decision = _build_deepsource_autoderive_decision(monkeypatch, **pr_extra)
        _assert_blocked(decision)


def test_deepsource_advisory_status_autoderive_no_live_required_evidence_blocks():
    """Absent live DeepSource rollup evidence cannot prove required checks are safe."""
    ASSERTIONS.assertIsNone(flow._deepsource_autoderived_required_failing({}, "abc"))
    ASSERTIONS.assertIsNone(flow._deepsource_autoderived_required_failing({"statusCheckRollup": "bad"}, "abc"))


def test_deepsource_advisory_status_autoderive_missing_advisory_evidence_blocks(monkeypatch):
    """URL/name-only advisory wording is not enough to auto-derive advisory evidence."""
    decision = _build_deepsource_autoderive_decision(
        monkeypatch,
        [
            _deepsource_python_failure_check(
                headSha="abc",
                name="DeepSource: Python / readability advisory",
                detailsUrl="https://example.test/complexity-advisory",
            ),
            _check("Codacy Static Code Analysis", "SUCCESS"),
        ],
    )

    _assert_blocked(decision)


def test_deepsource_advisory_status_autoderive_safety_signal_blocks(monkeypatch):
    """Safety, fail-open, correctness, and regression wording cannot auto-derive."""
    for signal in ("security", "fail-open", "correctness", "regression"):
        decision = _build_deepsource_autoderive_decision(
            monkeypatch,
            [
                _deepsource_python_advisory_failure(
                    summary=f"Cyclomatic complexity readability advisory with {signal} concern"
                ),
                _check("Codacy Static Code Analysis", "SUCCESS"),
            ],
        )
        _assert_blocked(decision)


def test_deepsource_autoderived_context_missing_malformed_count_blocks():
    """Malformed or absent Codacy count evidence fails closed without comparison errors."""
    context = {
        "current_head_sha": "abc",
        "evidence_head_sha": "abc",
        "deepsource_advisory_evidence": "Cyclomatic complexity readability advisory",
        "codacy_state": "SUCCESS",
        "codacy_annotations_count": None,
        "deepsource_required_current_head_check_failing": False,
    }

    ASSERTIONS.assertTrue(flow._deepsource_autoderived_context_missing(context))
