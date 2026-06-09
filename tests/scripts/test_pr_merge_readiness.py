"""PR8E required-check evidence readiness tests."""

# pylint: disable=duplicate-code

from __future__ import annotations

from typing import Any
from unittest import TestCase

import scripts.pr_flow_automation as flow

ASSERTIONS = TestCase(methodName="runTest")


def _ds_failure_check(**extra: Any) -> dict[str, Any]:
    """Build a completed failing DeepSource Python check."""
    check = {
        "name": "DeepSource: Python",
        "provider": "deepsource",
        "status": "COMPLETED",
        "state": "FAILURE",
        "conclusion": "FAILURE",
        "completed": True,
        "url": "https://app.deepsource.com/gh/owner/repo/run/python/",
        "headSha": "head-sha",
    }
    check.update(extra)
    return check


def _required_context(**extra: Any) -> dict[str, Any]:
    """Build explicit current-head required-check evidence."""
    context = {
        "current_head_sha": "head-sha",
        "evidence_head_sha": "head-sha",
        "evidence_present": True,
        "deepsource_advisory_evidence": "Cyclomatic complexity readability advisory",
        "codacy_state": "SUCCESS",
        "codacy_annotations_count": 0,
        "unresolved_active": 0,
        "pending_checks": [],
        "pending_checks_count": 0,
        "checks_green": True,
        "deepsource_required_current_head_check_failing": False,
        "branch_protection_absent": True,
        "required_checks": [],
        "required_checks_source": "branch_protection",
        "required_checks_head_sha": "head-sha",
        "deepsource_required": False,
        "deepsource_blocking": False,
    }
    context.update(extra)
    return context


def _decision(context: dict[str, Any], **check_extra: Any) -> dict[str, Any]:
    """Evaluate DeepSource advisory readiness with a failing check."""
    return flow.deepsource_advisory_status_nonblocking_evidence(
        _ds_failure_check(**check_extra),
        context,
    )


def test_absent_bp_nonblocking():
    """Absent branch protection plus empty required checks is nonblocking."""
    decision = _decision(_required_context())
    ASSERTIONS.assertTrue(decision["nonblocking"])


def test_bp_without_ds():
    """Branch protection excluding DeepSource keeps advisory nonblocking."""
    decision = _decision(
        _required_context(
            branch_protection_absent=False,
            required_checks=["Codacy Static Code Analysis", "unit"],
        )
    )
    ASSERTIONS.assertTrue(decision["nonblocking"])


def test_optional_ds_absent():
    """Absent optional DeepSource status is not blocking."""
    decision = _decision(
        _required_context(
            branch_protection_absent=False,
            required_checks=["Codacy Static Code Analysis"],
        )
    )
    ASSERTIONS.assertTrue(decision["nonblocking"])


def test_stale_ds_nonblocking():
    """Stale DeepSource failure is ignored with current-head proof."""
    decision = _decision(_required_context(), headSha="old-head")
    ASSERTIONS.assertTrue(decision["nonblocking"])


def test_missing_evidence_blocks():
    """Metadata-only evidence is insufficient."""
    context = _required_context()
    for key in (
        "branch_protection_absent",
        "required_checks",
        "deepsource_required",
        "deepsource_blocking",
    ):
        context.pop(key, None)
    decision = _decision(context)
    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_metadata_only_blocks():
    """Source and head metadata alone cannot prove non-required status."""
    context = _required_context()
    for key in ("branch_protection_absent", "required_checks", "deepsource_required", "deepsource_blocking"):
        context.pop(key, None)
    decision = _decision(context)
    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_required_api_error_blocks():
    """Required-check API errors fail closed."""
    decision = _decision(_required_context(required_checks_api_error=True))
    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_bp_api_error_blocks():
    """Branch protection API errors fail closed."""
    decision = _decision(_required_context(branch_protection_api_error=True))
    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_bad_checks_blocks():
    """Malformed required-check containers fail closed."""
    decision = _decision(_required_context(required_checks="Codacy Static Code Analysis"))
    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_required_ds_blocks():
    """Required DeepSource failures remain blocking."""
    decision = _decision(
        _required_context(
            branch_protection_absent=False,
            required_checks=["DeepSource: Python"],
            deepsource_required=True,
        )
    )
    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_ds_blocking_blocks():
    """Explicit DeepSource blocking evidence blocks."""
    decision = _decision(_required_context(deepsource_blocking=True))
    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_required_ds_fail_blocks():
    """Current-head required DeepSource failure blocks."""
    decision = _decision(_required_context(deepsource_required_current_head_check_failing=True))
    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_codacy_fail_blocks():
    """Codacy failure keeps advisory manual."""
    decision = _decision(_required_context(codacy_state="FAILURE"))
    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_codacy_notes_block():
    """Codacy annotations keep advisory manual."""
    decision = _decision(_required_context(codacy_annotations_count=1))
    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_active_reviews_block():
    """Active unresolved review evidence blocks."""
    decision = _decision(_required_context(unresolved_active=1))
    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_head_mismatch_blocks():
    """Required-check evidence must match current head."""
    decision = _decision(_required_context(required_checks_head_sha="old-head"))
    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_absent_bp_conflict_blocks():
    """Absent branch protection conflicts with non-empty required checks."""
    decision = _decision(_required_context(required_checks=["Codacy Static Code Analysis"]))
    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_unknown_state_blocks():
    """Unknown required-check states fail closed."""
    decision = _decision(
        _required_context(
            branch_protection_absent=False,
            required_checks=[{"name": "Unknown provider", "state": "UNKNOWN"}],
        )
    )
    ASSERTIONS.assertFalse(decision["nonblocking"])
