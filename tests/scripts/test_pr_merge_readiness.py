from __future__ import annotations

from typing import Any

import scripts.pr_flow_automation as flow


class _Assertions:
    @staticmethod
    def assertTrue(value: object) -> None:
        assert value is True

    @staticmethod
    def assertFalse(value: object) -> None:
        assert value is False


ASSERTIONS = _Assertions()


def _deepsource_python_failure_check(**extra: Any) -> dict[str, Any]:
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


def _required_check_evidence_context(**extra: Any) -> dict[str, Any]:
    context = {
        "current_head_sha": "head-sha",
        "evidence_head_sha": "head-sha",
        "evidence_present": True,
        "deepsource_advisory_evidence": "DeepSource Python complexity/readability advisory, non-required.",
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
    return flow.deepsource_advisory_status_nonblocking_evidence(
        _deepsource_python_failure_check(**check_extra),
        context,
    )


def test_branch_protection_absent_deepsource_failure_is_nonblocking_with_required_check_evidence():
    decision = _decision(_required_check_evidence_context())

    ASSERTIONS.assertTrue(decision["nonblocking"])


def test_branch_protection_present_without_deepsource_required_is_nonblocking():
    decision = _decision(
        _required_check_evidence_context(
            branch_protection_absent=False,
            required_checks=["Codacy Static Code Analysis", "unit"],
            deepsource_required=False,
            deepsource_blocking=False,
        )
    )

    ASSERTIONS.assertTrue(decision["nonblocking"])


def test_deepsource_absent_and_not_required_evidence_is_nonblocking_for_optional_provider():
    decision = _decision(
        _required_check_evidence_context(
            branch_protection_absent=False,
            required_checks=["Codacy Static Code Analysis"],
            deepsource_required=False,
            deepsource_blocking=False,
        )
    )

    ASSERTIONS.assertTrue(decision["nonblocking"])


def test_stale_deepsource_failure_is_nonblocking_when_current_head_evidence_is_nonrequired():
    decision = _decision(_required_check_evidence_context(), headSha="old-head")

    ASSERTIONS.assertTrue(decision["nonblocking"])


def test_missing_required_check_evidence_blocks_deepsource_advisory():
    context = _required_check_evidence_context()
    for key in (
        "branch_protection_absent",
        "required_checks",
        "required_checks_source",
        "required_checks_head_sha",
        "deepsource_required",
        "deepsource_blocking",
    ):
        context.pop(key, None)

    decision = _decision(context)

    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_branch_protection_api_error_blocks_deepsource_advisory():
    decision = _decision(_required_check_evidence_context(branch_protection_api_error=True))

    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_malformed_required_checks_blocks_deepsource_advisory():
    decision = _decision(_required_check_evidence_context(required_checks="Codacy Static Code Analysis"))

    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_required_deepsource_failure_blocks_deepsource_advisory():
    decision = _decision(
        _required_check_evidence_context(
            branch_protection_absent=False,
            required_checks=["DeepSource: Python"],
            deepsource_required=True,
        )
    )

    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_deepsource_blocking_flag_blocks_deepsource_advisory():
    decision = _decision(_required_check_evidence_context(deepsource_blocking=True))

    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_deepsource_required_current_head_failure_blocks_deepsource_advisory():
    decision = _decision(_required_check_evidence_context(deepsource_required_current_head_check_failing=True))

    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_codacy_failure_blocks_deepsource_advisory():
    decision = _decision(_required_check_evidence_context(codacy_state="FAILURE"))

    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_codacy_annotations_block_deepsource_advisory():
    decision = _decision(_required_check_evidence_context(codacy_annotations_count=1))

    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_unresolved_active_reviews_block_deepsource_advisory():
    decision = _decision(_required_check_evidence_context(unresolved_active=1))

    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_required_check_evidence_head_mismatch_blocks_deepsource_advisory():
    decision = _decision(_required_check_evidence_context(required_checks_head_sha="old-head"))

    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_branch_protection_absent_with_required_checks_conflict_blocks_deepsource_advisory():
    decision = _decision(
        _required_check_evidence_context(
            branch_protection_absent=True,
            required_checks=["Codacy Static Code Analysis"],
        )
    )

    ASSERTIONS.assertFalse(decision["nonblocking"])


def test_unknown_provider_state_blocks_without_explicit_success_state():
    decision = _decision(
        _required_check_evidence_context(
            branch_protection_absent=False,
            required_checks=[{"name": "Unknown provider", "state": "UNKNOWN"}],
            deepsource_required=False,
            deepsource_blocking=False,
        )
    )

    ASSERTIONS.assertFalse(decision["nonblocking"])
