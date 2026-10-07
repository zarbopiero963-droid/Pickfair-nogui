"""Bounded PR repair policy. This ledger belongs to automation, never Pickfair.

Assessment and owner-comment inputs must come from the trusted orchestrator's
current-head inspection / authenticated GitHub API, never from reviewer prose.
Keep the ledger outside disposable checkouts; missing state denies mutations.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import subprocess
import uuid
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any

MAX_FIX_LOOP_ITERATIONS_PER_PR = 5
PROTECTED_RISKS = (
    "safety", "security", "credentials", "real_money", "fail_open",
    "data_corruption", "risk_bypass", "runtime_bug", "owner_contract_violation",
)


def stopped(reason: str) -> dict[str, Any]:
    return {"allowed": False, "AUTO_PR_FLOW_STATUS": "NEEDS_MANUAL",
            "REASON": reason, "reason": reason, "next_action": "needs_manual",
            "review_status": "REVIEW_CHURN" if reason.startswith("review_churn") else ""}


def _assessment_invalid_reason(assessment: dict[str, Any]) -> str:
    if not isinstance(assessment, dict):
        return "malformed_assessment"
    evidence = assessment.get("evidence")
    boolean_fields = (*PROTECTED_RISKS, "current_head_correct", "material", "contract_critical",
                      "scope_forbidden", "owner_decision_required")
    if any(type(assessment[key]) is not bool for key in boolean_fields if key in assessment):
        return "malformed_assessment"
    if not isinstance(evidence, str) or not evidence.strip() or not all(
        isinstance(assessment.get(key), str) and assessment[key].strip()
        for key in ("current_head_sha", "thread_id")
    ):
        return "insufficient_current_head_evidence"
    return ""


def _correct_head_decision(assessment: dict[str, Any]) -> tuple[str, str]:
    kind = assessment.get("class")
    if kind in {"STALE", "DUPLICATE", "ALREADY_COVERED"}:
        return "EVIDENCE_RESOLVE", "current_head_covered"
    if kind == "THEORETICAL_MUTATION":
        return "NEEDS_MANUAL", "theoretical_mutation"
    if kind == "GUARDRAIL_GAP":
        critical = assessment.get("contract_critical") is True or any(
            assessment.get(risk) is True for risk in PROTECTED_RISKS
        )
        if critical and assessment.get("material") is True:
            return "PATCH_REQUIRED", "material_guardrail_gap"
        return "NEEDS_MANUAL", "noncritical_guardrail_gap"
    return "NEEDS_MANUAL", "unclassified_finding"


def triage(assessment: dict[str, Any]) -> dict[str, Any]:
    """Classify verified evidence, independently of reviewer identity."""
    invalid = _assessment_invalid_reason(assessment)
    kind = assessment.get("class") if isinstance(assessment, dict) else ""
    if invalid:
        return {"decision": "NEEDS_MANUAL", "reason": invalid, "finding_class": kind}
    decision, reason = "NEEDS_MANUAL", "current_head_not_proven_correct"
    if assessment.get("scope_forbidden") is True or assessment.get("owner_decision_required") is True:
        reason = "scope_or_owner_decision"
    elif kind == "CURRENT_DEFECT" and assessment.get("current_head_correct") is False:
        decision, reason = "PATCH_REQUIRED", "current_defect"
    elif assessment.get("current_head_correct") is not True:
        reason = "current_head_not_proven_correct"
    else:
        decision, reason = _correct_head_decision(assessment)
    return {"decision": decision, "reason": reason, "finding_class": kind}


def owner_comment_matches(comment: dict[str, Any], repo: str, pr: int) -> bool:
    """Check identity of an API-fetched comment, not a reviewer-supplied claim."""
    identifier = str(comment.get("id") or "")
    return bool(
        identifier.isdigit() and comment.get("repo") == repo
        and comment.get("pr") == pr and comment.get("author") == repo.split("/")[0]
        and comment.get("html_url") ==
        f"https://github.com/{repo}/pull/{pr}#issuecomment-{identifier}"
    )


def accept_limitation(assessment: dict[str, Any], comment: dict[str, Any],
                      repo: str, pr: int) -> dict[str, Any]:
    decision = triage(assessment)
    if decision["reason"] not in {"theoretical_mutation", "noncritical_guardrail_gap"}:
        return stopped("finding_not_a_verified_limitation")
    if decision["decision"] != "NEEDS_MANUAL" or assessment.get("current_head_correct") is not True:
        return stopped("current_defect_cannot_be_accepted")
    if assessment.get("material") is not False or any(assessment.get(risk) is not False
                                                     for risk in PROTECTED_RISKS):
        return stopped("material_risk_requires_manual_decision")
    if assessment.get("class") not in {"GUARDRAIL_GAP", "THEORETICAL_MUTATION"}:
        return stopped("finding_not_a_known_limitation")
    if assessment.get("scope_forbidden") is True or assessment.get("owner_decision_required") is True:
        return stopped("scope_or_owner_decision")
    thread, head = assessment.get("thread_id"), assessment.get("current_head_sha")
    if not thread or not head:
        return stopped("missing_current_head_or_thread")
    expected = f"KNOWN_LIMITATION_ACCEPTED_BY_OWNER PR={pr} THREAD={thread} HEAD={head}"
    if not owner_comment_matches(comment, repo, pr) or str(comment.get("body", "")).strip() != expected:
        return stopped("explicit_owner_acceptance_missing")
    return {
        "allowed": True, "decision": "KNOWN_LIMITATION_ACCEPTED_BY_OWNER",
        "owner_decision_url": comment["html_url"],
        "reply_body": f"KNOWN_LIMITATION_ACCEPTED_BY_OWNER — current head {head}; "
                      f"finding {thread}; limit remains. Owner decision: {comment['html_url']}. "
                      f"Evidence: {assessment['evidence']}",
    }


class FixLoopLedger:
    """Transactional, PR-scoped reservations and completed patch→push cycles.

    Reservations hold a slot before any patch. An interrupted reservation blocks
    another cycle; it is never refunded automatically. Push transport retries use
    the same cycle id. Completed cycles and grants are append-only.
    """

    def __init__(self, path: str | Path, repo: str, pr: int):
        if not re.fullmatch(r"[\w.-]+/[\w.-]+", repo) or type(pr) is not int or pr <= 0:
            raise ValueError("invalid PR identity")
        self.path, self.repo, self.pr = Path(path), repo, pr

    @contextmanager
    def _connect(self):
        if not self.path.is_file() or self.path.is_symlink():
            raise ValueError("fix_loop_state_missing")
        connection = sqlite3.connect(f"{self.path.resolve().as_uri()}?mode=rw", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("BEGIN IMMEDIATE")
            with connection:
                yield connection
        finally:
            connection.close()

    def initialize(self, historical_count: int, history_evidence: str) -> None:
        """Explicit bootstrap only: new PR or attested complete repair history."""
        if type(historical_count) is not int or historical_count < 0 or not history_evidence.strip():
            raise ValueError("complete repair history required")
        if self.path.is_symlink():
            raise ValueError("ledger symlink forbidden")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path)) as connection, connection:
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS prs (
                    repo TEXT, pr INTEGER, historical_count INTEGER, evidence TEXT,
                    PRIMARY KEY(repo, pr));
                CREATE TABLE IF NOT EXISTS cycles (
                    repo TEXT, pr INTEGER, cycle TEXT, assessment TEXT, head TEXT, stage TEXT,
                    PRIMARY KEY(repo, pr, cycle));
                CREATE TABLE IF NOT EXISTS grants (
                    repo TEXT, pr INTEGER, decision TEXT, ceiling INTEGER, evidence TEXT,
                    PRIMARY KEY(repo, pr, decision));
                CREATE TABLE IF NOT EXISTS observations (
                    repo TEXT, pr INTEGER, area TEXT, assessment TEXT);
            """)
            connection.execute("INSERT OR IGNORE INTO prs VALUES (?, ?, ?, ?)",
                               (self.repo, self.pr, historical_count, history_evidence))

    def _status(self, connection: sqlite3.Connection) -> dict[str, Any]:
        identity = (self.repo, self.pr)
        initial = connection.execute("SELECT historical_count, evidence FROM prs WHERE repo=? AND pr=?", identity).fetchone()
        if initial is None:
            raise ValueError("fix_loop_pr_history_missing")
        if type(initial[0]) is not int or initial[0] < 0 or not isinstance(initial[1], str) or not initial[1].strip():
            raise ValueError("invalid_persisted_history")
        rows = connection.execute("SELECT assessment, head, stage FROM cycles WHERE repo=? AND pr=?", identity).fetchall()
        for row in rows:
            self._validate_cycle_row(row)
        completed, reserved = sum(row[1] is not None for row in rows), len(rows)
        grants = connection.execute("SELECT decision, ceiling, evidence FROM grants WHERE repo=? AND pr=?", identity).fetchall()
        for row in grants:
            self._validate_grant_row(row, initial[0] + completed)
        grant = max((row[1] for row in grants), default=0)
        if initial[0] + reserved > max(MAX_FIX_LOOP_ITERATIONS_PER_PR, grant):
            raise ValueError("persisted_budget_exceeds_ceiling")
        return {"completed_count": initial[0] + completed, "reserved_count": reserved - completed,
                "used_slots": initial[0] + reserved,
                "ceiling": max(MAX_FIX_LOOP_ITERATIONS_PER_PR, grant or 0)}

    def _validate_cycle_row(self, row: sqlite3.Row) -> None:
        if not isinstance(row[0], str):
            raise ValueError("invalid_persisted_assessment")
        evidence = json.loads(row[0])
        if triage(evidence)["decision"] != "PATCH_REQUIRED":
            raise ValueError("invalid_persisted_assessment")
        stage, head = row[2], row[1]
        if stage not in {"reserved", "working", "pushing", "retry_ready", "completed"}:
            raise ValueError("invalid_persisted_stage")
        if (stage == "completed") != (isinstance(head, str) and bool(head.strip())) or (head is not None and stage != "completed"):
            raise ValueError("invalid_persisted_head_stage")
        if stage in {"working", "pushing", "retry_ready"} and not isinstance(evidence.get("claim_token"), str):
            raise ValueError("invalid_persisted_claim")
        if stage in {"working", "pushing", "retry_ready"} and not evidence.get("claim_token"):
            raise ValueError("invalid_persisted_claim")

    def _validate_grant_row(self, row: sqlite3.Row, completed: int) -> None:
        if not isinstance(row[2], str):
            raise ValueError("invalid_persisted_grant")
        comment = json.loads(row[2])
        if type(row[1]) is not int or row[1] <= MAX_FIX_LOOP_ITERATIONS_PER_PR:
            raise ValueError("invalid_persisted_grant")
        if not isinstance(comment, dict) or not owner_comment_matches(comment, self.repo, self.pr) or str(comment.get("id")) != row[0]:
            raise ValueError("invalid_persisted_owner_grant")
        match = re.fullmatch(rf"OWNER_FIX_LOOP_OVERRIDE PR={self.pr} ADDITIONAL=([1-9][0-9]*) AT_COUNT=([0-9]+)", str(comment.get("body", "")).strip())
        if not match or int(match[2]) < MAX_FIX_LOOP_ITERATIONS_PER_PR or int(match[2]) > completed or row[1] != int(match[1]) + int(match[2]):
            raise ValueError("invalid_persisted_grant_bounds")

    def status(self) -> dict[str, Any]:
        try:
            with self._connect() as connection:
                return {"allowed": True, **self._status(connection)}
        except (ValueError, sqlite3.Error, OSError) as error:
            return stopped(f"fix_loop_state_unavailable:{error}")

    def reserve(self, cycle: str, assessment: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(assessment, dict):
            return stopped("malformed_assessment")
        if not isinstance(cycle, str) or not cycle.strip():
            return stopped("cycle_id_missing")
        finding = triage(assessment)
        try:
            with self._connect() as connection:
                state = self._status(connection)
                area = str(assessment.get("area") or assessment.get("thread_id") or "")
                if finding["decision"] != "PATCH_REQUIRED":
                    previous = connection.execute(
                        "SELECT count(*) FROM observations WHERE repo=? AND pr=? AND area=?",
                        (self.repo, self.pr, area),
                    ).fetchone()[0]
                    connection.execute("INSERT INTO observations VALUES (?, ?, ?, ?)",
                                       (self.repo, self.pr, area, json.dumps(assessment)))
                    churn = previous and area and assessment.get("current_head_correct") is True
                    reason = "review_churn" if churn else finding["reason"]
                    if churn and state["used_slots"] >= state["ceiling"]:
                        reason = "review_churn_or_fix_loop_budget_exhausted"
                    return stopped(reason)
                if state["reserved_count"]:
                    return stopped("unfinished_fix_loop_cycle")
                if state["used_slots"] >= state["ceiling"]:
                    return stopped("fix_loop_budget_exhausted")
                connection.execute("INSERT INTO cycles VALUES (?, ?, ?, ?, NULL, 'reserved')",
                                   (self.repo, self.pr, cycle, json.dumps(assessment)))
                return {"allowed": True, "cycle_id": cycle, **self._status(connection)}
        except (ValueError, sqlite3.Error, OSError) as error:
            return stopped(f"fix_loop_state_unavailable:{error}")

    def authorize(self, cycle: str, current_head: str | None = None, stage: str = "reserved",
                  claim_token: str | None = None) -> dict[str, Any]:
        try:
            with self._connect() as connection:
                state = self._status(connection)
                row = connection.execute("SELECT head, stage, assessment FROM cycles WHERE repo=? AND pr=? AND cycle=?",
                                         (self.repo, self.pr, cycle)).fetchone()
                if row is None and state["used_slots"] >= state["ceiling"]:
                    return stopped("fix_loop_budget_exhausted")
                if stage not in {"reserved", "working", "retry_ready"} or row is None or row[0] is not None or row[1] != stage or state["used_slots"] > state["ceiling"]:
                    return stopped("fix_loop_reservation_invalid")
                evidence = json.loads(row[2])
                if evidence.get("claim_token") and evidence["claim_token"] != claim_token:
                    return stopped("fix_loop_claim_mismatch")
                if current_head is not None and (not current_head or evidence.get("current_head_sha") != current_head):
                    return stopped("fix_loop_assessed_head_mismatch")
                return {"allowed": True, **state}
        except (ValueError, sqlite3.Error, OSError) as error:
            return stopped(f"fix_loop_state_unavailable:{error}")

    def claim_patch(self, cycle: str, head: str) -> dict[str, Any]:
        """Give one worker an exclusive capability before any file mutation."""
        try:
            with self._connect() as connection:
                state = self._status(connection)
                row = connection.execute("SELECT assessment, stage FROM cycles WHERE repo=? AND pr=? AND cycle=?",
                                         (self.repo, self.pr, cycle)).fetchone()
                if row is None or row[1] != "reserved" or state["used_slots"] > state["ceiling"]:
                    return stopped("fix_loop_reservation_invalid")
                evidence = json.loads(row[0])
                if not head or evidence.get("current_head_sha") != head:
                    return stopped("fix_loop_assessed_head_mismatch")
                token = uuid.uuid4().hex
                evidence["claim_token"] = token
                connection.execute("UPDATE cycles SET stage='working', assessment=? WHERE repo=? AND pr=? AND cycle=?",
                                   (json.dumps(evidence), self.repo, self.pr, cycle))
                return {"allowed": True, "claim_token": token}
        except (ValueError, sqlite3.Error, OSError) as error:
            return stopped(f"fix_loop_state_unavailable:{error}")

    def _transition_push(self, cycle: str, expected: str, target: str) -> dict[str, Any]:
        try:
            with self._connect() as connection:
                state = self._status(connection)
                if state["used_slots"] > state["ceiling"]:
                    return stopped("fix_loop_budget_exhausted")
                updated = connection.execute(
                    "UPDATE cycles SET stage=? WHERE repo=? AND pr=? AND cycle=? AND head IS NULL AND stage=?",
                    (target, self.repo, self.pr, cycle, expected),
                ).rowcount
                if updated != 1:
                    return stopped("fix_loop_push_state_uncertain")
                return {"allowed": True}
        except (ValueError, sqlite3.Error, OSError) as error:
            return stopped(f"fix_loop_state_unavailable:{error}")

    def start_push(self, cycle: str, claim_token: str | None = None) -> dict[str, Any]:
        """Persist intent before dispatch: crash/response lost cannot replay it."""
        permission = self.authorize(cycle, stage="working", claim_token=claim_token)
        return self._transition_push(cycle, "working", "pushing") if permission["allowed"] else permission

    def _retry_after_confirmed_rejection(self, cycle: str) -> dict[str, Any]:
        """Internal transport retry only after an actual non-fast-forward rejection."""
        return self._transition_push(cycle, "pushing", "retry_ready")

    def complete(self, cycle: str, head: str) -> dict[str, Any]:
        if not isinstance(head, str) or not head.strip():
            return stopped("confirmed_push_head_missing")
        try:
            with self._connect() as connection:
                row = connection.execute("SELECT head, stage FROM cycles WHERE repo=? AND pr=? AND cycle=?",
                                         (self.repo, self.pr, cycle)).fetchone()
                if row is None or (row[0] is not None and row[0] != head):
                    return stopped("fix_loop_completion_mismatch")
                if row[0] is None and row[1] != "pushing":
                    return stopped("fix_loop_push_intent_missing")
                connection.execute("UPDATE cycles SET head=?, stage='completed' WHERE repo=? AND pr=? AND cycle=? AND head IS NULL",
                                   (head, self.repo, self.pr, cycle))
                return {"allowed": True, **self._status(connection)}
        except (ValueError, sqlite3.Error, OSError) as error:
            return stopped(f"fix_loop_state_unavailable:{error}")

    def grant_override(self, comment: dict[str, Any]) -> None:
        if not owner_comment_matches(comment, self.repo, self.pr):
            raise ValueError("owner override identity mismatch")
        matches = re.findall(
            rf"OWNER_FIX_LOOP_OVERRIDE PR={self.pr} ADDITIONAL=([1-9][0-9]*) AT_COUNT=([0-9]+)",
            str(comment.get("body", "")).strip(),
        )
        expected = (f"OWNER_FIX_LOOP_OVERRIDE PR={self.pr} ADDITIONAL={matches[0][0]} AT_COUNT={matches[0][1]}"
                    if len(matches) == 1 else "")
        if not expected or str(comment.get("body", "")).strip() != expected:
            raise ValueError("explicit bounded owner override required")
        extra, at_count = map(int, matches[0])
        with self._connect() as connection:
            if connection.execute("SELECT 1 FROM grants WHERE repo=? AND pr=? AND decision=?",
                                  (self.repo, self.pr, str(comment["id"]))).fetchone():
                return
            state = self._status(connection)
            if state["reserved_count"] or state["completed_count"] != at_count or at_count < state["ceiling"]:
                raise ValueError("owner override count mismatch")
            connection.execute("INSERT INTO grants VALUES (?, ?, ?, ?, ?)",
                               (self.repo, self.pr, str(comment["id"]), at_count + extra, json.dumps(comment)))


def reservation_gate(context: dict[str, Any] | None, *, stage: str = "reserved") -> dict[str, Any]:
    """Do not infer/reset the budget from head, author, CI or resolved threads."""
    budget = (context or {}).get("fix_loop")
    if not isinstance(budget, dict):
        return stopped("fix_loop_reservation_missing")
    for key in ("repo", "pr"):
        outer = (context or {}).get(key)
        if outer is not None and str(outer) != str(budget.get(key)):
            return stopped("fix_loop_identity_mismatch")
    try:
        ledger = FixLoopLedger(budget["path"], budget["repo"], budget["pr"])
        head = str((context or {}).get("current_head_sha") or (context or {}).get("head_sha") or (context or {}).get("headRefOid") or "")
        actual_stage = "working" if stage == "reserved" and budget.get("claim_token") else stage
        return ledger.authorize(budget["cycle_id"], head, actual_stage, budget.get("claim_token"))
    except (KeyError, ValueError, TypeError):
        return stopped("fix_loop_reservation_malformed")


def start_push_gate(context: dict[str, Any], *, stage: str = "working") -> dict[str, Any]:
    if stage not in {"working", "retry_ready"}:
        return stopped("exclusive_patch_claim_required")
    permission = reservation_gate(context, stage=stage)
    if not permission["allowed"]:
        return permission
    budget = context["fix_loop"]
    ledger = FixLoopLedger(budget["path"], budget["repo"], budget["pr"])
    return ledger._transition_push(budget["cycle_id"], stage, "pushing")


def claim_patch_gate(context: dict[str, Any] | None) -> dict[str, Any]:
    permission = reservation_gate(context)
    if not permission["allowed"]:
        return permission
    budget = context["fix_loop"]
    if budget.get("claim_token"):
        return permission
    ledger = FixLoopLedger(budget["path"], budget["repo"], budget["pr"])
    head = str(context.get("current_head_sha") or context.get("head_sha") or context.get("headRefOid") or "")
    result = ledger.claim_patch(budget["cycle_id"], head)
    if result["allowed"]:
        budget["claim_token"] = result["claim_token"]
    return result


def retry_after_confirmed_rejection(context: dict[str, Any]) -> dict[str, Any]:
    try:
        budget = context["fix_loop"]
        ledger = FixLoopLedger(budget["path"], budget["repo"], budget["pr"])
        return ledger._retry_after_confirmed_rejection(budget["cycle_id"])
    except (KeyError, ValueError, TypeError):
        return stopped("fix_loop_reservation_malformed")


def fetch_owner_comment(repo: str, pr: int, identifier: str) -> dict[str, Any]:
    """Fetch authorization from GitHub; never accept a reviewer-authored object."""
    if not identifier.isdigit() or not re.fullmatch(r"[\w.-]+/[\w.-]+", repo):
        raise ValueError("invalid owner decision reference")
    raw = subprocess.check_output(  # nosec B603 B607: fixed command, validated API path
        ["gh", "api", f"repos/{repo}/issues/comments/{identifier}"], text=True,
    )
    comment = json.loads(raw)
    return {"repo": repo, "pr": pr, "id": str(comment["id"]),
            "author": comment["user"]["login"], "html_url": comment["html_url"],
            "body": comment["body"]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("initialize", "status", "reserve", "complete", "override"))
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--pr", type=int, required=True)
    parser.add_argument("--cycle", default="")
    parser.add_argument("--assessment-file", default="")
    parser.add_argument("--head", default="")
    parser.add_argument("--historical-count", type=int, default=None)
    parser.add_argument("--history-evidence", default="")
    parser.add_argument("--owner-comment-id", default="")
    args = parser.parse_args()
    ledger = FixLoopLedger(args.ledger, args.repo, args.pr)
    try:
        if args.action == "initialize":
            ledger.initialize(args.historical_count, args.history_evidence)
            result = ledger.status()
        elif args.action == "reserve":
            result = ledger.reserve(args.cycle, json.loads(Path(args.assessment_file).read_text(encoding="utf-8")))
        elif args.action == "complete":
            result = ledger.complete(args.cycle, args.head)
        elif args.action == "override":
            ledger.grant_override(fetch_owner_comment(args.repo, args.pr, args.owner_comment_id))
            result = ledger.status()
        else:
            result = ledger.status()
    except (ValueError, TypeError, KeyError, OSError, sqlite3.Error, subprocess.SubprocessError) as error:
        result = stopped(f"policy_evidence_unavailable:{error}")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["allowed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
