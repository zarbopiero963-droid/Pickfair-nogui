#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import scripts.pr_automation_controller as controller
except ModuleNotFoundError:
    import pr_automation_controller as controller

SELF_CHECK_NAMES = {
    "safe pr autofix",
    "pr autofix safe supervisor",
    "merge readiness",
    "pr merge readiness",
}

OK_STATES = {"SUCCESS", "SKIPPED", "NEUTRAL"}
PENDING_STATES = {"", "PENDING", "QUEUED", "IN_PROGRESS", "WAITING", "REQUESTED"}
BAD_STATES = {"FAILURE", "ERROR", "ACTION_REQUIRED", "CANCELLED", "TIMED_OUT", "STARTUP_FAILURE", "STALE"}

FLOW_WORKFLOWS = {
    "PR Autofix Safe Supervisor",
    "PR Merge Readiness",
    "PR Self Check Refresh",
    "PR Flow Guardrails",
}

ALLOWED_COMMAND_FAMILIES = {"gh", "git", "python", "python3", "pytest"}


def validate_command(cmd: list[str]) -> list[str]:
    """Validate and normalize a shell command."""
    if not cmd:
        raise ValueError("empty command")
    validate_command_family(Path(str(cmd[0])).name)
    validate_command_args(cmd)
    safe_cmd = list(cmd)
    executable = shutil.which(safe_cmd[0])
    if not executable:
        raise ValueError(f"command not found: {safe_cmd[0]}")
    safe_cmd[0] = executable
    return safe_cmd


def validate_command_family(family: str) -> None:
    """Reject command families outside the local allowlist."""
    if family not in ALLOWED_COMMAND_FAMILIES:
        raise ValueError(f"command family not allowed: {family}")


def validate_command_args(cmd: list[str]) -> None:
    """Reject non-string or null-byte command arguments."""
    for arg in cmd:
        if not isinstance(arg, str) or "\x00" in arg:
            raise ValueError("invalid command argument")


def sh(cmd: list[str], *, check: bool = True) -> str:
    safe_cmd = validate_command(cmd)
    proc = subprocess.run(  # nosec B603  # nosemgrep
        safe_cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if check and proc.returncode:
        raise RuntimeError(
            f"command failed: {' '.join(safe_cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    return proc.stdout.strip()


def gh_json(cmd: list[str]) -> Any:
    return json.loads(sh(cmd))


def norm_state(value: Any) -> str:
    return str(value or "").strip().upper()


def check_name(check: dict[str, Any]) -> str:
    return str(check.get("name") or check.get("context") or "").strip()


def check_url(check: dict[str, Any]) -> str:
    return str(check.get("detailsUrl") or check.get("targetUrl") or check.get("url") or "").strip()


def is_self_check(check: dict[str, Any]) -> bool:
    name = check_name(check).lower()
    url = check_url(check).lower()
    return (
        name in SELF_CHECK_NAMES
        or "pr-autofix-safe-supervisor" in url
        or "pr-merge-readiness" in url
        or "pr-self-check-refresh" in url
        or "pr-flow-guardrails" in url
    )


#: Nome ESATTO del check pubblicato dalla GitHub App di Codacy, dismessa.
#: FONTE UNICA nel controller: i due percorsi decisionali (questo gate CI e
#: `pr_automation_controller`) devono usare la STESSA identita', altrimenti
#: divergono — ed e' esattamente quello che era successo: stretto qui al giro
#: 3, lasciato largo nel controller fino al giro 4.
#: Deliberatamente un match esatto e non una sottostringa "codacy": un
#: predicato per sottostringa e' un vettore FAIL-OPEN sul gate di merge —
#: qualunque check futuro col nome che contiene "codacy" (una guardia sulla
#: dismissione, un workflow rinominato) sparirebbe dai blockers anche da
#: FAILURE. Rilievo convergente di Fugu Ultra, Claude Fable 5 e Codex su #462.
#: Un check Codacy con un nome diverso da questi NON e' esente: bloccherebbe,
#: che e' la direzione sicura (fail-closed).
DECOMMISSIONED_CODACY_CHECK_NAMES = controller.DECOMMISSIONED_CODACY_CHECK_NAMES


def is_decommissioned_codacy_check(check: dict[str, Any]) -> bool:
    """Il check pubblicato dalla GitHub App di Codacy, che e' DISMESSA.

    Codacy non ha piu' ne' API ne' token: quel check resta appeso finche'
    l'owner non disinstalla l'app, e la sua ACTION_REQUIRED e' stantia per
    definizione. Un servizio dismesso non e' un segnale: non blocca (non e'
    un blocker) e non tiene in attesa (non e' un pending).

    Rilievo Codex P1 su #462: il gate CI gira `pr_merge_readiness.py`, che
    delega QUI. Filtrare il check solo nel controller non toglieva il falso
    rosso, perche' il controller non sta su questo percorso.
    """
    return check_name(check).strip().lower() in DECOMMISSIONED_CODACY_CHECK_NAMES


def pr_view(repo: str, pr: str) -> dict[str, Any]:
    return gh_json([
        "gh", "pr", "view", str(pr),
        "--repo", repo,
        "--json",
        "state,isDraft,mergeable,mergeStateStatus,reviewDecision,headRefOid,baseRefName,headRefName,statusCheckRollup,url,mergedAt,mergedBy,mergeCommit",
    ])


def split_checks(pr: dict[str, Any], *, ignore_self: bool = True) -> dict[str, list[dict[str, Any]]]:
    blockers: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    ignored: list[dict[str, Any]] = []
    self_stale: list[dict[str, Any]] = []
    # Quanti check REALI sono stati osservati: esclusi il self-check del gate e
    # i servizi dismessi, inclusi quelli gia' passati.
    visti = 0

    for payload in pr.get("statusCheckRollup") or []:
        raw = payload if isinstance(payload, dict) else {}
        item = {
            "name": check_name(raw),
            "state": norm_state(raw.get("conclusion") or raw.get("state") or raw.get("status")),
            "url": check_url(raw),
            "rollup_typename": str(raw.get("__typename") or ""),
            "provider": str(raw.get("provider") or raw.get("source") or ""),
            "source": str(raw.get("source") or raw.get("provider") or ""),
            "status": raw.get("status"),
            "conclusion": raw.get("conclusion"),
            "external_status": raw.get("external_status"),
            "analysis_status": raw.get("analysis_status"),
            "head_sha": _check_head_sha(raw),
            "advisory_evidence": _check_advisory_evidence(raw),
            "is_self_check": is_self_check(raw),
        }

        if ignore_self and item["is_self_check"]:
            item["ignored"] = True
            ignored.append(item)
            if item["state"] in BAD_STATES:
                self_stale.append(item)
            continue

        # Codacy e' DISMESSO: il check residuo della GitHub App non e' ne' un
        # blocker ne' un pending, qualunque sia il suo stato (vedi
        # is_decommissioned_codacy_check).
        if is_decommissioned_codacy_check(raw):
            item["ignored"] = True
            item["decommissioned"] = True
            ignored.append(item)
            continue

        item["ignored"] = False
        # I check gia' VERDI non finiscono in nessuna lista: vanno contati qui,
        # o "tutto verde" e "nessun check ancora registrato" diventano
        # indistinguibili — ed e' la differenza fra "pronta" e "non lo so".
        visti += 1
        if item["state"] in PENDING_STATES:
            pending.append(item)
        elif item["state"] not in OK_STATES:
            blockers.append(item)

    return {
        "blockers": blockers,
        "pending": pending,
        "ignored": ignored,
        "self_stale": self_stale,
        "checks_seen": visti,
    }


def effective_merge_state_ok(merge_state: str, blockers: list[dict[str, Any]], pending: list[dict[str, Any]]) -> bool:
    state = norm_state(merge_state)
    if state == "CLEAN":
        return True
    if state == "UNSTABLE" and not blockers and not pending:
        return True
    return False


def _check_head_sha(check: Any) -> str:
    payload = _dict_payload(check)
    suite = _dict_payload(payload.get("checkSuite"))
    commit = _dict_payload(payload.get("commit"))
    return _clean_check_text(
        first_nonempty(
            payload.get("head_sha"),
            payload.get("headSha"),
            payload.get("commitOid"),
            payload.get("sha"),
            suite.get("headSha"),
            commit.get("oid"),
        )
    )


def _check_advisory_evidence(check: Any) -> str:
    payload = _dict_payload(check)
    output = _dict_payload(payload.get("output"))
    return _clean_check_text(
        first_nonempty(
            payload.get("advisory_evidence"),
            payload.get("summary"),
            payload.get("description"),
            payload.get("message"),
            payload.get("text"),
            output.get("summary"),
            output.get("text"),
        )
    )


def _dict_payload(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _clean_check_text(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() in {"none", "null"} else text



def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
















def dict_items_from_list(value: Any) -> list[dict[str, Any]]:
    """Return only dictionary entries when the payload is a list."""
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]






def nested_dict(source: dict[str, Any], key: str) -> dict[str, Any]:
    """Return a nested dictionary value or an empty dictionary."""
    value = source.get(key)
    return value if isinstance(value, dict) else {}


def first_nonempty(*values: Any) -> Any:
    """Return the first truthy value or an empty string."""
    for value in values:
        if value:
            return value
    return ""






def build_post_fix_micro_audit_prompt(task_text: str, context: dict[str, Any] | None = None) -> str:
    return controller.build_post_fix_micro_audit_prompt(task_text, context)


def build_codex_task_prompt(context: dict[str, Any] | None = None) -> str:
    return controller.build_codex_task_prompt(context)


def ensure_codex_prompt_contract(prompt: str, context: dict[str, Any] | None = None) -> str:
    return controller.ensure_codex_prompt_contract(prompt, context)


def validate_codex_prompt_contract(prompt: str) -> dict[str, Any]:
    return controller.validate_codex_prompt_contract(prompt)


def codex_prompt_contract_missing_sections(prompt: str) -> list[str]:
    return controller.codex_prompt_contract_missing_sections(prompt)


def build_phase0_preflight_prompt(context: dict[str, Any] | None = None) -> str:
    return controller.build_phase0_preflight_prompt(context)


def ensure_phase0_preflight_section(prompt: str, context: dict[str, Any] | None = None) -> str:
    return controller.ensure_phase0_preflight_section(prompt, context)


def parse_phase0_preflight_result(text: str) -> dict[str, Any]:
    return controller.parse_phase0_preflight_result(text)


def phase0_preflight_status(report: dict[str, Any] | None) -> str:
    return controller.phase0_preflight_status(report)


def phase0_preflight_failed(report: dict[str, Any] | None) -> bool:
    return controller.phase0_preflight_failed(report)


def ensure_post_fix_micro_audit_section(prompt: str) -> str:
    return controller.ensure_post_fix_micro_audit_section(prompt)


def parse_post_fix_micro_audit_result(text: str) -> dict[str, Any]:
    return controller.parse_post_fix_micro_audit_result(text)


def post_fix_micro_audit_status(report: dict[str, Any] | None) -> str:
    return controller.post_fix_micro_audit_status(report)


def post_fix_micro_audit_failed(report: dict[str, Any] | None) -> bool:
    return controller.post_fix_micro_audit_failed(report)








def post_or_update_comment(repo: str, pr: str, marker: str, body: str) -> None:
    payload_path = Path(".pr-flow-comment-payload.json")
    payload_path.write_text(json.dumps({"body": body}), encoding="utf-8")
    comments = gh_json(["gh", "api", f"repos/{repo}/issues/{pr}/comments", "--paginate"])
    existing_id = None
    for c in comments:
        if marker in (c.get("body") or ""):
            existing_id = c.get("id")
            break
    if existing_id:
        sh(["gh", "api", f"repos/{repo}/issues/comments/{existing_id}", "--method", "PATCH", "--input", str(payload_path)])
    else:
        sh(["gh", "api", f"repos/{repo}/issues/{pr}/comments", "--method", "POST", "--input", str(payload_path)])
    payload_path.unlink(missing_ok=True)


def _active_review_taxonomy_items(review_threads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "name": "Active unresolved review thread",
            "state": "ACTION_REQUIRED",
            "source": "review",
            "reason": "active unresolved review thread",
            "active": True,
            "id": thread.get("id"),
            "path": thread.get("path"),
            "line": thread.get("line"),
            "author": thread.get("author"),
            "url": thread.get("url"),
        }
        for thread in eligible_review_comments_for_auto_resolve(review_threads)
    ]


def build_decision(
    repo: str,
    pr_number: str,
    *,
    ignore_self: bool = True,
    review_threads: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build merge readiness decision and apply blocker taxonomy routing."""
    pr = pr_view(repo, pr_number)
    checks = split_checks(pr, ignore_self=ignore_self)
    active_review_threads = eligible_review_comments_for_auto_resolve(review_threads or [])
    checks = _apply_deepsource_advisory_policy(pr, checks, active_review_threads)
    merge_state = _merge_readiness_state(pr, checks, active_review_threads)
    decision = _base_decision({"repo": repo, "pr_number": pr_number}, pr, checks, merge_state)
    taxonomy_context = _taxonomy_context(pr, checks, merge_state["can_merge"])
    taxonomy_items = _decision_taxonomy_items(checks, review_threads or [])
    taxonomy_items.extend(_merge_conflict_taxonomy_items(pr, _effective_merge_conflict_scope(pr, decision)))
    decision["blocker_taxonomy"] = controller.summarize_blocker_actions(taxonomy_items, taxonomy_context)
    _apply_taxonomy_next_action(decision)
    return decision


def _merge_readiness_state(
    pr_data: dict[str, Any],
    checks: dict[str, Any],
    active_review_threads: list[dict[str, Any]],
) -> dict[str, Any]:
    already_merged = bool(pr_data.get("mergedAt"))
    reasons = _merge_state_reasons(
        already_merged,
        pr_data,
        checks,
        active_review_threads,
    )
    can_merge = _can_merge_from_reasons(already_merged, reasons, active_review_threads)
    return {"already_merged": already_merged, "can_merge": can_merge, "reasons": reasons}


def _merge_state_reasons(
    already_merged: bool,
    pr_data: dict[str, Any],
    checks: dict[str, Any],
    active_review_threads: list[dict[str, Any]],
) -> list[str]:
    if already_merged:
        return []
    reasons = _merge_readiness_reasons(pr_data, checks, active_review_threads)
    review_reason = _reason_review_threads(active_review_threads)
    if review_reason and review_reason not in reasons:
        reasons.append(review_reason)
    return reasons


def _can_merge_from_reasons(
    already_merged: bool,
    reasons: list[str],
    active_review_threads: list[dict[str, Any]],
) -> bool:
    return already_merged or (not reasons and not active_review_threads)


def _reason_no_checks_seen(checks: dict[str, Any]) -> str:
    """Zero check reali osservati non vuol dire "tutto a posto": vuol dire
    "non lo so ancora".

    Rilievo di Claude Fable 5 sulla #463. Nella finestra iniziale dopo il push
    GitHub puo' non aver ancora registrato NESSUN check: `pending` e' vuoto,
    `blockers` e' vuoto, e con `mergeStateStatus` CLEAN la readiness concludeva
    `can_merge=True` — un verde con zero check eseguiti. E' il falso verde
    simmetrico a quello che questa PR toglie, e ora che il gate torna
    affidabile sarebbe quello a cui si crede.
    """
    if checks.get("checks_seen", 0) <= 0:
        return "no real checks observed yet (only self-checks or empty rollup)"
    return ""


def _merge_readiness_reasons(
    pr_data: dict[str, Any],
    checks: dict[str, Any],
    active_review_threads: list[dict[str, Any]],
) -> list[str]:
    return [
        reason
        for reason in (
            _reason_pr_state(pr_data),
            _reason_pr_draft(pr_data),
            _reason_pr_mergeable(pr_data),
            _reason_pr_merge_state_status(pr_data, checks),
            _reason_blocking_checks(checks),
            _reason_pending_checks(checks),
            _reason_no_checks_seen(checks),
            _reason_review_threads(active_review_threads),
        )
        if reason
    ]


def _reason_pr_state(pr_data: dict[str, Any]) -> str:
    return "" if pr_data.get("state") == "OPEN" else f"PR state is {pr_data.get('state')}, expected OPEN"


def _reason_pr_draft(pr_data: dict[str, Any]) -> str:
    return "PR is draft" if pr_data.get("isDraft") else ""


def _reason_pr_mergeable(pr_data: dict[str, Any]) -> str:
    return (
        ""
        if pr_data.get("mergeable") == "MERGEABLE"
        else f"mergeable is {pr_data.get('mergeable')}, expected MERGEABLE"
    )


def _reason_pr_merge_state_status(pr_data: dict[str, Any], checks: dict[str, Any]) -> str:
    if effective_merge_state_ok(str(pr_data.get("mergeStateStatus") or ""), checks["blockers"], checks["pending"]):
        return ""
    return f"mergeStateStatus is {pr_data.get('mergeStateStatus')}, expected CLEAN"


def _reason_blocking_checks(checks: dict[str, Any]) -> str:
    return f"{len(checks['blockers'])} real blocking check(s)" if checks["blockers"] else ""


def _reason_pending_checks(checks: dict[str, Any]) -> str:
    return f"{len(checks['pending'])} real pending check(s)" if checks["pending"] else ""


def _reason_review_threads(active_review_threads: list[dict[str, Any]]) -> str:
    count = len(active_review_threads)
    return f"{count} active unresolved review thread(s)" if count else ""


def _base_decision(
    identity: dict[str, str],
    pr_data: dict[str, Any],
    checks: dict[str, Any],
    merge_state: dict[str, Any],
) -> dict[str, Any]:
    """Build normalized PR decision payload before taxonomy next-action overrides."""
    already_merged = bool(merge_state["already_merged"])
    can_merge = bool(merge_state["can_merge"])
    payload = _base_decision_metadata(identity, pr_data)
    payload.update(_base_decision_checks(checks))
    payload.update(_base_decision_status(pr_data, merge_state, can_merge, already_merged))
    payload.update(_base_decision_result(can_merge, already_merged))
    return payload


def _base_decision_metadata(identity: dict[str, str], pr_data: dict[str, Any]) -> dict[str, Any]:
    return {
        "repo": identity["repo"],
        "pr": str(identity["pr_number"]),
        "url": pr_data.get("url"),
        "state": pr_data.get("state"),
        "mergedAt": pr_data.get("mergedAt"),
        "mergedBy": _pr_actor_login(pr_data.get("mergedBy")),
        "mergeCommit": _pr_oid(pr_data.get("mergeCommit")),
        "headRefName": pr_data.get("headRefName"),
        "headRefOid": pr_data.get("headRefOid"),
        "baseRefName": pr_data.get("baseRefName"),
    }


def _base_decision_checks(checks: dict[str, Any]) -> dict[str, Any]:
    return {
        "blockers": checks["blockers"],
        "pending": checks["pending"],
        "ignored_self_checks": checks["ignored"],
        "self_stale": checks["self_stale"],
        "checks_seen": checks.get("checks_seen", 0),
    }


def _base_decision_status(
    pr_data: dict[str, Any],
    merge_state: dict[str, Any],
    can_merge: bool,
    already_merged: bool,
) -> dict[str, Any]:
    return {
        "already_merged": already_merged,
        "mergeable": pr_data.get("mergeable"),
        "mergeStateStatus": pr_data.get("mergeStateStatus"),
        "reviewDecision": pr_data.get("reviewDecision"),
        "is_draft": bool(pr_data.get("isDraft")),
        "is_open": pr_data.get("state") == "OPEN",
        "can_merge": can_merge,
        "reasons": merge_state["reasons"],
    }


def _base_decision_result(can_merge: bool, already_merged: bool) -> dict[str, Any]:
    next_action = "already_merged" if already_merged else ("ready_to_merge" if can_merge else "blocked")
    return {"next_action": next_action}


def _pr_actor_login(value: Any) -> Any:
    return value.get("login") if isinstance(value, dict) else value


def _pr_oid(value: Any) -> Any:
    return value.get("oid") if isinstance(value, dict) else value


def _taxonomy_context(pr_data: dict[str, Any], checks: dict[str, Any], can_merge: bool) -> dict[str, Any]:
    return {
        "logs_clear": False,
        "can_merge": can_merge,
        "is_draft": bool(pr_data.get("isDraft")),
        "is_open": pr_data.get("state") == "OPEN",
        "mergeable": pr_data.get("mergeable"),
        "mergeStateStatus": pr_data.get("mergeStateStatus"),
        "pending": checks["pending"],
        "bad": checks["blockers"],
        "blockers": checks["blockers"],
        "blockers_count": len(checks["blockers"]),
    }


def _decision_taxonomy_items(checks: dict[str, Any], review_threads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items = list(checks["blockers"])
    items.extend(checks["pending"])
    items.extend(_review_thread_taxonomy_items(review_threads))
    return items


def _merge_conflict_taxonomy_items(pr_data: dict[str, Any], task_scope: dict[str, Any]) -> list[dict[str, Any]]:
    classified = controller.classify_merge_conflict(pr_data, _conflicted_files_from_pr(pr_data), task_scope)
    if str(classified.get("category") or "") != "merge_conflict":
        return []
    return [dict(classified, name="PR merge conflict", state="FAILURE", source="merge")]


def _effective_merge_conflict_scope(pr_data: dict[str, Any], decision: dict[str, Any]) -> dict[str, Any]:
    candidates: list[Any] = [
        pr_data.get("task_scope"),
        pr_data.get("taskScope"),
        pr_data.get("scope"),
        decision.get("task_scope"),
        decision.get("taskScope"),
    ]
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        scoped = _extract_effective_scope(candidate)
        if scoped:
            return scoped
    return {}


def _extract_effective_scope(scope: dict[str, Any]) -> dict[str, Any]:
    scoped: dict[str, Any] = {}
    for key in _scope_keys():
        files = _scope_list_values(scope.get(key))
        if files:
            scoped[key] = files
    return scoped


def _scope_keys() -> tuple[str, ...]:
    return ("files", "allowed_files", "allowlist", "in_scope_files")


def _scope_list_values(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    cleaned: list[str] = []
    for path in value:
        candidate = _clean_scope_path(path)
        if candidate:
            cleaned.append(candidate)
    return cleaned


def _clean_scope_path(path: Any) -> str:
    if not isinstance(path, str):
        return ""
    return path.strip()


def _conflicted_files_from_pr(pr_data: dict[str, Any]) -> list[str]:
    fields = ("conflicted_files", "conflictedFiles", "files")
    for field in fields:
        files = pr_data.get(field)
        if isinstance(files, list):
            return [str(path).strip() for path in files if str(path).strip()]
    return []


def _review_thread_taxonomy_items(review_threads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _active_review_taxonomy_items(review_threads)


def _apply_taxonomy_next_action(decision: dict[str, Any]) -> None:
    if decision.get("already_merged"):
        return
    next_action = _resolved_taxonomy_next_action(decision)
    if next_action:
        decision["next_action"] = next_action


def _resolved_taxonomy_next_action(decision: dict[str, Any]) -> str:
    taxonomy_dict = _taxonomy_dict(decision)
    taxonomy_next_action = _taxonomy_next_action(taxonomy_dict)
    current_action = _current_next_action(decision)
    if _should_set_taxonomy_action(current_action, taxonomy_next_action):
        return taxonomy_next_action
    if _should_force_review_fix(current_action, taxonomy_dict):
        return "fix_review_comments"
    if _can_apply_ready_route(decision):
        return "ready_to_merge"
    return ""


def _should_force_review_fix(current_action: str, taxonomy_dict: dict[str, Any]) -> bool:
    return _should_set_review_fix_action(current_action, taxonomy_dict) or (
        _has_review_blocker(taxonomy_dict) and current_action == "ready_to_merge"
    )


def _taxonomy_dict(decision: dict[str, Any]) -> dict[str, Any]:
    taxonomy = decision.get("blocker_taxonomy")
    return taxonomy if isinstance(taxonomy, dict) else {}


def _taxonomy_next_action(decision: dict[str, Any]) -> str:
    return str(decision.get("next_action") or "").strip()


def _current_next_action(decision: dict[str, Any]) -> str:
    return str(decision.get("next_action") or "").strip()


def _can_apply_ready_route(decision: dict[str, Any]) -> bool:
    taxonomy_dict = _taxonomy_dict(decision)
    return (
        _taxonomy_next_action(taxonomy_dict) == "ready_to_merge"
        and bool(decision.get("can_merge"))
        and not _has_review_blocker(taxonomy_dict)
        and not _is_high_priority_action(_current_next_action(decision))
    )


def _should_set_taxonomy_action(current_action: str, taxonomy_action: str) -> bool:
    if current_action == "blocked" and taxonomy_action == "checks_green_or_no_action":
        return False
    return (
        bool(taxonomy_action)
        and taxonomy_action not in {"ready_to_merge", "fix_review_comments"}
        and not _is_high_priority_action(current_action)
    )


def _has_review_blocker(taxonomy_dict: dict[str, Any]) -> bool:
    categories = taxonomy_dict.get("categories")
    return isinstance(categories, list) and "review_comment_active" in categories


def _is_high_priority_action(action: str) -> bool:
    return action in {
        "wait_pending",
        "needs_manual_merge_conflict",
        "needs_manual_secret",
        "needs_manual_scope_violation",
        "rerun_stale_checks",
        "auto_resolve_merge_conflict",
    }


def _should_set_review_fix_action(current_action: str, taxonomy_dict: dict[str, Any]) -> bool:
    if not _has_review_blocker(taxonomy_dict):
        return False
    return _review_override_allowed(current_action, taxonomy_dict)


def _review_override_allowed(current_action: str, taxonomy_dict: dict[str, Any]) -> bool:
    if not _review_only_blocker(taxonomy_dict):
        return False
    return current_action in {"", "blocked", "checks_green_or_no_action", "ready_to_merge"}


def _review_only_blocker(taxonomy_dict: dict[str, Any]) -> bool:
    categories = taxonomy_dict.get("categories")
    if not isinstance(categories, list):
        return False
    normalized = {str(category).strip() for category in categories if str(category).strip()}
    return normalized == {"review_comment_active"}


def classify_blocker(item: dict[str, Any]) -> dict[str, Any]:
    """Proxy blocker taxonomy classification to controller helper."""
    return controller.classify_blocker(item)


def route_blocker_action(category: str, context: dict[str, Any]) -> str:
    """Proxy blocker taxonomy routing to controller helper."""
    return controller.route_blocker_action(category, context)


def classify_merge_conflict(
    pr_data: dict[str, Any],
    conflicted_files: list[str],
    task_scope: dict[str, Any],
) -> dict[str, Any]:
    """Proxy merge-conflict taxonomy classification to controller helper."""
    return controller.classify_merge_conflict(pr_data, conflicted_files, task_scope)


def summarize_blocker_actions(blockers: list[dict[str, Any]], context: dict[str, Any]) -> dict[str, Any]:
    """Proxy taxonomy summarization to controller helper."""
    return controller.summarize_blocker_actions(blockers, context)


def build_telegram_summary(context: dict[str, Any]) -> dict[str, Any]:
    """Build a Telegram-ready summary payload from workflow context."""
    review = context.get("review")
    review_dict = review if isinstance(review, dict) else {}
    return {
        "pr_number": context.get("pr"),
        "head_sha": context.get("headRefOid"),
        "active_unresolved_review_count": review_dict.get("unresolved_active"),
        "next_action": context.get("next_action"),
    }


def should_notify_ready_to_merge(context: dict[str, Any]) -> bool:
    """Return True when current context indicates PR is ready to merge."""
    bad = context.get("bad")
    unresolved_active = context.get("unresolved_active")
    mergeable = context.get("mergeable")
    merge_state_status = context.get("mergeStateStatus")
    return (
        isinstance(bad, list)
        and not bad
        and not unresolved_active
        and mergeable == "MERGEABLE"
        and merge_state_status == "CLEAN"
    )


def _apply_deepsource_advisory_policy(
    pr_data: dict[str, Any],
    checks: dict[str, list[dict[str, Any]]],
    active_review_threads: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    blockers = checks.get("blockers", [])
    if not blockers:
        return checks
    advisory, kept = _split_deepsource_advisory_blockers(blockers)
    if not advisory:
        return checks
    context = _deepsource_advisory_policy_context(pr_data, checks, active_review_threads, kept, advisory)
    if not context:
        return checks
    decisions = _deepsource_advisory_policy_decisions(advisory, context)
    if not _deepsource_advisory_decisions_nonblocking(decisions):
        return checks
    updated = dict(checks)
    updated["blockers"] = kept
    updated["deepsource_advisory_nonblocking"] = _deepsource_nonblocking_checks(advisory, decisions)
    return updated


def _deepsource_advisory_policy_decisions(
    advisory: list[dict[str, Any]],
    context: dict[str, Any],
) -> list[dict[str, Any]]:
    return [deepsource_advisory_status_nonblocking_evidence(check, context) for check in advisory]


def _deepsource_advisory_decisions_nonblocking(decisions: list[dict[str, Any]]) -> bool:
    return all(decision.get("nonblocking") is True for decision in decisions)


def _split_deepsource_advisory_blockers(
    blockers: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    advisory: list[dict[str, Any]] = []
    kept: list[dict[str, Any]] = []
    for check in blockers:
        target = advisory if _deepsource_advisory_check_reason(check) == "" else kept
        target.append(check)
    return advisory, kept


def _deepsource_nonblocking_checks(
    checks: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        dict(check, advisory_nonblocking_reason=decision.get("reason"))
        for check, decision in zip(checks, decisions)
    ]


def _deepsource_advisory_policy_context(
    pr_data: dict[str, Any],
    checks: dict[str, list[dict[str, Any]]],
    active_review_threads: list[dict[str, Any]],
    non_advisory_blockers: list[dict[str, Any]],
    advisory: list[dict[str, Any]],
) -> dict[str, Any]:
    configured = _deepsource_advisory_configured_context(pr_data)
    if configured:
        configured = _deepsource_completed_advisory_context(pr_data, configured, advisory)
    elif not _has_supplied_deepsource_advisory_context(pr_data):
        configured = _deepsource_autoderived_advisory_context(pr_data, advisory)
    pending = checks.get("pending", [])
    context = dict(configured)
    context.update(_deepsource_merge_policy_context(active_review_threads, pending, non_advisory_blockers))
    context["unresolved_active"] = _deepsource_policy_unresolved_active(configured, active_review_threads)
    _preserve_explicit_deepsource_merge_evidence(context, configured)
    return context


def _preserve_explicit_deepsource_merge_evidence(
    context: dict[str, Any],
    configured: dict[str, Any],
) -> None:
    if _deepsource_has_explicit_pending_checks(configured):
        context["pending_checks"] = configured.get("pending_checks")
    if "pending_checks_count" in configured:
        context["pending_checks_count"] = configured.get("pending_checks_count")
    if configured.get("checks_green") is False:
        context["checks_green"] = False


def _deepsource_has_explicit_pending_checks(context: dict[str, Any]) -> bool:
    pending_checks = context.get("pending_checks")
    return isinstance(pending_checks, list) and bool(pending_checks)


def _deepsource_policy_unresolved_active(
    configured: dict[str, Any],
    active_review_threads: list[dict[str, Any]],
) -> Any:
    if "unresolved_active" in configured:
        return configured.get("unresolved_active")
    return len(active_review_threads)


def _deepsource_merge_policy_context(
    active_review_threads: list[dict[str, Any]],
    pending: list[dict[str, Any]],
    non_advisory_blockers: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "unresolved_active": len(active_review_threads),
        "pending_checks": pending,
        "pending_checks_count": len(pending),
        "checks_green": not non_advisory_blockers and not pending,
    }


def _has_explicit_deepsource_advisory_context(pr_data: dict[str, Any]) -> bool:
    return bool(_deepsource_advisory_configured_context(pr_data))


def _deepsource_advisory_configured_context(pr_data: dict[str, Any]) -> dict[str, Any]:
    if _has_supplied_deepsource_advisory_context(pr_data):
        context = pr_data.get("deepsource_advisory_context")
        return dict(context) if isinstance(context, dict) else {}
    return _deepsource_advisory_top_level_context(pr_data)


def _deepsource_advisory_top_level_context(pr_data: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "current_head_sha",
        "evidence_head_sha",
        "evidence_present",
        "deepsource_advisory_evidence",
        "github_annotations_count",
        "github_annotations",
        "unresolved_active",
        "pending_checks",
        "pending_checks_count",
        "checks_green",
        "deepsource_required_current_head_check_failing",
    )
    context = {key: pr_data.get(key) for key in keys if key in pr_data}
    return context if _deepsource_has_explicit_advisory_evidence(context) else {}


def _deepsource_has_explicit_advisory_evidence(context: dict[str, Any]) -> bool:
    return (
        context.get("evidence_present") is True
        or bool(context.get("deepsource_advisory_evidence"))
    )


def _has_supplied_deepsource_advisory_context(pr_data: dict[str, Any]) -> bool:
    return "deepsource_advisory_context" in pr_data


# [TASK: claude_bug_pr8c_deepsource_advisory_context_autoderive]
def _deepsource_completed_advisory_context(
    pr_data: dict[str, Any],
    configured: dict[str, Any],
    advisory: list[dict[str, Any]],
) -> dict[str, Any]:
    context = dict(configured)
    current_head_sha = _completed_advisory_current_head_sha(pr_data, context)
    _complete_advisory_head_context(pr_data, context, advisory, current_head_sha)
    _complete_advisory_explicit_evidence(context)
    _complete_advisory_required_context(pr_data, context, current_head_sha)
    return context


def _completed_advisory_current_head_sha(pr_data: dict[str, Any], context: dict[str, Any]) -> str:
    return str(context.get("current_head_sha") or pr_data.get("headRefOid") or "").strip()


def _complete_advisory_head_context(
    pr_data: dict[str, Any],
    context: dict[str, Any],
    advisory: list[dict[str, Any]],
    current_head_sha: str,
) -> None:
    if current_head_sha and "current_head_sha" not in context:
        context["current_head_sha"] = current_head_sha
    if "evidence_head_sha" not in context:
        context["evidence_head_sha"] = _deepsource_advisory_group_head_sha(pr_data, advisory)


def _complete_advisory_explicit_evidence(context: dict[str, Any]) -> None:
    if context.get("deepsource_advisory_evidence") and "evidence_present" not in context:
        context["evidence_present"] = True




def _complete_advisory_required_context(
    pr_data: dict[str, Any],
    context: dict[str, Any],
    current_head_sha: str,
) -> None:
    if "deepsource_required_current_head_check_failing" not in context:
        context["deepsource_required_current_head_check_failing"] = _deepsource_autoderived_required_failing(
            pr_data, current_head_sha
        )


def _deepsource_autoderived_advisory_context(
    pr_data: dict[str, Any],
    advisory: list[dict[str, Any]],
) -> dict[str, Any]:
    if _deepsource_autoderived_merge_evidence_malformed(pr_data):
        return {}
    context = _deepsource_autoderived_context(pr_data, advisory)
    if _deepsource_autoderived_context_missing(context):
        return {}
    return context


def _deepsource_autoderived_context(
    pr_data: dict[str, Any],
    advisory: list[dict[str, Any]],
) -> dict[str, Any]:
    current_head_sha = str(pr_data.get("headRefOid") or "").strip()
    return {
        "current_head_sha": current_head_sha,
        "evidence_head_sha": _deepsource_advisory_group_head_sha(pr_data, advisory),
        "evidence_present": True,
        "deepsource_advisory_evidence": _deepsource_advisory_group_evidence(advisory),
        "deepsource_required_current_head_check_failing": _deepsource_autoderived_required_failing(
            pr_data, current_head_sha
        ),
        "required_failing_checks": pr_data.get("required_failing_checks"),
    }


def _deepsource_autoderived_context_missing(context: dict[str, Any]) -> bool:
    return (
        _deepsource_autoderived_head_or_evidence_missing(context)
        or context.get("deepsource_required_current_head_check_failing") is not False
    )


def _deepsource_autoderived_head_or_evidence_missing(context: dict[str, Any]) -> bool:
    return (
        not context.get("current_head_sha")
        or not context.get("evidence_head_sha")
        or not context.get("deepsource_advisory_evidence")
    )




def _deepsource_autoderived_merge_evidence_malformed(pr_data: dict[str, Any]) -> bool:
    return _deepsource_pending_checks_malformed(pr_data) or _deepsource_merge_counts_malformed(pr_data)


def _deepsource_pending_checks_malformed(pr_data: dict[str, Any]) -> bool:
    pending_checks = pr_data.get("pending_checks")
    return "pending_checks" in pr_data and (not isinstance(pending_checks, list) or bool(pending_checks))


def _deepsource_merge_counts_malformed(pr_data: dict[str, Any]) -> bool:
    return any(
        key in pr_data and controller.safe_nonnegative_int(pr_data.get(key), -1) != 0
        for key in ("pending_checks_count", "unresolved_active")
    )


def _deepsource_advisory_group_head_sha(
    pr_data: dict[str, Any],
    advisory: list[dict[str, Any]],
) -> str:
    current_head_sha = str(pr_data.get("headRefOid") or "").strip()
    heads: set[str] = set()
    for check in advisory:
        head = _deepsource_advisory_check_head_sha(check, current_head_sha)
        if not head:
            return ""
        heads.add(head)
    return heads.pop() if len(heads) == 1 else ""


def _deepsource_advisory_check_head_sha(check: dict[str, Any], current_head_sha: str) -> str:
    head = _check_head_sha(check)
    if head:
        return head if head == current_head_sha else ""
    if current_head_sha and _is_live_current_pr_rollup_item(check):
        return current_head_sha
    return ""


def _is_live_current_pr_rollup_item(check: dict[str, Any]) -> bool:
    return (check.get("rollup_typename") or check.get("__typename")) in {"StatusContext", "CheckRun"}


def _deepsource_advisory_group_evidence(advisory: list[dict[str, Any]]) -> str:
    evidence: list[str] = []
    for check in advisory:
        text = _check_advisory_evidence(check)
        if not text or not controller.deepsource_claim_is_advisory(text):
            return ""
        if _deepsource_claim_has_blocking_signal(text.lower()):
            return ""
        evidence.append(text)
    return "\n".join(evidence)




















def _deepsource_autoderived_required_failing(pr_data: dict[str, Any], current_head_sha: str) -> Any:
    if "deepsource_required_current_head_check_failing" in pr_data:
        return pr_data.get("deepsource_required_current_head_check_failing")
    context = dict(pr_data)
    if _deepsource_required_failure_blocking(context, current_head_sha):
        return True
    if _deepsource_required_evidence_ambiguous(context):
        return None
    if _deepsource_affirmative_nonrequired_evidence(context):
        return False
    return None


def _deepsource_required_failure_blocking(context: dict[str, Any], current_head_sha: str) -> bool:
    return (
        not current_head_sha
        or controller.deepsource_required_current_head_check_failing(context, current_head_sha)
        or _deepsource_live_rollup_has_required_failure(context)
        or _deepsource_required_evidence_indicates_required(context)
    )


def _deepsource_affirmative_nonrequired_evidence(context: dict[str, Any]) -> bool:
    if _required_checks_exclude_deepsource_python(context):
        return True
    return _branch_protection_excludes_deepsource_python(context)


def _required_checks_exclude_deepsource_python(context: dict[str, Any]) -> bool:
    required_checks = context.get("required_checks")
    return (
        "required_checks" in context
        and isinstance(required_checks, (list, tuple, set))
        and not _required_names_include_deepsource_python(required_checks)
    )


def _branch_protection_excludes_deepsource_python(context: dict[str, Any]) -> bool:
    for key in ("branchProtectionRule", "branch_protection", "branchProtection"):
        protection = context.get(key)
        if isinstance(protection, dict) and _branch_protection_rule_excludes_deepsource(key, protection):
            return True
    return False


def _branch_protection_rule_excludes_deepsource(key: str, protection: dict[str, Any]) -> bool:
    names = _branch_protection_required_names({key: protection})
    return (
        bool(names)
        and not _required_names_include_deepsource_python(names)
        or _branch_protection_required_names_present_empty(protection)
    )


def _branch_protection_required_names_present_empty(protection: dict[str, Any]) -> bool:
    return any(
        names_key in protection and protection.get(names_key) == []
        for names_key in ("requiredStatusCheckContexts", "required_status_checks", "requiredChecks")
    )


def _deepsource_live_rollup_has_required_failure(context: dict[str, Any]) -> bool:
    return any(
        _is_completed_deepsource_failure(check) and _deepsource_rollup_check_has_required_marker(check)
        for check in _deepsource_live_rollup_checks(context)
    )


def _deepsource_rollup_check_has_required_marker(check: dict[str, Any]) -> bool:
    return any(
        check.get(marker) is True
        for marker in ("required", "isRequired", "blocking", "isBlocking", "requiredStatus")
    )


def _deepsource_live_rollup_checks(context: dict[str, Any]) -> list[dict[str, Any]]:
    rollup = context.get("statusCheckRollup")
    if not isinstance(rollup, list):
        return []
    return [check for check in rollup if isinstance(check, dict) and _is_deepsource_python_check(check)]


def _deepsource_required_evidence_indicates_required(context: dict[str, Any]) -> bool:
    if context.get("deepsource_required") is True or context.get("deepsource_blocking") is True:
        return True
    return _required_names_include_deepsource_python(
        context.get("required_checks")
    ) or _required_names_include_deepsource_python(_branch_protection_required_names(context))


def _required_names_include_deepsource_python(required_checks: Any) -> bool:
    if not isinstance(required_checks, (list, tuple, set)):
        return False
    return any(
        _check_name_mentions_deepsource(name) and _check_name_mentions_python(name)
        for name in required_checks
    )


def _branch_protection_required_names(context: dict[str, Any]) -> list[Any]:
    for key in ("branchProtectionRule", "branch_protection", "branchProtection"):
        protection = context.get(key)
        if not isinstance(protection, dict):
            continue
        for names_key in ("requiredStatusCheckContexts", "required_status_checks", "requiredChecks"):
            names = protection.get(names_key)
            if isinstance(names, list):
                return names
    return []


def _deepsource_required_evidence_ambiguous(context: dict[str, Any]) -> bool:
    if _deepsource_required_list_evidence_ambiguous(context):
        return True
    return _deepsource_branch_protection_evidence_ambiguous(context)


def _deepsource_required_list_evidence_ambiguous(context: dict[str, Any]) -> bool:
    return any(
        key in context and not isinstance(context.get(key), (list, tuple, set))
        for key in ("required_failing_checks", "required_checks")
    )


def _deepsource_branch_protection_evidence_ambiguous(context: dict[str, Any]) -> bool:
    for key in ("branchProtectionRule", "branch_protection", "branchProtection"):
        if key not in context:
            continue
        if _branch_protection_value_ambiguous(context.get(key)):
            return True
    return False


def _branch_protection_value_ambiguous(protection: Any) -> bool:
    if not isinstance(protection, dict):
        return True
    return any(
        names_key in protection and not isinstance(protection.get(names_key), list)
        for names_key in ("requiredStatusCheckContexts", "required_status_checks", "requiredChecks")
    )


def deepsource_advisory_status_nonblocking_evidence(
    check: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    """Return a fail-closed DeepSource advisory external-status decision."""
    manual = {"nonblocking": False, "next_action": "needs_manual"}
    reason = _deepsource_advisory_status_manual_reason(check, context)
    if reason:
        return manual | {"reason": reason}
    return {
        "nonblocking": True,
        "reason": "deepsource_advisory_completed_failure_nonblocking",
        "next_action": "nonblocking",
    }


def _deepsource_advisory_status_manual_reason(check: dict[str, Any], context: dict[str, Any]) -> str:
    check_reason = _deepsource_advisory_check_reason(check)
    if check_reason:
        return check_reason
    evidence_reason = _deepsource_advisory_evidence_reason(context)
    if evidence_reason:
        return evidence_reason
    return _deepsource_required_check_reason(context)


def _deepsource_advisory_check_reason(check: dict[str, Any]) -> str:
    if not _is_deepsource_python_check(check):
        return "not_deepsource_python"
    if not _is_completed_deepsource_failure(check):
        return "deepsource_status_not_completed_failure"
    return ""


def _deepsource_advisory_evidence_reason(context: dict[str, Any]) -> str:
    for decision in (
        _deepsource_current_head_evidence_decision(context),
        _deepsource_advisory_claim_decision(context),
        _deepsource_explicit_evidence_decision(context),
        _deepsource_merge_evidence_decision(context),
    ):
        if decision:
            return decision
    return ""


def _deepsource_explicit_evidence_decision(context: dict[str, Any]) -> str:
    return "" if context.get("evidence_present") is True else "missing_explicit_evidence"




def _deepsource_merge_evidence_decision(context: dict[str, Any]) -> str:
    return "" if _deepsource_merge_evidence_clear(context) else "merge_evidence_not_clear"


def _deepsource_required_check_reason(context: dict[str, Any]) -> str:
    current_head_sha = str(context.get("current_head_sha") or "").strip()
    if context.get("deepsource_required_current_head_check_failing") is not False:
        return "missing_required_deepsource_evidence"
    if controller.deepsource_required_current_head_check_failing(context, current_head_sha):
        return "required_deepsource_check_failing"
    return ""


def _is_deepsource_python_check(check: dict[str, Any]) -> bool:
    name = check_name(check)
    return _check_name_mentions_python(name) and _check_provider_or_name_is_deepsource(check, name)


def _check_name_mentions_python(name: object) -> bool:
    return "python" in str(name or "").lower()


def _check_provider_or_name_is_deepsource(check: dict[str, Any], name: object) -> bool:
    provider = _check_provider_or_source(check, name)
    return _check_provider_is_deepsource(provider) or _check_name_mentions_deepsource(name)


def _check_provider_or_source(check: dict[str, Any], fallback: object) -> str:
    return str(check.get("provider") or check.get("source") or fallback)


def _check_provider_is_deepsource(provider: str) -> bool:
    normalized = controller.review_provider_from_author(provider) or provider.strip().lower()
    return controller.is_deepsource_review_provider(normalized)


def _check_name_mentions_deepsource(name: object) -> bool:
    return "deepsource" in str(name or "").lower()


def _is_completed_deepsource_failure(check: dict[str, Any]) -> bool:
    failure_state, progress_state = _deepsource_check_states(check)
    if failure_state in _DEEPSOURCE_INCOMPLETE_STATES or progress_state in _DEEPSOURCE_INCOMPLETE_STATES:
        return False
    return failure_state == "FAILURE" and progress_state in {"", "COMPLETED", "SUCCESS"}


_DEEPSOURCE_INCOMPLETE_STATES = {
    "CANCELLED",
    "CANCELED",
    "TIMED_OUT",
    "STALE",
    "IN_PROGRESS",
    "PENDING",
    "QUEUED",
    "WAITING",
}


def _deepsource_check_states(check: dict[str, Any]) -> tuple[str, str]:
    return (
        norm_state(check.get("conclusion") or check.get("state") or check.get("external_status")),
        norm_state(check.get("status") or check.get("analysis_status")),
    )


def _deepsource_current_head_evidence_decision(context: dict[str, Any]) -> str:
    current_head = _deepsource_evidence_head_pair(context)
    if not current_head[0]:
        return "missing_current_head_sha"
    if not current_head[1]:
        return "missing_evidence_head_sha"
    if current_head[1] != current_head[0]:
        return "evidence_head_mismatch"
    return ""


def _deepsource_evidence_head_pair(context: dict[str, Any]) -> tuple[str, str]:
    return (
        str(context.get("current_head_sha") or "").strip(),
        str(context.get("evidence_head_sha") or "").strip(),
    )


def _deepsource_advisory_claim_decision(context: dict[str, Any]) -> str:
    claimed_issue = _deepsource_claimed_issue(context)
    if not claimed_issue or not controller.deepsource_claim_is_advisory(claimed_issue):
        return "missing_advisory_evidence"
    if _deepsource_claim_has_blocking_signal(claimed_issue):
        return "deepsource_safety_or_correctness_signal"
    return ""


def _deepsource_claimed_issue(context: dict[str, Any]) -> str:
    return str(
        context.get("deepsource_advisory_evidence")
        or context.get("claimed_issue")
        or context.get("body")
        or ""
    ).lower()


def _deepsource_claim_has_blocking_signal(claimed_issue: str) -> bool:
    return controller.deepsource_claim_is_blocking(
        claimed_issue
    ) or controller.deepsource_claim_is_reproducible_patch_claim(
        claimed_issue
    ) or _deepsource_claim_has_guardrail_signal(claimed_issue)


def _deepsource_claim_has_guardrail_signal(claimed_issue: str) -> bool:
    return bool(
        re.search(
            r"\b(?:safety|security|fail-open|fail open|correctness|regression|bypass)\b",
            claimed_issue,
        )
    )










def _deepsource_annotation_count(value: Any) -> int:
    if isinstance(value, list):
        return len(value)
    if value is None:
        return -1
    return controller.safe_nonnegative_int(value, -1)


def _deepsource_merge_evidence_clear(context: dict[str, Any]) -> bool:
    unresolved_active = controller.safe_nonnegative_int(context.get("unresolved_active"), -1)
    return (
        unresolved_active == 0
        and _deepsource_pending_checks_clear(context)
        and context.get("checks_green") is True
    )


def _deepsource_pending_checks_clear(context: dict[str, Any]) -> bool:
    pending_checks = context.get("pending_checks")
    pending_checks_count = controller.safe_nonnegative_int(context.get("pending_checks_count"), -1)
    return isinstance(pending_checks, list) and not pending_checks and pending_checks_count == 0

def eligible_review_comments_for_auto_resolve(
    review_nodes: list[dict[str, Any]],
    context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Return deterministic review-thread eligibility; fail closed for malformed threads."""
    ctx = context if isinstance(context, dict) else {}
    evidence_only = bool(ctx.get("evidence_only", False))
    eligible: list[dict[str, Any]] = []

    nodes = review_nodes if isinstance(review_nodes, list) else []
    for node in nodes:
        if not isinstance(node, dict):
            continue

        if evidence_only:
            triage = controller.triage_review_thread_contract(node, ctx)
            if (
                triage.get("decision") == "EVIDENCE_RESOLVE"
                and controller.should_resolve_review_thread(node, ctx)
            ):
                eligible.append(node)
            continue

        decision = _eligible_review_triage_decision(node, ctx)
        if decision in {"PATCH_REQUIRED", "EVIDENCE_RESOLVE", "NEEDS_MANUAL"}:
            eligible.append(node)

    return eligible

def _eligible_review_triage_decision(node: dict[str, Any], context: dict[str, Any]) -> str:
    if not _node_is_open_for_triage(node):
        return ""
    triage = controller.triage_review_thread_contract(node, context)
    decision = str(triage.get("decision") or "")
    if decision not in {"PATCH_REQUIRED", "EVIDENCE_RESOLVE", "NEEDS_MANUAL"}:
        return ""
    provider = str(triage.get("provider") or "")
    if (
        controller.is_deepsource_review_provider(provider)
        and decision != "PATCH_REQUIRED"
        and context.get("evidence_only") is not True
    ):
        if decision == "NEEDS_MANUAL" and str(triage.get("reason") or "") == "deepsource_broad_refactor_or_roadmap":
            return decision
        return ""
    if _is_outdated_needs_manual_without_evidence(node, context, decision):
        return ""
    return decision


def _node_is_open_for_triage(node: dict[str, Any]) -> bool:
    if _node_is_resolved(node):
        return False
    if _node_is_inactive(node):
        return False
    return True


def _node_is_resolved(node: dict[str, Any]) -> bool:
    return node.get("isResolved") is True or node.get("is_resolved") is True


def _node_is_inactive(node: dict[str, Any]) -> bool:
    return (
        node.get("isActive") is False
        or node.get("is_active") is False
        or node.get("active") is False
    )


def _is_outdated_needs_manual_without_evidence(
    node: dict[str, Any],
    context: dict[str, Any],
    decision: str,
) -> bool:
    return (
        (node.get("isOutdated") is True or node.get("is_outdated") is True)
        and decision == "NEEDS_MANUAL"
        and not bool(context.get("evidence_present"))
    )


def _decision_is_eligible_for_auto_resolve(decision: str, evidence_only: bool) -> bool:
    if not decision:
        return False
    if evidence_only:
        return decision == "EVIDENCE_RESOLVE"
    return True


def build_passive_review_evidence_resolution_plan(
    review_threads: list[dict[str, Any]],
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a passive evidence-resolution plan without GitHub mutations."""
    return controller.build_review_thread_resolution_plan(review_threads, context if isinstance(context, dict) else {})


def build_passive_rerun_readiness_plan(context: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return passive rerun readiness without executing workflow reruns."""
    return controller.build_passive_rerun_readiness_plan(context)










def _normalized_issue_file(issue: dict[str, Any]) -> str:
    return _normalized_issue_field(issue, "filePath", "filename")


def _normalized_issue_line(issue: dict[str, Any]) -> str:
    return _normalized_issue_field(issue, "lineNumber", "line", "startLine")


def _normalized_issue_column(issue: dict[str, Any]) -> str:
    return _normalized_issue_field(issue, "column", "startColumn")


def _normalized_issue_symbol(issue: dict[str, Any]) -> str:
    return _normalized_issue_field(issue, "symbol", "entity")


def _normalized_issue_field(issue: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = issue.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""




# Per quanti intervalli di poll consecutivi il numero di check osservati deve
# restare INVARIATO prima di considerare il rollup fermo.
#
# Un solo intervallo non basta: rilievo convergente di Claude Fable 5 e Fugu
# Ultra sulla review full-range della #463. Se GitHub ritarda la registrazione
# oltre un singolo intervallo con pochi check gia' verdi, il gate uscirebbe e
# direbbe "pronta" con la suite non ancora comparsa.
#
# Limite DICHIARATO: nessun valore di N elimina la finestra, la stringe
# soltanto. Eliminarla davvero richiederebbe l'elenco dei check ATTESI, che
# invecchierebbe a ogni workflow aggiunto o tolto — e un manifest stantio
# produce falsi ROSSI sistematici, cioe' un danno peggiore del rischio che
# chiude. Misurato sulla #463: 28 check registrati entro 20s dal push; con
# poll=15s, due intervalli coprono 30s di stabilita' oltre ai ~20s che il gate
# impiega ad avviarsi.
INTERVALLI_STABILI_RICHIESTI = 2


def cmd_readiness(args: argparse.Namespace) -> int:
    deadline = time.time() + args.wait_unknown_seconds
    decision = _readiness_decision(args.repo, args.pr, args.ignore_safe_autofix)

    while (
        args.wait_unknown_seconds > 0
        and time.time() < deadline
        and not decision["already_merged"]
        and "review_threads_api_unavailable" not in decision.get("reasons", [])
        and (decision["mergeable"] == "UNKNOWN" or decision["mergeStateStatus"] == "UNKNOWN")
    ):
        time.sleep(args.poll_seconds)
        decision = _readiness_decision(args.repo, args.pr, args.ignore_safe_autofix)

    # I check dell'head non sono istantanei. La run `pull_request` parte ~20s
    # dopo il push, quando decine di check sono ancora in volo: decidere li'
    # significa dire "non pronta" SEMPRE, e quel rosso e' l'unico che si attacca
    # all'head della PR. Si aspetta che i check siano settled, poi si giudica.
    #
    # Anti-stallo: il self-check del gate non compare mai tra i `pending`
    # (`split_checks` lo esclude). Senza quella garanzia questa attesa sarebbe un
    # deadlock — il gate aspetterebbe se stesso fino allo scadere del budget.
    # C'e' un test che la inchioda, perche' se cambiasse si romperebbe qui.
    #
    # Fail-closed: scaduto il budget si giudica lo stato REALE. Se i check non
    # sono finiti `can_merge` resta falso e il gate fallisce. L'attesa serve a
    # dare un giudizio vero, non a fabbricare un verde.
    deadline_pending = time.time() + args.wait_pending_seconds
    # Terza condizione: il rollup non deve piu' CRESCERE.
    #
    # "Ho visto almeno un check" non basta (rilievo di Claude Fable 5 sulla
    # full-range della #463): se un check veloce e' gia' verde mentre gli altri
    # non sono ancora registrati, `pending` e' vuoto e `checks_seen` vale 1 —
    # il gate uscirebbe dall'attesa dichiarando PRONTA con la suite ancora da
    # partire. Riprodotto: 1 check verde, 38 non registrati => can_merge=True.
    #
    # Il criterio giusto e' "il rollup ha smesso di crescere": si aspetta
    # finche' il numero di check osservati cambia fra due letture. Si
    # auto-calibra (niente soglia inventata, che invecchierebbe a ogni
    # workflow aggiunto o tolto) e impone almeno un giro di grazia.
    visti_prima = -1
    intervalli_stabili = 0
    while (
        args.wait_pending_seconds > 0
        and time.time() < deadline_pending
        and not decision["already_merged"]
        and (
            decision.get("pending")
            or decision.get("checks_seen", 0) <= 0
            or intervalli_stabili < INTERVALLI_STABILI_RICHIESTI
        )
    ):
        visti_prima = decision.get("checks_seen", 0)
        time.sleep(args.poll_seconds)
        decision = _readiness_decision(args.repo, args.pr, args.ignore_safe_autofix)
        # Il contatore si aggiorna sulla lettura APPENA FATTA, non su quella
        # precedente. Calcolarlo prima del fetch (come faceva la prima stesura,
        # rilievo di Grok 4.6) significa che la condizione d'uscita consulta un
        # valore che non sa nulla dell'ultimo fetch: se il conteggio cresce
        # proprio all'ultima lettura, il gate esce lo stesso — su un rollup che
        # sta ancora crescendo.
        # La stabilita' si conta SOLO a pending vuoto (rilievo di Fugu Ultra).
        # Un conteggio fermo mentre la suite gira dice che i check ci sono gia'
        # tutti, non che il rollup sia completo: senza questo vincolo il
        # contatore arrivava a N durante l'attesa e il gate usciva nell'istante
        # in cui l'ultimo pending diventava verde, senza mai osservare la
        # finestra DOPO. Costa due poll (~30s) su un gate che ne impiega ~300.
        intervalli_stabili = (
            intervalli_stabili + 1
            if decision.get("checks_seen", 0) == visti_prima
            and not decision.get("pending")
            else 0
        )

    # FAIL-CLOSED sul ramo timeout (rilievo di Claude Fable 5, e contraddiceva
    # quanto avevo dichiarato io nella spec).
    #
    # Uscire dal ciclo per budget scaduto NON e' come uscirne perche' il rollup
    # si e' stabilizzato. Se `pending` e' momentaneamente vuoto mentre i check
    # continuano a comparire, `can_merge` resta vero e il gate pubblicherebbe un
    # verde su una suite incompleta. Il budget e' generoso (900s contro ~20s di
    # registrazione), quindi il caso e' raro: ma "raro" non e' "fail-closed", e
    # un gate che promette fail-closed senza esserlo e' peggio di uno che non lo
    # promette.
    if (
        args.wait_pending_seconds > 0
        and not decision["already_merged"]
        and intervalli_stabili < INTERVALLI_STABILI_RICHIESTI
    ):
        decision["can_merge"] = False
        motivo = (
            f"rollup never stayed stable for {INTERVALLI_STABILI_RICHIESTI} "
            f"poll interval(s) within the wait budget"
        )
        if motivo not in decision.setdefault("reasons", []):
            decision["reasons"].append(motivo)

    print(json.dumps(decision, indent=2, sort_keys=True))
    if args.output:
        write_json(Path(args.output), decision)
    return 0 if decision["can_merge"] or args.no_fail else 1


def _readiness_decision(repo: str, pr_number: str, ignore_self: bool) -> dict[str, Any]:
    try:
        review_threads = fetch_all_review_threads(repo, pr_number)
    except (RuntimeError, ValueError, OSError, AttributeError):
        return _readiness_review_api_blocked(repo, pr_number, ignore_self)
    return build_decision(repo, pr_number, ignore_self=ignore_self, review_threads=review_threads)


def _readiness_review_api_blocked(repo: str, pr_number: str, ignore_self: bool) -> dict[str, Any]:
    decision = build_decision(repo, pr_number, ignore_self=ignore_self, review_threads=None)
    reasons = decision.get("reasons")
    if isinstance(reasons, list) and "review_threads_api_unavailable" not in reasons:
        reasons.append("review_threads_api_unavailable")
    decision["can_merge"] = False
    decision["next_action"] = "needs_manual_review_api"
    return decision


def current_run_id(url: str) -> str:
    m = re.search(r"/actions/runs/(\d+)", url or "")
    return m.group(1) if m else ""


def cmd_cleanup(args: argparse.Namespace) -> int:
    pr = pr_view(args.repo, args.pr)
    branch = pr.get("headRefName") or ""
    head = pr.get("headRefOid") or ""

    runs = gh_json([
        "gh", "run", "list",
        "--repo", args.repo,
        "--limit", str(args.limit),
        "--json", "databaseId,status,conclusion,event,workflowName,createdAt,headBranch,headSha,url",
    ])

    candidates = []
    for r in runs:
        if r.get("status") == "completed":
            continue
        wf = r.get("workflowName") or ""
        same_pr = r.get("headBranch") == branch or r.get("headSha") == head
        if wf in FLOW_WORKFLOWS and same_pr:
            candidates.append(r)

    by_workflow: dict[str, list[dict[str, Any]]] = {}
    for r in candidates:
        by_workflow.setdefault(r.get("workflowName") or "", []).append(r)

    cancelled = []
    kept = []
    for wf, items in by_workflow.items():
        items.sort(key=lambda x: x.get("createdAt") or "", reverse=True)
        keep = items[: args.keep_per_workflow]
        old = items[args.keep_per_workflow :]
        kept.extend(keep)
        for r in old:
            entry = {
                "databaseId": r.get("databaseId"),
                "workflowName": wf,
                "status": r.get("status"),
                "url": r.get("url"),
                "dry_run": args.dry_run,
            }
            if not args.dry_run:
                sh(["gh", "run", "cancel", str(r["databaseId"]), "--repo", args.repo], check=False)
            cancelled.append(entry)

    result = {
        "action": "cleanup_stale_runs",
        "repo": args.repo,
        "pr": str(args.pr),
        "branch": branch,
        "head": head,
        "kept": kept,
        "cancelled": cancelled,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.output:
        write_json(Path(args.output), result)
    return 0


def safe_autofix_commits(branch_ref: str, pr: str) -> list[str]:
    out = sh([
        "git", "log", "--format=%H", "--grep", f"Safe autofix PR {pr}", branch_ref,
    ], check=False)
    return [x for x in out.splitlines() if x.strip()]


def changed_files_for_commit(commit: str) -> list[str]:
    out = sh(["git", "show", "--name-only", "--format=", commit], check=False)
    return [x for x in out.splitlines() if x.strip()]


def cmd_preflight(args: argparse.Namespace) -> int:
    pr = pr_view(args.repo, args.pr)
    checks = split_checks(pr, ignore_self=True)
    issues: list[str] = []
    warnings: list[str] = []

    if os.environ.get("HAS_PICKFAIR_ACTIONS_TOKEN", "").lower() not in {"true", "1", "yes"}:
        warnings.append("PICKFAIR_ACTIONS_TOKEN appears missing/empty")

    branch = pr.get("headRefName") or ""
    branch_ref = f"origin/{branch}"
    sh(["git", "fetch", "origin", branch], check=True)

    commits = safe_autofix_commits(branch_ref, str(args.pr)) if branch else []
    file_touches: dict[str, int] = {}
    for c in commits:
        for f in changed_files_for_commit(c):
            file_touches[f] = file_touches.get(f, 0) + 1

    # Guard anti-loop autofix: prima era gatato su "Codacy sta bloccando"; con
    # Codacy dismesso il gate diventerebbe morto, quindi il limite vale SEMPRE
    # (piu' severo, mai piu' permissivo).
    if len(commits) > args.max_safe_autofix_commits:
        issues.append(f"safe autofix commit limit exceeded: {len(commits)} > {args.max_safe_autofix_commits}")

    oscillating = [
        {"file": f, "touches": n}
        for f, n in sorted(file_touches.items())
        if n >= args.oscillation_touch_limit
    ]
    if oscillating:
        issues.append("possible autofix oscillation detected")

    result = {
        "repo": args.repo,
        "pr": str(args.pr),
        "state": pr.get("state"),
        "head": pr.get("headRefOid"),
        "branch": branch,
        "safe_autofix_commits": commits,
        "file_touches": file_touches,
        "oscillating_files": oscillating,
        "issues": issues,
        "warnings": warnings,
        "ok": not issues,
    }

    print(json.dumps(result, indent=2, sort_keys=True))
    if args.output:
        write_json(Path(args.output), result)

    if issues and args.comment:
        marker = "<!-- pickfair-pr-flow-preflight -->"
        body = marker + "\n" + "## PR flow preflight stopped\n\n" + "\n".join(f"- {x}" for x in issues)
        if warnings:
            body += "\n\nWarnings:\n" + "\n".join(f"- {x}" for x in warnings)
        post_or_update_comment(args.repo, str(args.pr), marker, body)

    return 0 if not issues or args.no_fail else 1


def cmd_report(args: argparse.Namespace) -> int:
    review_nodes: list[dict[str, Any]] = []
    try:
        review_nodes = fetch_all_review_threads(args.repo, args.pr)
    except (RuntimeError, ValueError, OSError, AttributeError):
        review_nodes = []
    decision = build_decision(args.repo, args.pr, ignore_self=True, review_threads=review_nodes)
    blockers = decision.get("blockers")
    _ = blockers if isinstance(blockers, list) else []
    unresolved_active = len(eligible_review_comments_for_auto_resolve(review_nodes))
    review = {"unresolved_active": unresolved_active, "total_threads": len(review_nodes)}
    decision["review"] = review

    decision["ready_to_merge_notification"] = should_notify_ready_to_merge({
        "bad": decision.get("blockers"),
        "unresolved_active": unresolved_active,
        "mergeable": decision.get("mergeable"),
        "mergeStateStatus": decision.get("mergeStateStatus"),
    })
    decision["review_auto_resolve_candidates"] = unresolved_active
    decision["telegram_summary"] = build_telegram_summary({
        "pr": decision.get("pr"),
        "headRefOid": decision.get("headRefOid"),
        "review": review,
        "next_action": decision.get("next_action"),
    })
    outdir = Path(args.outdir)
    write_json(outdir / "pr-flow-decision.json", decision)

    if args.comment:
        marker = "<!-- pickfair-pr-flow-report -->"
        status = "✅ Merge allowed" if decision["can_merge"] and not decision["already_merged"] else (
            "✅ Already merged" if decision["already_merged"] else "⛔ Blocked"
        )
        lines = [
            marker,
            f"## PR flow report: {status}",
            "",
            f"- PR: #{args.pr}",
            f"- State: `{decision['state']}`",
            f"- Mergeable: `{decision['mergeable']}`",
            f"- Merge state: `{decision['mergeStateStatus']}`",
            f"- Real blockers: `{len(decision['blockers'])}`",
            f"- Real pending: `{len(decision['pending'])}`",
            f"- Ignored self-checks: `{len(decision['ignored_self_checks'])}`",
            f"- Next action: `{decision['next_action']}`",
        ]
        if decision["reasons"]:
            lines.append("")
            lines.append("Reasons:")
            lines.extend(f"- {r}" for r in decision["reasons"])
        post_or_update_comment(args.repo, str(args.pr), marker, "\n".join(lines) + "\n")

    print(json.dumps(decision, indent=2, sort_keys=True))
    return 0 if decision["can_merge"] or decision["already_merged"] or args.no_fail else 1


def fetch_all_review_threads(repo: str, pr_number: str | int) -> list[dict[str, Any]]:
    """Fetch every review-thread node for a PR using GraphQL pagination."""
    after_cursor: str | None = None
    collected: list[dict[str, Any]] = []
    while True:
        page = _review_threads_page(repo, pr_number, after_cursor)
        collected.extend(_review_threads_nodes(page))
        next_cursor = _review_threads_next_cursor(page)
        if not next_cursor:
            break
        after_cursor = next_cursor
    return collected


def _review_threads_page_query() -> str:
    return (
        "query($owner:String!, $name:String!, $number:Int!, $after:String) "
        "{ repository(owner:$owner, name:$name) { pullRequest(number:$number) "
        "{ reviewThreads(first:100, after:$after) { "
        "nodes { id isResolved isOutdated } pageInfo { hasNextPage endCursor }"
        " } } } }"
    )


def _review_threads_page(repo: str, pr_number: str | int, after_cursor: str | None = None) -> dict[str, Any]:
    owner, name = str(repo).split("/", 1)
    cmd = [
        "gh", "api", "graphql",
        "-f", f"owner={owner}",
        "-f", f"name={name}",
        "-F", f"number={pr_number}",
        "-f", f"query={_review_threads_page_query()}",
    ]
    if after_cursor:
        cmd.extend(["-f", f"after={after_cursor}"])
    return gh_json(cmd)


def _review_threads_nodes(review_raw: dict[str, Any]) -> list[dict[str, Any]]:
    review_threads = (
        review_raw.get("data", {})
        .get("repository", {})
        .get("pullRequest", {})
        .get("reviewThreads", {})
    )
    if not isinstance(review_threads, dict):
        return []
    nodes = review_threads.get("nodes", [])
    if not isinstance(nodes, list):
        return []
    return [node for node in nodes if isinstance(node, dict)]


def _review_threads_next_cursor(review_raw: dict[str, Any]) -> str:
    review_threads = (
        review_raw.get("data", {})
        .get("repository", {})
        .get("pullRequest", {})
        .get("reviewThreads", {})
    )
    if not isinstance(review_threads, dict):
        return ""
    page_info = review_threads.get("pageInfo", {})
    if not isinstance(page_info, dict) or not page_info.get("hasNextPage"):
        return ""
    next_cursor = page_info.get("endCursor")
    return str(next_cursor) if next_cursor else ""




def is_non_fast_forward_push_error(exc: Exception) -> bool:
    """Return True when a push failure looks like a non-fast-forward conflict."""
    message = str(exc).lower()
    return "non-fast-forward" in message or "failed to push some refs" in message


@dataclass
class PushResultContext:
    repo: str
    branch: str
    retried: bool
    needs_manual: bool
    error: str | None = None
    initial_error: str | None = None


@dataclass
class PushRetryResult:
    ok: bool
    status: str
    ctx: PushResultContext

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "ok": self.ok,
            "status": self.status,
            "repo": self.ctx.repo,
            "branch": self.ctx.branch,
            "retried": self.ctx.retried,
            "needs_manual": self.ctx.needs_manual,
        }
        if self.ctx.error is not None:
            result["error"] = self.ctx.error
        if self.ctx.initial_error is not None:
            result["initial_error"] = self.ctx.initial_error
        return result


@dataclass
class NonFastForwardRetryContext:
    repo: str
    branch: str
    initial_exc: RuntimeError


def _push_initial(run_func: Any, remote: str, branch: str) -> None:
    run_func(["git", "push", remote, branch], check=True)


def _fetch_branch(run_func: Any, remote: str, branch: str) -> None:
    run_func(["git", "fetch", remote, branch], check=True)


def _push_force_with_lease(run_func: Any, remote: str, branch: str) -> None:
    run_func(["git", "push", remote, branch, "--force-with-lease"], check=True)


def push_retry_with_force_lease(run_func: Any, remote: str, branch: str) -> None:
    """Fetch latest remote branch, then retry push with force-with-lease once."""
    _fetch_branch(run_func, remote, branch)
    _push_force_with_lease(run_func, remote, branch)


def _build_push_result(
    ok: bool,
    status: str,
    ctx: PushResultContext,
) -> dict[str, Any]:
    return PushRetryResult(ok=ok, status=status, ctx=ctx).to_dict()


def _post_fix_push_gate_raw(context: dict[str, Any] | None = None) -> dict[str, Any]:
    gate_func = getattr(controller, "can_push_after_post_fix_audit", None)
    raw = gate_func(context) if callable(gate_func) else controller.evaluate_post_fix_audit_gate(context)
    return raw if isinstance(raw, dict) else {}


def _post_fix_push_gate_allowed(gate: dict[str, Any]) -> bool:
    return bool(gate.get("allowed")) and bool(gate.get("can_push"))


def _post_fix_push_gate_can_push(gate: dict[str, Any], allowed: bool) -> bool:
    return bool(gate.get("can_push")) if gate else allowed


def _post_fix_push_gate_reason(gate: dict[str, Any], allowed: bool) -> str:
    default_reason = "allowed" if allowed else "post_fix_audit_gate_denied"
    return str(gate.get("reason") or default_reason)


def _post_fix_push_gate_next_action(gate: dict[str, Any], allowed: bool) -> str:
    default_action = "proceed" if allowed else "needs_manual"
    return str(gate.get("next_action") or default_action)


def _post_fix_push_gate_needs_manual(gate: dict[str, Any], allowed: bool) -> bool:
    return bool(gate.get("needs_manual")) or not allowed


def _post_fix_push_gate_response(gate: dict[str, Any], allowed: bool) -> dict[str, Any]:
    return {
        "allowed": allowed,
        "can_push": _post_fix_push_gate_can_push(gate, allowed),
        "reason": _post_fix_push_gate_reason(gate, allowed),
        "next_action": _post_fix_push_gate_next_action(gate, allowed),
        "needs_manual": _post_fix_push_gate_needs_manual(gate, allowed),
    }


def ensure_post_fix_audit_gate_before_push(context: dict[str, Any] | None = None) -> dict[str, Any]:
    """Evaluate PR3H gate before any push path; fail closed on malformed payloads."""
    gate = _post_fix_push_gate_raw(context)
    return _post_fix_push_gate_response(gate, _post_fix_push_gate_allowed(gate))


def _failed_push_result(repo: str, branch: str, exc: RuntimeError) -> dict[str, Any]:
    return _build_push_result(
        False,
        "failed",
        PushResultContext(repo=repo, branch=branch, retried=False, needs_manual=False, error=str(exc)),
    )


def _needs_manual_push_result(
    repo: str,
    branch: str,
    initial_exc: RuntimeError,
    retry_exc: RuntimeError,
) -> dict[str, Any]:
    return _build_push_result(
        False,
        "needs_manual",
        PushResultContext(
            repo=repo,
            branch=branch,
            retried=True,
            needs_manual=True,
            error=str(retry_exc),
            initial_error=str(initial_exc),
        ),
    )


def _retry_non_fast_forward_push(
    run_func: Any,
    ctx: NonFastForwardRetryContext,
    remote: str,
    gate_context: dict[str, Any] | None,
) -> dict[str, Any]:
    retry_gate_context = build_retry_push_gate_context(gate_context)
    gate = ensure_post_fix_audit_gate_before_push(retry_gate_context)
    if not gate["allowed"]:
        return _build_push_result(
            False,
            "needs_manual",
            PushResultContext(
                repo=ctx.repo,
                branch=ctx.branch,
                retried=True,
                needs_manual=True,
                error=f"post-fix audit gate denied before retry push: {gate['reason']}",
                initial_error=str(ctx.initial_exc),
            ),
        )
    try:
        push_retry_with_force_lease(run_func, remote, ctx.branch)
        return _build_push_result(
            True, "success", PushResultContext(repo=ctx.repo, branch=ctx.branch, retried=True, needs_manual=False)
        )
    except RuntimeError as retry_exc:
        return _needs_manual_push_result(ctx.repo, ctx.branch, ctx.initial_exc, retry_exc)


def _retry_gate_context_payload(context: dict[str, Any]) -> dict[str, Any]:
    payload = context.get("retry_push_gate_context")
    return payload if isinstance(payload, dict) else {}


def build_retry_push_gate_context(context: dict[str, Any] | None = None) -> dict[str, Any]:
    """Require explicit refreshed gate evidence before non-fast-forward retry push."""
    if not isinstance(context, dict):
        return {"post_fix_audit": "FAIL", "retry_push_gate_context": "missing"}
    retry_payload = _retry_gate_context_payload(context)
    refreshed = retry_payload.get("explicitly_refreshed")
    if refreshed is not True:
        return {"post_fix_audit": "FAIL", "retry_push_gate_context": "stale_or_missing"}
    retry_gate = retry_payload.get("gate_context")
    if not isinstance(retry_gate, dict):
        return {"post_fix_audit": "FAIL", "retry_push_gate_context": "malformed"}
    return retry_gate




def push_with_retry_once(
    run_func: Any,
    repo: str,
    branch: str,
    *,
    remote: str = "origin",
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Push once, recover once on non-fast-forward, then stop with explicit status."""
    gate = ensure_post_fix_audit_gate_before_push(context)
    if not gate["allowed"]:
        return _build_push_result(
            False,
            "needs_manual",
            PushResultContext(
                repo=repo,
                branch=branch,
                retried=False,
                needs_manual=True,
                error=f"post-fix audit gate denied before push: {gate['reason']}",
            ),
        )
    try:
        _push_initial(run_func, remote, branch)
        return _build_push_result(
            True, "success", PushResultContext(repo=repo, branch=branch, retried=False, needs_manual=False)
        )
    except RuntimeError as exc:
        if not is_non_fast_forward_push_error(exc):
            return _failed_push_result(repo, branch, exc)
        retry_ctx = NonFastForwardRetryContext(repo=repo, branch=branch, initial_exc=exc)
        return _retry_non_fast_forward_push(run_func, retry_ctx, remote, context)






def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("readiness")
    p.add_argument("--repo", required=True)
    p.add_argument("--pr", required=True)
    p.add_argument("--ignore-safe-autofix", action="store_true")
    p.add_argument("--wait-unknown-seconds", type=int, default=0)
    p.add_argument("--wait-pending-seconds", type=int, default=0)
    p.add_argument("--poll-seconds", type=int, default=10)
    p.add_argument("--output", default="")
    p.add_argument("--no-fail", action="store_true")
    p.set_defaults(func=cmd_readiness)

    p = sub.add_parser("cleanup")
    p.add_argument("--repo", required=True)
    p.add_argument("--pr", required=True)
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--keep-per-workflow", type=int, default=1)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--output", default="")
    p.set_defaults(func=cmd_cleanup)

    p = sub.add_parser("preflight")
    p.add_argument("--repo", required=True)
    p.add_argument("--pr", required=True)
    p.add_argument("--max-safe-autofix-commits", type=int, default=3)
    p.add_argument("--oscillation-touch-limit", type=int, default=3)
    p.add_argument("--output", default="")
    p.add_argument("--comment", action="store_true")
    p.add_argument("--no-fail", action="store_true")
    p.set_defaults(func=cmd_preflight)

    p = sub.add_parser("report")
    p.add_argument("--repo", required=True)
    p.add_argument("--pr", required=True)
    p.add_argument("--outdir", default=".pr-flow")
    p.add_argument("--comment", action="store_true")
    p.add_argument("--no-fail", action="store_true")
    p.set_defaults(func=cmd_report)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
