from __future__ import annotations

import json
import re
import sys
from pathlib import Path


CRITICAL_FILES = {
    "core/trading_engine.py",
    "order_manager.py",
    "core/reconciliation_engine.py",
    "core/state_recovery.py",
    "database.py",
    "core/execution_guard.py",
    "core/risk_middleware.py",
    "core/runtime_controller.py",
    "core/money_management.py",
    "dutching.py",
    "pnl_engine.py",
    "telegram_listener.py",
    "copy_engine.py",
    "simulation_broker.py",
    "session_manager.py",
    "rate_limiter.py",
    "live_gate.py",
}

TASK_PATTERN = re.compile(r"\[TASK:\s*([^\]]+)\]", re.IGNORECASE)
TASK_KEY_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
PLACEHOLDER_TASKS = {"todo"}
EXTRA_ALLOWED_TASKS = {
    "observability_phase1_cto_telegram",
    "observability_phase1_review_fixes",
    "ci_pr_guard_task_source_hardening",
    "ci_pr_guard_unknown_task_fix",
}


def load_json(path: str) -> dict | list:
    p = Path(path)
    if not p.exists():
        fail(f"Missing required file: {path}")
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        fail(f"Invalid JSON in {path}: {exc}")
    except Exception as exc:
        fail(f"Unable to read {path}: {exc}")


def fail(message: str) -> None:
    print(f"❌ {message}")
    raise SystemExit(1)


def warn(message: str) -> None:
    print(f"⚠️ {message}")


def info(message: str) -> None:
    print(f"ℹ️ {message}")


def normalize_changed_files(raw: list[dict] | list[str]) -> list[str]:
    changed_files: list[str] = []
    for item in raw:
        if isinstance(item, str):
            filename = item.strip()
            if filename:
                changed_files.append(filename)
            continue
        if isinstance(item, dict) and "filename" in item:
            filename = str(item["filename"]).strip()
            if filename:
                changed_files.append(filename)
    return changed_files


def extract_tasks(text: str) -> list[str]:
    out: list[str] = []
    for match in TASK_PATTERN.finditer(text or ""):
        task = (match.group(1) or "").strip()
        if task:
            out.append(task)
    return out


def _is_placeholder_or_invalid(task: str) -> bool:
    norm = (task or "").strip().lower()
    if not norm:
        return True
    if norm in PLACEHOLDER_TASKS:
        return True
    if all(ch == "." for ch in norm):
        return True
    return TASK_KEY_PATTERN.fullmatch(norm) is None


def resolve_task(pr_meta: dict, changed_files: list[str], allowed_tasks: set[str]) -> tuple[str | None, str | None, list[tuple[str, str]], list[tuple[str, str]]]:
    sources = [
        ("pr_title", str(pr_meta.get("title", "") or "")),
        ("pr_body", str(pr_meta.get("body", "") or "")),
        ("branch", str(pr_meta.get("branch", "") or pr_meta.get("head_ref", "") or "")),
        ("latest_commit_message", str(pr_meta.get("latest_commit_message", "") or "")),
    ]
    unknown_candidates: list[tuple[str, str]] = []
    ignored_candidates: list[tuple[str, str]] = []
    for source_name, text in sources:
        for task in extract_tasks(text):
            if _is_placeholder_or_invalid(task):
                ignored_candidates.append((source_name, task))
                continue
            if task in allowed_tasks:
                return task, source_name, unknown_candidates, ignored_candidates
            unknown_candidates.append((source_name, task))
    commit_messages = pr_meta.get("commit_messages")
    if isinstance(commit_messages, list):
        for msg in commit_messages:
            for task in extract_tasks(str(msg or "")):
                if _is_placeholder_or_invalid(task):
                    ignored_candidates.append(("commit_messages", task))
                    continue
                if task in allowed_tasks:
                    return task, "commit_messages", unknown_candidates, ignored_candidates
                unknown_candidates.append(("commit_messages", task))

    task_path_hits = [
        path for path in changed_files
        if path.startswith("ops/tasks/") or path.startswith("ops/tasks_done/")
    ]
    if task_path_hits:
        return "task_file_change", "changed_task_files", unknown_candidates, ignored_candidates
    return None, None, unknown_candidates, ignored_candidates


def touches_critical_files(changed_files: list[str]) -> list[str]:
    return [path for path in changed_files if path in CRITICAL_FILES]


def validate_task_selection(task: str | None, critical_touched: list[str], allowed_tasks: set[str], unknown_candidates: list[tuple[str, str]]) -> None:
    if not task:
        if unknown_candidates:
            rendered = ", ".join(f"{source}:{value}" for source, value in unknown_candidates)
            fail(f"Unknown TASK tag candidates: {rendered}. Must be one of configured task keys.")
        fail("Missing TASK marker/source across title/body/branch/commit/task files. TASK validation is fail-closed.")
    if task == "task_file_change" and critical_touched:
        fail("PRs inferred from task-file changes must not also touch critical files.")
    if task != "task_file_change" and task not in allowed_tasks:
        fail(f"Unknown TASK tag: {task}. Must be one of configured task keys.")


def main() -> int:
    pr_meta = load_json("pr_meta.json")
    pr_files_raw = load_json("pr_files_raw.json")

    if not isinstance(pr_meta, dict):
        fail("pr_meta.json must contain a JSON object")
    if not isinstance(pr_files_raw, list):
        fail("pr_files_raw.json must contain a JSON array")

    changed_files = normalize_changed_files(pr_files_raw)
    critical_touched = touches_critical_files(changed_files)

    allowed_scope = load_json(".guardrails/allowed_scope.json")
    allowed_tasks = set()
    if isinstance(allowed_scope, dict):
        tasks = allowed_scope.get("tasks")
        if isinstance(tasks, dict):
            allowed_tasks = {str(k) for k in tasks.keys()}
    allowed_tasks |= EXTRA_ALLOWED_TASKS
    task, task_source, unknown_candidates, ignored_candidates = resolve_task(pr_meta, changed_files, allowed_tasks)

    print("=" * 80)
    print("PR GUARD REPORT")
    print("=" * 80)
    info(f"Changed files: {len(changed_files)}")
    if changed_files:
        for path in changed_files:
            print(f" - {path}")

    print()
    if critical_touched:
        info("Critical files touched:")
        for path in critical_touched:
            print(f" - {path}")
    else:
        info("No critical files touched")

    print()
    validate_task_selection(task, critical_touched, allowed_tasks, unknown_candidates)
    info(f"TASK source found ({task_source}): {task}")
    if ignored_candidates:
        warn(f"Ignored placeholder/invalid TASK markers: {ignored_candidates}")

    # Optional hygiene warnings
    if len(changed_files) > 25:
        warn(
            f"PR changes {len(changed_files)} files. "
            "Consider splitting if this was intended to be a focused PR."
        )

    if task and len(task) < 3:
        warn("TASK tag looks unusually short; verify it is meaningful.")

    print()
    print("✅ PR guard completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
