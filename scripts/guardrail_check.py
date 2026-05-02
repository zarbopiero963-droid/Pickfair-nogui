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


def extract_task(text: str) -> str | None:
    match = TASK_PATTERN.search(text)
    if not match:
        return None
    task = match.group(1).strip()
    return task or None


def resolve_task(pr_meta: dict, changed_files: list[str]) -> tuple[str | None, str | None]:
    sources = [
        ("pr_title", str(pr_meta.get("title", "") or "")),
        ("pr_body", str(pr_meta.get("body", "") or "")),
        ("branch", str(pr_meta.get("branch", "") or pr_meta.get("head_ref", "") or "")),
        ("latest_commit_message", str(pr_meta.get("latest_commit_message", "") or "")),
    ]
    for source_name, text in sources:
        task = extract_task(text)
        if task:
            return task, source_name
    commit_messages = pr_meta.get("commit_messages")
    if isinstance(commit_messages, list):
        for msg in commit_messages:
            task = extract_task(str(msg or ""))
            if task:
                return task, "commit_messages"

    task_path_hits = [
        path for path in changed_files
        if path.startswith("ops/tasks/") or path.startswith("ops/tasks_done/")
    ]
    if task_path_hits:
        return "task_file_change", "changed_task_files"
    return None, None


def touches_critical_files(changed_files: list[str]) -> list[str]:
    return [path for path in changed_files if path in CRITICAL_FILES]


def validate_task_selection(task: str | None, critical_touched: list[str], allowed_tasks: set[str]) -> None:
    if not task:
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

    task, task_source = resolve_task(pr_meta, changed_files)
    allowed_scope = load_json(".guardrails/allowed_scope.json")
    allowed_tasks = set()
    if isinstance(allowed_scope, dict):
        tasks = allowed_scope.get("tasks")
        if isinstance(tasks, dict):
            allowed_tasks = {str(k) for k in tasks.keys()}

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
    validate_task_selection(task, critical_touched, allowed_tasks)
    info(f"TASK source found ({task_source}): {task}")

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
