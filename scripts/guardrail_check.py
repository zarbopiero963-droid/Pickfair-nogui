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


def normalize_changed_files(raw: list[dict]) -> list[str]:
    changed_files: list[str] = []
    for item in raw:
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


def touches_critical_files(changed_files: list[str]) -> list[str]:
    return [path for path in changed_files if path in CRITICAL_FILES]


def main() -> int:
    pr_meta = load_json("pr_meta.json")
    pr_files_raw = load_json("pr_files_raw.json")

    if not isinstance(pr_meta, dict):
        fail("pr_meta.json must contain a JSON object")
    if not isinstance(pr_files_raw, list):
        fail("pr_files_raw.json must contain a JSON array")

    title = str(pr_meta.get("title", "") or "")
    body = str(pr_meta.get("body", "") or "")
    text = f"{title}\n{body}"

    changed_files = normalize_changed_files(pr_files_raw)
    critical_touched = touches_critical_files(changed_files)

    task = extract_task(text)
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
    if not task:
        fail("Missing [TASK: ...] tag in PR title/body. TASK validation is fail-closed.")
    if task not in allowed_tasks:
        fail(f"Unknown TASK tag: {task}. Must be one of configured task keys.")
    info(f"TASK tag found: {task}")

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
