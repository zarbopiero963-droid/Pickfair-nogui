from __future__ import annotations

from pathlib import Path
import os
import re
import shutil
import sys

DONE_DIR = Path("ops/tasks_done")


def write_task_moved(value: bool) -> None:
    """Write the task_moved flag to GitHub step outputs when available."""
    github_output_path = os.environ.get("GITHUB_OUTPUT")
    if not github_output_path:
        return

    with Path(github_output_path).open("a", encoding="utf-8") as output_file:
        output_file.write(f"task_moved={'true' if value else 'false'}\n")


def main() -> int:
    pr_body = os.environ.get("PR_BODY", "")
    match = re.search(r"Task-File:\s*(ops/tasks/[^\s]+\.md)", pr_body)
    if not match:
        print("Skipping task completion: no Task-File marker found in PR body")
        write_task_moved(False)
        return 0

    src = Path(match.group(1))
    if not src.exists():
        print(f"Task file does not exist: {src}", file=sys.stderr)
        write_task_moved(False)
        return 1

    DONE_DIR.mkdir(parents=True, exist_ok=True)
    dst = DONE_DIR / src.name
    shutil.move(str(src), str(dst))

    print(f"Moved {src} -> {dst}")
    write_task_moved(True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
