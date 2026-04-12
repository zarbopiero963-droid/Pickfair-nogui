from __future__ import annotations

from pathlib import Path
import os
import subprocess
import sys

TASK_PATH = Path(os.environ["TASK_PATH"])
HEADER_PATH = Path("ops/templates/task_header.md")


def main() -> int:
    if not TASK_PATH.exists():
        print(f"Task file not found: {TASK_PATH}", file=sys.stderr)
        return 1
    if not HEADER_PATH.exists():
        print(f"Header file not found: {HEADER_PATH}", file=sys.stderr)
        return 1

    header = HEADER_PATH.read_text(encoding="utf-8").strip()
    task_body = TASK_PATH.read_text(encoding="utf-8").strip()

    title = f"Next Codex task ready: {TASK_PATH.name}"
    body = f"""@codex

{header}

Execute exactly this next task and no later task.

Task-File: {TASK_PATH}

{task_body}
"""

    subprocess.run(
        ["gh", "issue", "create", "--title", title, "--body", body],
        check=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())