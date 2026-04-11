from __future__ import annotations

from pathlib import Path
import os
import sys

HEADER_PATH = Path("ops/templates/task_header.md")
TASK_PATH = Path(os.environ["TASK_PATH"])


def main() -> int:
    if not HEADER_PATH.exists():
        print("Missing header template", file=sys.stderr)
        return 1
    if not TASK_PATH.exists():
        print(f"Missing task file: {TASK_PATH}", file=sys.stderr)
        return 1

    header = HEADER_PATH.read_text(encoding="utf-8").strip()
    task_body = TASK_PATH.read_text(encoding="utf-8").strip()

    comment = f"""@codex

{header}

Execute exactly this next task:

{task_body}
"""
    print(comment)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())