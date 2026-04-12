from __future__ import annotations

from pathlib import Path
import os
import re
import shutil
import sys

PR_BODY = os.environ.get("PR_BODY", "")
DONE_DIR = Path("ops/tasks_done")


def main() -> int:
    match = re.search(r"Task-File:\s*(ops/tasks/[^\s]+\.md)", PR_BODY)
    if not match:
        print("No Task-File marker found in PR body", file=sys.stderr)
        return 1

    src = Path(match.group(1))
    if not src.exists():
        print(f"Task file does not exist: {src}", file=sys.stderr)
        return 1

    DONE_DIR.mkdir(parents=True, exist_ok=True)
    dst = DONE_DIR / src.name
    shutil.move(str(src), str(dst))

    print(f"Moved {src} -> {dst}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())