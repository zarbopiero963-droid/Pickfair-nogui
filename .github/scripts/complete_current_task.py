from __future__ import annotations

from pathlib import Path
import os
import shutil
import sys

TASK_PATH = os.environ.get("TASK_PATH", "").strip()
DONE_DIR = Path("ops/tasks_done")


def main() -> int:
    if not TASK_PATH:
        print("TASK_PATH not set", file=sys.stderr)
        return 1

    src = Path(TASK_PATH)
    if not src.exists():
        print(f"Task file not found: {src}", file=sys.stderr)
        return 1

    DONE_DIR.mkdir(parents=True, exist_ok=True)
    dst = DONE_DIR / src.name
    shutil.move(str(src), str(dst))
    print(f"Moved {src} -> {dst}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())