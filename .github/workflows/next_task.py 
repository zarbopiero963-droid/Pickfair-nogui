from __future__ import annotations

from pathlib import Path
import json
import sys

TASKS_DIR = Path("ops/tasks")


def main() -> int:
    if not TASKS_DIR.exists():
        print(json.dumps({"found": False, "reason": "ops/tasks missing"}))
        return 0

    tasks = sorted(
        p for p in TASKS_DIR.iterdir()
        if p.is_file() and p.suffix == ".md"
    )

    if not tasks:
        print(json.dumps({"found": False, "reason": "no pending tasks"}))
        return 0

    next_task = tasks[0]
    print(json.dumps({
        "found": True,
        "path": str(next_task),
        "name": next_task.name,
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())