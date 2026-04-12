from __future__ import annotations

from pathlib import Path
import json

TASKS_DIR = Path("ops/tasks")


def main() -> int:
    tasks = sorted(
        p for p in TASKS_DIR.iterdir()
        if p.is_file() and p.suffix == ".md" and p.name != ".gitkeep"
    )

    if not tasks:
        print(json.dumps({"found": False}))
        return 0

    task = tasks[0]
    print(json.dumps({
        "found": True,
        "path": str(task),
        "name": task.name,
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())