from __future__ import annotations

import json
import sys
from pathlib import Path


REQUIRED_TOP_LEVEL_DIRS = [
    "specs",
    "contracts",
    "state_models",
    "mutations",
]


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def main() -> int:
    root = Path("guardrails")

    if not root.exists():
        fail("guardrails directory does not exist")

    for dirname in REQUIRED_TOP_LEVEL_DIRS:
        path = root / dirname
        if not path.exists():
            fail(f"missing directory: {path}")
        if not path.is_dir():
            fail(f"not a directory: {path}")

    total = 0
    for json_path in sorted(root.rglob("*.json")):
        try:
            with json_path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
        except json.JSONDecodeError as exc:
            fail(f"invalid JSON in {json_path}: {exc}")
        except Exception as exc:
            fail(f"failed reading {json_path}: {exc}")

        if not isinstance(payload, dict):
            fail(f"top-level JSON must be object in {json_path}")

        total += 1
        print(f"OK JSON: {json_path}")

    if total == 0:
        fail("no guardrail JSON files found")

    print(f"Validated guardrail JSON files: {total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())