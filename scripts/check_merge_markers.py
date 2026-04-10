from __future__ import annotations

import sys
from pathlib import Path


EXCLUDED_DIRS = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    "node_modules",
    ".idea",
    ".vscode",
}

EXCLUDED_SUFFIXES = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".pdf",
    ".ico",
    ".lock",
    ".sqlite",
    ".db",
    ".pyc",
    ".pyo",
}

MARKERS = ("<<<<<<<", "=======", ">>>>>>>")


def is_excluded(path: Path) -> bool:
    if any(part in EXCLUDED_DIRS for part in path.parts):
        return True
    if path.suffix.lower() in EXCLUDED_SUFFIXES:
        return True
    return False


def main() -> int:
    root = Path(".").resolve()
    found = []

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if is_excluded(path):
            continue

        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue

        lines = text.splitlines()

        for lineno, line in enumerate(lines, start=1):
            stripped = line.strip()

            # 🔥 FIX: match SOLO se la riga INIZIA con il marker
            if any(stripped.startswith(marker) for marker in MARKERS):
                found.append((path.relative_to(root), lineno, stripped))

    if found:
        print("Merge conflict markers found:", file=sys.stderr)
        for rel_path, lineno, line in found:
            print(f" - {rel_path}:{lineno}: {line}", file=sys.stderr)
        return 1

    print("No merge conflict markers found")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())