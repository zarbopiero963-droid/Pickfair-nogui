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


def is_conflict_marker(stripped: str) -> bool:
    # Un vero marker di conflitto git è ESATTAMENTE 7 caratteri (`=======`)
    # oppure 7 caratteri seguiti da spazio + etichetta (`<<<<<<< branch`,
    # `>>>>>>> branch`). Le righe decorative più lunghe (es. `====...` da 52
    # char in un docstring) NON sono marker: matchare `startswith(marker)` dava
    # falsi positivi (betfair_market_api.py:3). Confronto esatto o `marker + " "`.
    return any(
        stripped == marker or stripped.startswith(marker + " ")
        for marker in MARKERS
    )


def scan_for_markers(root: Path) -> list[tuple[Path, int, str]]:
    """Restituisce i marker di conflitto trovati sotto `root` (ricorsivo)."""
    found: list[tuple[Path, int, str]] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if is_excluded(path):
            continue

        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue

        for lineno, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if is_conflict_marker(stripped):
                found.append((path.relative_to(root), lineno, stripped))
    return found


def main(argv: list[str] | None = None) -> int:
    # Root parametrizzabile (primo argomento posizionale) per testabilità;
    # default = cwd (uso CI corrente `python3 scripts/check_merge_markers.py`).
    args = sys.argv[1:] if argv is None else list(argv)
    root = Path(args[0]).resolve() if args else Path(".").resolve()
    found = scan_for_markers(root)

    if found:
        print("Merge conflict markers found:", file=sys.stderr)
        for rel_path, lineno, line in found:
            print(f" - {rel_path}:{lineno}: {line}", file=sys.stderr)
        return 1

    print("No merge conflict markers found")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())