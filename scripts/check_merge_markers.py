"""Guardrail CI: verifica che il repository non contenga marker di conflitto git.

Distingue i veri marker di conflitto (7 caratteri esatti come `=======`, o 7
caratteri seguiti da spazio + etichetta come `<<<<<<< branch`) dalle righe
decorative più lunghe (es. `====...` in un docstring), che NON sono marker.
Supporta i marker della modalità `diff3`/`zdiff3` (`|||||||`).
"""

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

# Marker di conflitto git: apertura, base (diff3/zdiff3), separatore, chiusura.
MARKERS = ("<<<<<<<", "|||||||", "=======", ">>>>>>>")


def is_excluded(path: Path) -> bool:
    if any(part in EXCLUDED_DIRS for part in path.parts):
        return True
    if path.suffix.lower() in EXCLUDED_SUFFIXES:
        return True
    return False


def is_conflict_marker(stripped: str) -> bool:
    """True se la riga (già `strip()`-ata) è un vero marker di conflitto git.

    Un marker git è ESATTAMENTE 7 caratteri (`=======`, `|||||||`) oppure 7
    caratteri seguiti da spazio + etichetta (`<<<<<<< branch`, `>>>>>>> branch`).
    Le righe decorative più lunghe (es. `====...` da 52 char in un docstring) NON
    sono marker: usare `startswith(marker)` darebbe falsi positivi.
    """
    return any(
        stripped == marker or stripped.startswith(marker + " ")
        for marker in MARKERS
    )


def _get_markers_in_file(path: Path, root: Path) -> list[tuple[Path, int, str]]:
    """Marker di conflitto in un singolo file (path relativo a `root`)."""
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return []
    markers: list[tuple[Path, int, str]] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if is_conflict_marker(stripped):
            markers.append((path.relative_to(root), lineno, stripped))
    return markers


def scan_for_markers(root: Path) -> list[tuple[Path, int, str]]:
    """Restituisce i marker di conflitto trovati sotto `root` (ricorsivo)."""
    found: list[tuple[Path, int, str]] = []
    for path in root.rglob("*"):
        if path.is_file() and not is_excluded(path):
            found.extend(_get_markers_in_file(path, root))
    return found


def main(argv: list[str] | None = None) -> int:
    """Scansiona la root (arg posizionale o cwd) e ritorna 1 se trova marker.

    La root è parametrizzabile (primo argomento posizionale) per testabilità;
    senza argomento ricade su cwd (uso CI `python3 scripts/check_merge_markers.py`).
    """
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
