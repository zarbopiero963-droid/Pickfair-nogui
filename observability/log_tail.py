from __future__ import annotations

from pathlib import Path
from typing import Iterable, List


def tail_text_from_files(paths: Iterable[str], max_bytes_per_file: int = 200_000) -> str:
    chunks: List[str] = []

    for raw_path in paths:
        try:
            path = Path(raw_path)
            if not path.exists() or not path.is_file():
                continue

            with path.open("rb") as f:
                f.seek(0, 2)
                size = f.tell()
                start = max(0, size - int(max_bytes_per_file))
                f.seek(start)
                content = f.read().decode("utf-8", errors="replace")

            chunks.append(f"\n===== LOG FILE: {path} =====\n")
            chunks.append(content)
            chunks.append("\n")
        except Exception as exc:
            chunks.append(f"\n===== LOG FILE ERROR: {raw_path} =====\n{exc}\n")

    return "".join(chunks)
