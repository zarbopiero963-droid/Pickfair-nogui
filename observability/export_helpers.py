from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any, Iterable, Mapping


class ExportHelpers:
    def __init__(self, export_dir: str = "diagnostics_exports") -> None:
        self.export_dir = Path(export_dir)
        self.export_dir.mkdir(parents=True, exist_ok=True)

    def export_json(self, name_prefix: str, payload: Any) -> str:
        ts = time.strftime("%Y%m%d_%H%M%S")
        path = self.export_dir / f"{name_prefix}_{ts}.json"
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        return str(path)

    def export_csv(self, name_prefix: str, rows: Iterable[Mapping[str, Any]]) -> str:
        ts = time.strftime("%Y%m%d_%H%M%S")
        path = self.export_dir / f"{name_prefix}_{ts}.csv"

        rows = list(rows)
        fieldnames = sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]

        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            if rows:
                for row in rows:
                    writer.writerow(dict(row))
            else:
                writer.writerow({"empty": ""})

        return str(path)
