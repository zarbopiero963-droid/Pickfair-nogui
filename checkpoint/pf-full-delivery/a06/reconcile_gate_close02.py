#!/usr/bin/env python3
"""Reconcile the 410-cell Roserpina crosswalk with immutable Gate-A evidence."""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
from pathlib import Path

FIELDS = (
    "requirement_id", "id", "status", "verdict", "classification", "result",
    "code_sha", "tested_sha", "tree_sha", "commit", "test", "tests", "evidence",
    "reason", "note", "source", "artifact",
)


def records(value, path: Path, out: dict[str, list[dict]]) -> None:
    if isinstance(value, dict):
        rid = next((value.get(k) for k in ("requirement_id", "id")
                    if isinstance(value.get(k), str) and value[k].startswith("PF-RSR-")), None)
        if rid:
            summary = {k: value[k] for k in FIELDS if k in value}
            summary["source_path"] = str(path)
            if summary not in out[rid]:
                out[rid].append(summary)
        for key, child in value.items():
            if isinstance(key, str) and key.startswith("PF-RSR-"):
                keyed = {"requirement_id": key, "source_path": str(path), "keyed_record": child}
                if keyed not in out[key]:
                    out[key].append(keyed)
            records(child, path, out)
    elif isinstance(value, list):
        for child in value:
            records(child, path, out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--crosswalk", type=Path, required=True)
    ap.add_argument("--gate", type=Path, required=True)
    ap.add_argument("--gate-sha", required=True)
    ap.add_argument("--code-sha", required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    baseline = json.loads(args.crosswalk.read_text())
    index: dict[str, list[dict]] = collections.defaultdict(list)
    parsed = []
    for path in sorted((args.gate / "checkpoint/close02/artifacts").rglob("*.json")):
        try:
            value = json.loads(path.read_text())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        before = sum(map(len, index.values()))
        records(value, path.relative_to(args.gate), index)
        if sum(map(len, index.values())) > before:
            parsed.append(str(path.relative_to(args.gate)))

    cells = []
    for cell in baseline["cells"]:
        competing = index.get(cell["id"], [])
        cells.append({
            **cell,
            "reconciled_status": cell["status"],
            "reconciliation": (
                "Preserved: competing evidence is from a different code tree or lacks an "
                "execution record tied to the tested code SHA; it cannot upgrade this cell."
                if competing else
                "Preserved: no competing record was recovered from the Gate-A branch."
            ),
            "gate_close02_evidence": competing,
        })

    counts = dict(collections.Counter(c["reconciled_status"] for c in cells))
    result = {
        "schema": "pf-a06-reconciliation/1",
        "source_crosswalk_sha256": hashlib.sha256(args.crosswalk.read_bytes()).hexdigest(),
        "source_code_sha": baseline["code_sha"],
        "current_code_sha": args.code_sha,
        "gate_branch": "checkpoint/gate-a-close02",
        "gate_branch_sha": args.gate_sha,
        "method": "Per-cell provenance index; no PASS upgrade across different or unproven code SHA.",
        "scope": "Evidence contribution to A06 only; this is not a Gate A PASS verdict.",
        "total": len(cells),
        "counts": counts,
        "gate_json_files_with_cell_records": len(parsed),
        "cells_with_competing_records": sum(bool(c["gate_close02_evidence"]) for c in cells),
        "source_files": parsed,
        "cells": cells,
    }
    args.out.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({k: result[k] for k in (
        "total", "counts", "gate_json_files_with_cell_records", "cells_with_competing_records"
    )}, sort_keys=True))
    return 0 if len(cells) == 410 and sum(counts.values()) == 410 else 2


if __name__ == "__main__":
    raise SystemExit(main())
