from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

DEFAULT_EVIDENCE_PATHS = (
    "ops/readiness_checklist.md",
    "ops/rollback_checklist.md",
    "ops/paper_trading_gate.md",
    "ops/live_microstake_gate.md",
    "ops/incident_playbook.md",
    "ops/observability_minimum.md",
)


class IncidentSnapshotError(RuntimeError):
    pass


def _sha256_for_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def collect_snapshot(evidence_paths: list[str] | None = None) -> dict[str, Any]:
    selected_paths = evidence_paths if evidence_paths is not None else list(DEFAULT_EVIDENCE_PATHS)

    existing_files: list[dict[str, Any]] = []
    missing_files: list[str] = []

    for raw_path in sorted(set(selected_paths)):
        path = Path(raw_path)
        if path.exists() and path.is_file():
            stat = path.stat()
            existing_files.append(
                {
                    "path": str(path),
                    "size_bytes": int(stat.st_size),
                    "sha256": _sha256_for_file(path),
                }
            )
        else:
            missing_files.append(str(path))

    status = "PASS" if not missing_files else "FAIL"
    reasons: list[str] = []
    if missing_files:
        reasons.append("one or more requested evidence files are missing")

    return {
        "status": status,
        "snapshot": "incident",
        "existing_files": existing_files,
        "missing_files": missing_files,
        "requested_files": sorted(set(selected_paths)),
        "reasons": reasons,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect local incident evidence snapshot")
    parser.add_argument(
        "--evidence-path",
        action="append",
        dest="evidence_paths",
        help="Optional evidence path (repeat flag for multiple paths)",
    )
    parser.add_argument("--report-path", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = collect_snapshot(evidence_paths=args.evidence_paths)

    if args.report_path:
        target = Path(args.report_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
