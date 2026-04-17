from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

REQUIRED_CHECKS = (
    "db_restore_procedure_documented",
    "db_restore_validation_verified",
)


class RollbackGateError(RuntimeError):
    pass


def _load_marked_checks(checklist_path: Path) -> set[str]:
    if not checklist_path.exists():
        raise RollbackGateError(f"missing checklist file: {checklist_path}")

    marked: set[str] = set()
    for raw in checklist_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("- [x] "):
            marked.add(line.replace("- [x] ", "", 1).strip())
    return marked


def evaluate(checklist_path: str = "ops/rollback_checklist.md") -> dict[str, Any]:
    passed_checks: list[str] = []
    failed_checks: list[str] = []
    missing_checks: list[str] = []
    reasons: list[str] = []

    checklist = Path(checklist_path)

    try:
        marked = _load_marked_checks(checklist)
    except RollbackGateError as exc:
        failed_checks.append("rollback_checklist_present")
        reasons.append(str(exc))
        return {
            "status": "FAIL",
            "gate": "rollback",
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "missing_checks": missing_checks,
            "reasons": reasons,
        }

    for check in REQUIRED_CHECKS:
        if check not in marked:
            missing_checks.append(check)

    if Path("ops/db_backup_restore.md").exists():
        passed_checks.append("db_restore_procedure_documented")
    else:
        if "db_restore_procedure_documented" not in missing_checks:
            missing_checks.append("db_restore_procedure_documented")
        failed_checks.append("db_restore_procedure_documented")
        reasons.append("missing ops/db_backup_restore.md")

    if Path("scripts/db_restore_validate.py").exists():
        passed_checks.append("db_restore_validation_verified")
    else:
        if "db_restore_validation_verified" not in missing_checks:
            missing_checks.append("db_restore_validation_verified")
        failed_checks.append("db_restore_validation_verified")
        reasons.append("missing scripts/db_restore_validate.py")

    for check in missing_checks:
        if check not in failed_checks:
            failed_checks.append(check)
    if missing_checks:
        reasons.append("required rollback evidence not checked in checklist")

    status = "PASS" if not failed_checks else "FAIL"
    return {
        "status": status,
        "gate": "rollback",
        "passed_checks": sorted(set(passed_checks)),
        "failed_checks": sorted(set(failed_checks)),
        "missing_checks": sorted(set(missing_checks)),
        "reasons": reasons,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fail-closed rollback gate")
    parser.add_argument("--checklist-path", default="ops/rollback_checklist.md")
    parser.add_argument("--report-path", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = evaluate(checklist_path=args.checklist_path)

    if args.report_path:
        report_path = Path(args.report_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
