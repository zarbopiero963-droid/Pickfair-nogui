from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

PAPER_REQUIRED_CHECKS = (
    "readiness_passed",
    "observability_minimum_passed",
    "db_backup_restore_discipline_present",
    "no_open_incident",
    "operator_acknowledged",
)

LIVE_MICRO_REQUIRED_CHECKS = (
    "readiness_passed",
    "rollback_passed",
    "observability_minimum_passed",
    "db_backup_restore_discipline_present",
    "hard_stop_limits_present",
    "strict_live_key_source_enabled_or_equivalent_explicit_confirmation",
    "no_open_incident",
    "operator_acknowledged",
    "paper_results_reviewed",
    "max_stake_approved",
    "kill_switch_confirmed",
)


class LiveGateError(RuntimeError):
    pass


def _load_marked_checks(checklist_path: Path) -> set[str]:
    if not checklist_path.exists():
        raise LiveGateError(f"missing checklist file: {checklist_path}")

    marked: set[str] = set()
    for raw in checklist_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("- [x] "):
            marked.add(line.replace("- [x] ", "", 1).strip())
    return marked


def _required_for_mode(mode: str) -> tuple[str, ...]:
    if mode == "paper":
        return PAPER_REQUIRED_CHECKS
    if mode == "live_micro":
        return LIVE_MICRO_REQUIRED_CHECKS
    raise LiveGateError(f"unsupported mode: {mode}")


def _default_checklist_path_for_mode(mode: str) -> str:
    if mode == "paper":
        return "ops/paper_trading_gate.md"
    if mode == "live_micro":
        return "ops/live_microstake_gate.md"
    raise LiveGateError(f"unsupported mode: {mode}")


def evaluate(mode: str, checklist_path: str | None = None) -> dict[str, Any]:
    passed_checks: list[str] = []
    failed_checks: list[str] = []
    missing_checks: list[str] = []
    reasons: list[str] = []

    try:
        required_checks = _required_for_mode(mode)
        path_str = checklist_path or _default_checklist_path_for_mode(mode)
    except LiveGateError as exc:
        failed_checks.append("mode_supported")
        reasons.append(str(exc))
        return {
            "status": "FAIL",
            "gate": "live_gate",
            "mode": mode,
            "checklist_path": checklist_path or "",
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "missing_checks": missing_checks,
            "reasons": reasons,
        }

    checklist = Path(path_str)

    try:
        marked = _load_marked_checks(checklist)
    except LiveGateError as exc:
        failed_checks.append("checklist_present")
        reasons.append(str(exc))
        return {
            "status": "FAIL",
            "gate": "live_gate",
            "mode": mode,
            "checklist_path": str(checklist),
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "missing_checks": missing_checks,
            "reasons": reasons,
        }

    for check in required_checks:
        if check in marked:
            passed_checks.append(check)
        else:
            missing_checks.append(check)
            failed_checks.append(check)

    if missing_checks:
        reasons.append("required gate evidence not checked in checklist")

    status = "PASS" if not failed_checks else "FAIL"
    return {
        "status": status,
        "gate": "live_gate",
        "mode": mode,
        "checklist_path": str(checklist),
        "passed_checks": sorted(set(passed_checks)),
        "failed_checks": sorted(set(failed_checks)),
        "missing_checks": sorted(set(missing_checks)),
        "reasons": reasons,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fail-closed paper/live micro gate")
    parser.add_argument("--mode", required=True, choices=["paper", "live_micro"])
    parser.add_argument("--checklist-path", default="")
    parser.add_argument("--report-path", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = evaluate(
        mode=args.mode,
        checklist_path=args.checklist_path or None,
    )

    if args.report_path:
        report_path = Path(args.report_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
