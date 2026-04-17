from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

REQUIRED_SECTION_MARKERS = (
    "## startup_shutdown_visibility",
    "## order_lifecycle_visibility",
    "## live_readiness_deploy_gate_visibility",
    "## risk_safety_deny_visibility",
    "## reconcile_recovery_visibility",
    "## anomaly_incident_visibility",
    "## alert_pipeline_deliverability_visibility",
    "## diagnostics_export_bundle_minimum",
    "## kill_switch_lockdown_emergency_visibility",
    "## non_negotiable_rule",
)


class ObservabilityMinimumCheckError(RuntimeError):
    pass


def evaluate(doc_path: str = "ops/observability_minimum.md") -> dict[str, Any]:
    passed_checks: list[str] = []
    failed_checks: list[str] = []
    missing_checks: list[str] = []
    reasons: list[str] = []

    path = Path(doc_path)
    if not path.exists():
        failed_checks.append("observability_minimum_doc_present")
        missing_checks.append("observability_minimum_doc_present")
        reasons.append(f"missing file: {path}")
        return {
            "status": "FAIL",
            "gate": "observability_minimum",
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "missing_checks": missing_checks,
            "reasons": reasons,
        }

    passed_checks.append("observability_minimum_doc_present")

    text = path.read_text(encoding="utf-8")
    for marker in REQUIRED_SECTION_MARKERS:
        key = f"section:{marker.replace('## ', '').strip()}"
        if marker in text:
            passed_checks.append(key)
        else:
            missing_checks.append(key)
            failed_checks.append(key)
            reasons.append(f"missing required section marker: {marker}")

    status = "PASS" if not failed_checks else "FAIL"
    return {
        "status": status,
        "gate": "observability_minimum",
        "passed_checks": sorted(set(passed_checks)),
        "failed_checks": sorted(set(failed_checks)),
        "missing_checks": sorted(set(missing_checks)),
        "reasons": reasons,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fail-closed observability minimum check")
    parser.add_argument("--doc-path", default="ops/observability_minimum.md")
    parser.add_argument("--report-path", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = evaluate(doc_path=args.doc_path)

    if args.report_path:
        report_path = Path(args.report_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
