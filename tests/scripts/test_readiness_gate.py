from __future__ import annotations

import json
from pathlib import Path

from scripts.readiness_gate import evaluate, main


def test_readiness_pass_when_db_evidence_checked(tmp_path: Path) -> None:
    checklist = tmp_path / "readiness_checklist.md"
    checklist.write_text(
        "\n".join([
            "- [x] db_backup_restore_runbook_present",
            "- [x] db_restore_validation_available",
        ]),
        encoding="utf-8",
    )

    result = evaluate(checklist_path=str(checklist))

    assert result["status"] == "PASS"
    assert result["failed_checks"] == []


def test_readiness_fails_when_db_evidence_missing(tmp_path: Path) -> None:
    checklist = tmp_path / "readiness_checklist.md"
    checklist.write_text("- [ ] db_backup_restore_runbook_present\n", encoding="utf-8")

    result = evaluate(checklist_path=str(checklist))

    assert result["status"] == "FAIL"
    assert "db_backup_restore_runbook_present" in result["missing_checks"]
    assert "db_restore_validation_available" in result["missing_checks"]
    assert "db_backup_restore_runbook_present" in result["failed_checks"]
    assert "db_restore_validation_available" in result["failed_checks"]


def test_readiness_main_writes_machine_readable_report(tmp_path: Path) -> None:
    checklist = tmp_path / "readiness_checklist.md"
    report = tmp_path / "out" / "readiness_report.json"
    checklist.write_text(
        "\n".join([
            "- [x] db_backup_restore_runbook_present",
            "- [x] db_restore_validation_available",
        ]),
        encoding="utf-8",
    )

    code = main([
        "--checklist-path",
        str(checklist),
        "--report-path",
        str(report),
    ])

    assert code == 0
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert payload["status"] == "PASS"
    assert payload["gate"] == "readiness"
