from __future__ import annotations

import json
from pathlib import Path

from scripts.rollback_gate import evaluate, main


def test_rollback_pass_when_db_evidence_checked(tmp_path: Path) -> None:
    checklist = tmp_path / "rollback_checklist.md"
    checklist.write_text(
        "\n".join([
            "- [x] db_restore_procedure_documented",
            "- [x] db_restore_validation_verified",
        ]),
        encoding="utf-8",
    )

    result = evaluate(checklist_path=str(checklist))

    assert result["status"] == "PASS"
    assert result["failed_checks"] == []


def test_rollback_fails_when_db_evidence_missing(tmp_path: Path) -> None:
    checklist = tmp_path / "rollback_checklist.md"
    checklist.write_text("- [ ] db_restore_procedure_documented\n", encoding="utf-8")

    result = evaluate(checklist_path=str(checklist))

    assert result["status"] == "FAIL"
    assert "db_restore_procedure_documented" in result["missing_checks"]
    assert "db_restore_validation_verified" in result["missing_checks"]
    assert "db_restore_procedure_documented" in result["failed_checks"]
    assert "db_restore_validation_verified" in result["failed_checks"]


def test_rollback_main_writes_machine_readable_report(tmp_path: Path) -> None:
    checklist = tmp_path / "rollback_checklist.md"
    report = tmp_path / "out" / "rollback_report.json"
    checklist.write_text(
        "\n".join([
            "- [x] db_restore_procedure_documented",
            "- [x] db_restore_validation_verified",
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
    assert payload["gate"] == "rollback"
