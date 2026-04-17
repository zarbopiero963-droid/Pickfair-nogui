from __future__ import annotations

import json
from pathlib import Path

from scripts.live_gate import evaluate, main


def test_paper_pass_with_all_required_evidence(tmp_path: Path) -> None:
    checklist = tmp_path / "paper_trading_gate.md"
    checklist.write_text(
        "\n".join([
            "- [x] readiness_passed",
            "- [x] observability_minimum_passed",
            "- [x] db_backup_restore_discipline_present",
            "- [x] no_open_incident",
            "- [x] operator_acknowledged",
        ]),
        encoding="utf-8",
    )

    result = evaluate(mode="paper", checklist_path=str(checklist))

    assert result["status"] == "PASS"
    assert result["failed_checks"] == []


def test_paper_fail_when_required_evidence_missing(tmp_path: Path) -> None:
    checklist = tmp_path / "paper_trading_gate.md"
    checklist.write_text("- [x] readiness_passed\n", encoding="utf-8")

    result = evaluate(mode="paper", checklist_path=str(checklist))

    assert result["status"] == "FAIL"
    assert "operator_acknowledged" in result["missing_checks"]


def test_live_micro_pass_with_all_required_evidence(tmp_path: Path) -> None:
    checklist = tmp_path / "live_microstake_gate.md"
    checklist.write_text(
        "\n".join([
            "- [x] readiness_passed",
            "- [x] rollback_passed",
            "- [x] observability_minimum_passed",
            "- [x] db_backup_restore_discipline_present",
            "- [x] hard_stop_limits_present",
            "- [x] strict_live_key_source_enabled_or_equivalent_explicit_confirmation",
            "- [x] no_open_incident",
            "- [x] operator_acknowledged",
            "- [x] paper_results_reviewed",
            "- [x] max_stake_approved",
            "- [x] kill_switch_confirmed",
        ]),
        encoding="utf-8",
    )

    result = evaluate(mode="live_micro", checklist_path=str(checklist))

    assert result["status"] == "PASS"
    assert result["failed_checks"] == []


def test_live_micro_fail_when_required_evidence_missing(tmp_path: Path) -> None:
    checklist = tmp_path / "live_microstake_gate.md"
    checklist.write_text("- [x] readiness_passed\n", encoding="utf-8")

    result = evaluate(mode="live_micro", checklist_path=str(checklist))

    assert result["status"] == "FAIL"
    assert "rollback_passed" in result["missing_checks"]
    assert "kill_switch_confirmed" in result["missing_checks"]


def test_live_gate_main_writes_machine_readable_report(tmp_path: Path) -> None:
    checklist = tmp_path / "paper_trading_gate.md"
    report = tmp_path / "out" / "live_gate_report.json"
    checklist.write_text(
        "\n".join([
            "- [x] readiness_passed",
            "- [x] observability_minimum_passed",
            "- [x] db_backup_restore_discipline_present",
            "- [x] no_open_incident",
            "- [x] operator_acknowledged",
        ]),
        encoding="utf-8",
    )

    code = main([
        "--mode",
        "paper",
        "--checklist-path",
        str(checklist),
        "--report-path",
        str(report),
    ])

    assert code == 0
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert set(payload.keys()) == {
        "status",
        "gate",
        "mode",
        "checklist_path",
        "passed_checks",
        "failed_checks",
        "missing_checks",
        "reasons",
    }
    assert payload["status"] == "PASS"
    assert payload["gate"] == "live_gate"
    assert payload["mode"] == "paper"
