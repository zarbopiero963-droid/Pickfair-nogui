from __future__ import annotations

import json
from pathlib import Path

from scripts.incident_snapshot import collect_snapshot, main


def test_collects_existing_files(tmp_path: Path) -> None:
    evidence_a = tmp_path / "a.txt"
    evidence_b = tmp_path / "b.txt"
    evidence_a.write_text("alpha", encoding="utf-8")
    evidence_b.write_text("beta", encoding="utf-8")

    result = collect_snapshot(evidence_paths=[str(evidence_a), str(evidence_b)])

    assert result["status"] == "PASS"
    assert result["missing_files"] == []
    assert len(result["existing_files"]) == 2
    assert {item["path"] for item in result["existing_files"]} == {
        str(evidence_a),
        str(evidence_b),
    }


def test_marks_missing_files_explicitly(tmp_path: Path) -> None:
    evidence_a = tmp_path / "a.txt"
    missing = tmp_path / "missing.txt"
    evidence_a.write_text("alpha", encoding="utf-8")

    result = collect_snapshot(evidence_paths=[str(evidence_a), str(missing)])

    assert result["status"] == "FAIL"
    assert result["missing_files"] == [str(missing)]
    assert result["reasons"]


def test_incident_snapshot_main_writes_machine_readable_report(tmp_path: Path) -> None:
    evidence_a = tmp_path / "a.txt"
    report = tmp_path / "out" / "incident_snapshot.json"
    evidence_a.write_text("alpha", encoding="utf-8")

    code = main([
        "--evidence-path",
        str(evidence_a),
        "--report-path",
        str(report),
    ])

    assert code == 0
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert set(payload.keys()) == {
        "status",
        "snapshot",
        "existing_files",
        "missing_files",
        "requested_files",
        "reasons",
    }
    assert payload["status"] == "PASS"
    assert payload["snapshot"] == "incident"
