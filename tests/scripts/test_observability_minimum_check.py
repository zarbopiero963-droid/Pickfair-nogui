from __future__ import annotations

import json
from pathlib import Path

from scripts.observability_minimum_check import REQUIRED_SECTION_MARKERS, evaluate, main


def _write_valid_doc(path: Path) -> None:
    lines = ["# Observability Minimum"]
    lines.extend(REQUIRED_SECTION_MARKERS)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_observability_minimum_pass_when_required_sections_present(tmp_path: Path) -> None:
    doc = tmp_path / "observability_minimum.md"
    _write_valid_doc(doc)

    result = evaluate(doc_path=str(doc))

    assert result["status"] == "PASS"
    assert result["failed_checks"] == []
    assert result["missing_checks"] == []


def test_observability_minimum_fail_when_file_missing(tmp_path: Path) -> None:
    doc = tmp_path / "missing_observability_minimum.md"

    result = evaluate(doc_path=str(doc))

    assert result["status"] == "FAIL"
    assert "observability_minimum_doc_present" in result["failed_checks"]
    assert "observability_minimum_doc_present" in result["missing_checks"]


def test_observability_minimum_fail_when_required_section_missing(tmp_path: Path) -> None:
    doc = tmp_path / "observability_minimum.md"
    markers = list(REQUIRED_SECTION_MARKERS)
    removed = markers.pop()
    doc.write_text("\n".join(["# Observability Minimum", *markers]) + "\n", encoding="utf-8")

    result = evaluate(doc_path=str(doc))

    assert result["status"] == "FAIL"
    missing_key = f"section:{removed.replace('## ', '').strip()}"
    assert missing_key in result["missing_checks"]
    assert missing_key in result["failed_checks"]


def test_observability_minimum_main_writes_machine_readable_json_contract(tmp_path: Path) -> None:
    doc = tmp_path / "observability_minimum.md"
    report = tmp_path / "out" / "observability_minimum_report.json"
    _write_valid_doc(doc)

    code = main([
        "--doc-path",
        str(doc),
        "--report-path",
        str(report),
    ])

    assert code == 0
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert set(payload.keys()) == {
        "status",
        "gate",
        "passed_checks",
        "failed_checks",
        "missing_checks",
        "reasons",
    }
    assert payload["status"] == "PASS"
    assert payload["gate"] == "observability_minimum"
