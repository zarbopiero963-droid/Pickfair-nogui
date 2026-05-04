from __future__ import annotations

from pathlib import Path

from scripts import incident_snapshot, live_gate, readiness_gate, rollback_gate

OPS_DOCS = (
    "ops/readiness_checklist.md",
    "ops/rollback_checklist.md",
    "ops/live_microstake_gate.md",
    "ops/incident_playbook.md",
)

OPS_SCRIPTS = (
    "scripts/readiness_gate.py",
    "scripts/rollback_gate.py",
    "scripts/live_gate.py",
    "scripts/incident_snapshot.py",
)

WORKFLOW_EXPECTATIONS = {
    ".github/workflows/readiness-gate.yml": (
        "ops/readiness_checklist.md",
        "scripts/readiness_gate.py",
        "tests/scripts/test_readiness_gate.py",
    ),
    ".github/workflows/rollback-gate.yml": (
        "ops/rollback_checklist.md",
        "scripts/rollback_gate.py",
        "tests/scripts/test_rollback_gate.py",
    ),
    ".github/workflows/live-gate.yml": (
        "ops/live_microstake_gate.md",
        "scripts/live_gate.py",
        "tests/scripts/test_live_gate.py",
    ),
    ".github/workflows/incident-snapshot.yml": (
        "ops/incident_playbook.md",
        "scripts/incident_snapshot.py",
        "tests/scripts/test_incident_snapshot.py",
    ),
}


def _checked_ids_from_markdown(path: Path) -> list[str]:
    ids: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("- [") and "] " in line:
            ids.append(line.split("] ", 1)[1].strip())
    return ids


def test_ops_docs_and_scripts_exist() -> None:
    for rel in OPS_DOCS + OPS_SCRIPTS:
        assert Path(rel).exists(), rel


def test_readiness_required_checks_present_and_unique() -> None:
    ids = _checked_ids_from_markdown(Path("ops/readiness_checklist.md"))
    assert len(ids) == len(set(ids)), "duplicate readiness checklist ids"
    for check in readiness_gate.REQUIRED_CHECKS:
        assert check in ids


def test_rollback_required_checks_present_and_unique() -> None:
    ids = _checked_ids_from_markdown(Path("ops/rollback_checklist.md"))
    assert len(ids) == len(set(ids)), "duplicate rollback checklist ids"
    for check in rollback_gate.REQUIRED_CHECKS:
        assert check in ids


def test_live_micro_required_checks_present_and_unique() -> None:
    ids = _checked_ids_from_markdown(Path("ops/live_microstake_gate.md"))
    assert len(ids) == len(set(ids)), "duplicate live checklist ids"
    for check in live_gate.LIVE_MICRO_REQUIRED_CHECKS:
        assert check in ids


def test_scripts_fail_closed_for_missing_checklist() -> None:
    assert readiness_gate.evaluate("does-not-exist.md")["status"] == "FAIL"
    assert rollback_gate.evaluate("does-not-exist.md")["status"] == "FAIL"
    assert live_gate.evaluate(mode="live_micro", checklist_path="does-not-exist.md")["status"] == "FAIL"


def test_live_gate_fails_when_required_item_not_marked(tmp_path: Path) -> None:
    checklist = tmp_path / "live_microstake_gate.md"
    checklist.write_text("- [x] readiness_passed\n", encoding="utf-8")

    result = live_gate.evaluate(mode="live_micro", checklist_path=str(checklist))
    assert result["status"] == "FAIL"
    assert "rollback_passed" in result["missing_checks"]


def test_incident_snapshot_contract_includes_ops_docs() -> None:
    for rel in OPS_DOCS:
        assert rel in incident_snapshot.DEFAULT_EVIDENCE_PATHS


def test_incident_snapshot_fails_closed_when_requested_file_missing() -> None:
    result = incident_snapshot.collect_snapshot(["ops/readiness_checklist.md", "missing-file.md"])
    assert result["status"] == "FAIL"
    assert "missing-file.md" in result["missing_files"]


def test_workflows_cover_ops_paths_and_invoke_tests() -> None:
    for workflow_path, expected_paths in WORKFLOW_EXPECTATIONS.items():
        workflow = Path(workflow_path)
        assert workflow.exists(), workflow_path

        text = workflow.read_text(encoding="utf-8")
        for rel in expected_paths:
            assert f'- "{rel}"' in text, f"{workflow_path} missing path: {rel}"

        expected_test = [p for p in expected_paths if p.startswith("tests/")][0]
        assert expected_test in text, f"{workflow_path} does not invoke {expected_test}"
