from __future__ import annotations

import re
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

CHECKBOX_RE = re.compile(r"^\s*[-*+]\s*\[\s*([xX ]?)\s*\]\s+(.+?)\s*$")
TRIGGER_KEY_RE = re.compile(r"^\s*(pull_request|push|workflow_dispatch|schedule|workflow_run):\s*$")


def _checklist_ids_from_markdown(path: Path) -> tuple[list[str], list[str]]:
    checked_ids: list[str] = []
    all_ids: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        match = CHECKBOX_RE.match(raw)
        if not match:
            continue
        marker = match.group(1)
        check_id = match.group(2).strip()
        all_ids.append(check_id)
        if marker.lower() == "x":
            checked_ids.append(check_id)
    return checked_ids, all_ids


def _extract_trigger_paths_from_workflow_yaml(text: str) -> set[str]:
    lines = text.splitlines()
    on_idx = next((i for i, l in enumerate(lines) if re.match(r"^\s*on\s*:\s*$", l)), None)
    if on_idx is None:
        raise AssertionError("workflow missing 'on:' trigger block")

    on_indent = len(lines[on_idx]) - len(lines[on_idx].lstrip(" "))
    trigger_indent: int | None = None
    in_paths = False
    paths_indent = -1
    extracted: set[str] = set()

    i = on_idx + 1
    while i < len(lines):
        line = lines[i]
        if not line.strip() or line.strip().startswith("#"):
            i += 1
            continue
        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()

        if indent <= on_indent:
            break

        if TRIGGER_KEY_RE.match(stripped):
            trigger_indent = indent
            in_paths = False
            i += 1
            continue

        if trigger_indent is None:
            i += 1
            continue

        if indent <= trigger_indent:
            in_paths = False
            i += 1
            continue

        if re.match(r"^paths\s*:\s*$", stripped):
            in_paths = True
            paths_indent = indent
            i += 1
            continue

        if in_paths and indent > paths_indent and stripped.startswith("-"):
            value = stripped[1:].strip().strip('"\'')
            if value:
                extracted.add(value)
            i += 1
            continue

        if in_paths and indent <= paths_indent:
            in_paths = False

        i += 1

    if not extracted:
        raise AssertionError("no trigger paths found under on.<trigger>.paths")
    return extracted


def _extract_run_blocks_from_workflow_text(text: str) -> list[str]:
    blocks: list[str] = []
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        match = re.match(r"^(\s*)run:\s*(.*)$", line)
        if not match:
            i += 1
            continue
        indent = len(match.group(1))
        tail = match.group(2).strip()
        if tail and tail != "|":
            blocks.append(tail.strip('"\''))
            i += 1
            continue

        block_lines: list[str] = []
        i += 1
        while i < len(lines):
            next_line = lines[i]
            if next_line.strip() == "":
                block_lines.append("")
                i += 1
                continue
            current_indent = len(next_line) - len(next_line.lstrip(" "))
            if current_indent <= indent:
                break
            block_lines.append(next_line.strip())
            i += 1
        blocks.append("\n".join(block_lines))
    return blocks


def test_ops_docs_and_scripts_exist() -> None:
    for rel in OPS_DOCS + OPS_SCRIPTS:
        assert Path(rel).exists(), rel


def test_markdown_parser_accepts_checkbox_variants(tmp_path: Path) -> None:
    sample = tmp_path / "ops_alignment_parser_sample.md"
    sample.write_text(
        "\n".join(
            [
                "- [x] alpha",
                "- [X] beta",
                "- [ ] gamma",
                "  * [ x ] delta",
                "+ [X] epsilon",
            ]
        ),
        encoding="utf-8",
    )
    checked, all_ids = _checklist_ids_from_markdown(sample)
    assert checked == ["alpha", "beta", "delta", "epsilon"]
    assert all_ids == ["alpha", "beta", "gamma", "delta", "epsilon"]


def test_readiness_required_checks_present_and_unique() -> None:
    _, ids = _checklist_ids_from_markdown(Path("ops/readiness_checklist.md"))
    assert len(ids) == len(set(ids)), "duplicate readiness checklist ids"
    for check in readiness_gate.REQUIRED_CHECKS:
        assert check in ids


def test_rollback_required_checks_present_and_unique() -> None:
    _, ids = _checklist_ids_from_markdown(Path("ops/rollback_checklist.md"))
    assert len(ids) == len(set(ids)), "duplicate rollback checklist ids"
    for check in rollback_gate.REQUIRED_CHECKS:
        assert check in ids


def test_live_micro_required_checks_present_and_unique() -> None:
    _, ids = _checklist_ids_from_markdown(Path("ops/live_microstake_gate.md"))
    assert len(ids) == len(set(ids)), "duplicate live checklist ids"
    for check in live_gate.LIVE_MICRO_REQUIRED_CHECKS:
        assert check in ids


def test_scripts_fail_closed_for_missing_checklist() -> None:
    assert readiness_gate.evaluate("does-not-exist.md")["status"] == "FAIL"
    assert rollback_gate.evaluate("does-not-exist.md")["status"] == "FAIL"
    assert live_gate.evaluate(mode="live_micro", checklist_path="does-not-exist.md")["status"] == "FAIL"


def test_readiness_fails_when_required_item_not_checked(tmp_path: Path) -> None:
    only_checked = readiness_gate.REQUIRED_CHECKS[0]
    checklist = tmp_path / "readiness_checklist.md"
    checklist.write_text(f"- [x] {only_checked}\n", encoding="utf-8")

    result = readiness_gate.evaluate(str(checklist))
    expected_missing = sorted(set(readiness_gate.REQUIRED_CHECKS) - {only_checked})
    assert result["status"] == "FAIL"
    assert result["missing_checks"] == expected_missing


def test_rollback_fails_when_required_item_not_checked(tmp_path: Path) -> None:
    only_checked = rollback_gate.REQUIRED_CHECKS[0]
    checklist = tmp_path / "rollback_checklist.md"
    checklist.write_text(f"- [x] {only_checked}\n", encoding="utf-8")

    result = rollback_gate.evaluate(str(checklist))
    expected_missing = sorted(set(rollback_gate.REQUIRED_CHECKS) - {only_checked})
    assert result["status"] == "FAIL"
    assert result["missing_checks"] == expected_missing


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


def test_incident_snapshot_passes_when_requested_files_exist() -> None:
    result = incident_snapshot.collect_snapshot(["ops/readiness_checklist.md"])
    assert result["status"] == "PASS"
    assert result["missing_files"] == []


def test_workflows_cover_ops_paths_and_invoke_tests() -> None:
    for workflow_path, expected_paths in WORKFLOW_EXPECTATIONS.items():
        workflow = Path(workflow_path)
        assert workflow.exists(), workflow_path

        text = workflow.read_text(encoding="utf-8")
        paths = _extract_trigger_paths_from_workflow_yaml(text)
        for rel in expected_paths:
            assert rel in paths, f"{workflow_path} missing path: {rel}"

        expected_test = [p for p in expected_paths if p.startswith("tests/")][0]
        run_blocks = _extract_run_blocks_from_workflow_text(text)
        assert any("pytest" in run and expected_test in run for run in run_blocks), (
            f"{workflow_path} does not run pytest against {expected_test}"
        )
