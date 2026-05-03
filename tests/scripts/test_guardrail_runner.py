from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from scripts import guardrail_runner


def _mk_file(path: Path, content: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _mk_guardrail(path: Path, module: str, module_paths: list[str], focused_tests: list[str] | None = None) -> None:
    payload = {"module": module, "module_paths": module_paths}
    if focused_tests is not None:
        payload["focused_tests"] = focused_tests
    if "mutations" in path.parts:
        payload["mutations"] = [{"id": "m1", "expected_failure": "tests"}]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_happy_path_real_module() -> None:
    repo_root = guardrail_runner.find_repo_root()
    report = guardrail_runner.validate_module_guardrails(
        module="telegram_signal_processor",
        guardrails_root=repo_root / "guardrails",
        repo_root=repo_root,
        fail_on_missing_tests=True,
    )
    assert report["ok"] is True
    assert len(report["checked_files"]) == 4


def test_missing_guardrail_file_fail_closed(tmp_path: Path) -> None:
    repo = tmp_path
    _mk_file(repo / ".git")
    _mk_file(repo / "src" / "m.py")
    for kind in ["specs", "contracts", "state_models"]:
        _mk_guardrail(repo / "guardrails" / kind / "mod.json", "mod", ["src/m.py"])
    report = guardrail_runner.validate_module_guardrails("mod", repo / "guardrails", repo, True)
    assert report["ok"] is False
    assert any("Missing required guardrail file" in e for e in report["errors"])


def test_invalid_json_fail_closed(tmp_path: Path) -> None:
    repo = tmp_path
    _mk_file(repo / ".git")
    _mk_file(repo / "src" / "m.py")
    for kind in ["specs", "contracts", "state_models", "mutations"]:
        p = repo / "guardrails" / kind / "mod.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("{bad", encoding="utf-8")
    report = guardrail_runner.validate_module_guardrails("mod", repo / "guardrails", repo, True)
    assert report["ok"] is False
    assert any("Invalid JSON" in e for e in report["errors"])


def test_module_mismatch_fail_closed(tmp_path: Path) -> None:
    repo = tmp_path
    _mk_file(repo / ".git")
    _mk_file(repo / "src" / "m.py")
    for kind in ["specs", "contracts", "state_models", "mutations"]:
        _mk_guardrail(repo / "guardrails" / kind / "mod.json", "other", ["src/m.py"])
    report = guardrail_runner.validate_module_guardrails("mod", repo / "guardrails", repo, True)
    assert any("Module mismatch" in e for e in report["errors"])


def test_missing_module_path_fail_closed(tmp_path: Path) -> None:
    repo = tmp_path
    _mk_file(repo / ".git")
    for kind in ["specs", "contracts", "state_models", "mutations"]:
        _mk_guardrail(repo / "guardrails" / kind / "mod.json", "mod", ["src/missing.py"])
    report = guardrail_runner.validate_module_guardrails("mod", repo / "guardrails", repo, True)
    assert any("Missing module path" in e for e in report["errors"])


def test_missing_focused_test_fail_closed(tmp_path: Path) -> None:
    repo = tmp_path
    _mk_file(repo / ".git")
    _mk_file(repo / "src" / "m.py")
    for kind in ["specs", "contracts", "state_models", "mutations"]:
        _mk_guardrail(repo / "guardrails" / kind / "mod.json", "mod", ["src/m.py"], ["tests/missing.py"])
    report = guardrail_runner.validate_module_guardrails("mod", repo / "guardrails", repo, True)
    assert any("Missing focused test" in e for e in report["errors"])


def test_mutations_empty_fail_closed(tmp_path: Path) -> None:
    repo = tmp_path
    _mk_file(repo / ".git")
    _mk_file(repo / "src" / "m.py")
    for kind in ["specs", "contracts", "state_models"]:
        _mk_guardrail(repo / "guardrails" / kind / "mod.json", "mod", ["src/m.py"])
    p = repo / "guardrails" / "mutations" / "mod.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"module": "mod", "module_paths": ["src/m.py"], "mutations": []}), encoding="utf-8")
    report = guardrail_runner.validate_module_guardrails("mod", repo / "guardrails", repo, True)
    assert any("mutations missing/empty" in e for e in report["errors"])


def test_mutation_expected_failure_required(tmp_path: Path) -> None:
    repo = tmp_path
    _mk_file(repo / ".git")
    _mk_file(repo / "src" / "m.py")
    for kind in ["specs", "contracts", "state_models"]:
        _mk_guardrail(repo / "guardrails" / kind / "mod.json", "mod", ["src/m.py"])
    p = repo / "guardrails" / "mutations" / "mod.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"module": "mod", "module_paths": ["src/m.py"], "mutations": [{"id": "m1"}]}), encoding="utf-8")
    report = guardrail_runner.validate_module_guardrails("mod", repo / "guardrails", repo, True)
    assert any("missing expected_failure" in e for e in report["errors"])


def test_no_repo_root_mutation_artifact() -> None:
    repo_root = guardrail_runner.find_repo_root()
    root_artifact = repo_root / "mutation_guardrails_report.json"
    if root_artifact.exists():
        root_artifact.unlink()
    out = Path("/tmp/guardrail_runner_test_output.json")
    cmd = [sys.executable, "scripts/guardrail_runner.py", "--module", "telegram_signal_processor", "--run-mutations", "--mutation-timeout-sec", "1", "--output", str(out)]
    subprocess.run(cmd, cwd=repo_root, capture_output=True, text=True)
    assert out.exists()
    assert not root_artifact.exists()


def test_cli_help() -> None:
    repo_root = guardrail_runner.find_repo_root()
    proc = subprocess.run([sys.executable, "scripts/guardrail_runner.py", "--help"], cwd=repo_root, capture_output=True, text=True)
    assert proc.returncode == 0
    assert "--module" in proc.stdout
