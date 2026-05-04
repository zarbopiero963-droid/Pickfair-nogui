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
    report = guardrail_runner.validate_module_guardrails("telegram_signal_processor", repo_root / "guardrails", repo_root, True)
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


def test_module_path_validation_safe_relative_file(tmp_path: Path) -> None:
    repo = tmp_path
    _mk_file(repo / ".git")
    _mk_file(repo / "src" / "m.py")
    _mk_file(repo / "src" / "dir" / "child.py")
    for kind in ["specs", "contracts", "state_models"]:
        _mk_guardrail(repo / "guardrails" / kind / "mod.json", "mod", ["/etc/passwd"])
    _mk_guardrail(repo / "guardrails" / "mutations" / "mod.json", "mod", ["../outside.py"])
    report = guardrail_runner.validate_module_guardrails("mod", repo / "guardrails", repo, True)
    assert any("absolute" in e for e in report["errors"])
    assert any("escapes repo_root" in e for e in report["errors"])


def test_module_path_must_be_file_and_non_empty(tmp_path: Path) -> None:
    repo = tmp_path
    _mk_file(repo / ".git")
    (repo / "src" / "pkg").mkdir(parents=True)
    for kind in ["specs", "contracts", "state_models", "mutations"]:
        p = repo / "guardrails" / kind / "mod.json"
        payload = {"module": "mod", "module_paths": ["src/pkg", ""], "mutations": [{"id": "m1", "expected_failure": "tests"}]} if kind == "mutations" else {"module": "mod", "module_paths": ["src/pkg", ""]}
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(payload), encoding="utf-8")
    report = guardrail_runner.validate_module_guardrails("mod", repo / "guardrails", repo, True)
    assert any("not a file" in e for e in report["errors"])
    assert any("Missing module path" in e for e in report["errors"])


def test_missing_focused_test_fail_closed(tmp_path: Path) -> None:
    repo = tmp_path
    _mk_file(repo / ".git")
    _mk_file(repo / "src" / "m.py")
    for kind in ["specs", "contracts", "state_models", "mutations"]:
        _mk_guardrail(repo / "guardrails" / kind / "mod.json", "mod", ["src/m.py"], ["tests/missing.py"])
    report = guardrail_runner.validate_module_guardrails("mod", repo / "guardrails", repo, True)
    assert any("Missing module path" in e and "focused_tests" in e for e in report["errors"])


def test_focused_tests_validation_absolute_escape_and_file(tmp_path: Path) -> None:
    repo = tmp_path
    _mk_file(repo / ".git")
    _mk_file(repo / "src" / "m.py")
    (repo / "tests" / "pkg").mkdir(parents=True)
    for kind in ["specs", "contracts", "state_models", "mutations"]:
        _mk_guardrail(
            repo / "guardrails" / kind / "mod.json",
            "mod",
            ["src/m.py"],
            ["/tmp/a.py", "../outside.py", "tests/pkg"],
        )
    report = guardrail_runner.validate_module_guardrails("mod", repo / "guardrails", repo, True)
    assert any("absolute" in e and "focused_tests" in e for e in report["errors"])
    assert any("escapes repo_root" in e and "focused_tests" in e for e in report["errors"])
    assert any("not a file" in e and "focused_tests" in e for e in report["errors"])


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


def test_mutations_not_list_fail_closed(tmp_path: Path) -> None:
    repo = tmp_path
    _mk_file(repo / ".git")
    _mk_file(repo / "src" / "m.py")
    for kind in ["specs", "contracts", "state_models"]:
        _mk_guardrail(repo / "guardrails" / kind / "mod.json", "mod", ["src/m.py"])
    p = repo / "guardrails" / "mutations" / "mod.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"module": "mod", "module_paths": ["src/m.py"], "mutations": "bad"}), encoding="utf-8")
    report = guardrail_runner.validate_module_guardrails("mod", repo / "guardrails", repo, True)
    assert any("mutations missing/empty" in e for e in report["errors"])


def test_mutation_entry_not_object_and_missing_id(tmp_path: Path) -> None:
    repo = tmp_path
    _mk_file(repo / ".git")
    _mk_file(repo / "src" / "m.py")
    for kind in ["specs", "contracts", "state_models"]:
        _mk_guardrail(repo / "guardrails" / kind / "mod.json", "mod", ["src/m.py"])
    p = repo / "guardrails" / "mutations" / "mod.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"module": "mod", "module_paths": ["src/m.py"], "mutations": ["bad", {"expected_failure": "x"}]}), encoding="utf-8")
    report = guardrail_runner.validate_module_guardrails("mod", repo / "guardrails", repo, True)
    assert any("is not object" in e for e in report["errors"])
    assert any("missing id" in e for e in report["errors"])


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


def test_delegate_total_zero_is_fatal(monkeypatch: object, tmp_path: Path) -> None:
    def fake_run(*args: object, **kwargs: object):
        out = Path(args[0][-1])
        out.write_text(json.dumps({"summary": {"total": 0, "killed": 0, "survived": 0, "score": 0.0}}), encoding="utf-8")
        class R: returncode = 0; stdout = ""; stderr = ""
        return R()

    monkeypatch.setattr(guardrail_runner.subprocess, "run", fake_run)
    res = guardrail_runner.run_mutation_delegate("telegram_signal_processor", 1, guardrail_runner.find_repo_root())
    assert res["ok"] is False
    assert "total=0" in res["error"]


def test_delegate_negative_total_is_fatal(monkeypatch: object) -> None:
    def fake_run(*args: object, **kwargs: object):
        out = Path(args[0][-1])
        out.write_text(json.dumps({"summary": {"total": -1, "killed": 0, "survived": 0, "score": 0.0}}), encoding="utf-8")
        class R: returncode = 0; stdout = ""; stderr = ""
        return R()

    monkeypatch.setattr(guardrail_runner.subprocess, "run", fake_run)
    res = guardrail_runner.run_mutation_delegate("telegram_signal_processor", 1, guardrail_runner.find_repo_root())
    assert res["ok"] is False
    assert "total < 0" in res["error"]


def test_delegate_no_parseable_total_is_fatal(monkeypatch: object) -> None:
    def fake_run(*args: object, **kwargs: object):
        out = Path(args[0][-1])
        out.write_text(json.dumps({"summary": {"killed": 1}}), encoding="utf-8")
        class R: returncode = 0; stdout = ""; stderr = ""
        return R()

    monkeypatch.setattr(guardrail_runner.subprocess, "run", fake_run)
    res = guardrail_runner.run_mutation_delegate("telegram_signal_processor", 1, guardrail_runner.find_repo_root())
    assert res["ok"] is False
    assert "parseable integer total" in res["error"]


def test_delegate_timeout(monkeypatch: object) -> None:
    def fake_run(*args: object, **kwargs: object):
        raise subprocess.TimeoutExpired(cmd=args[0], timeout=1)
    monkeypatch.setattr(guardrail_runner.subprocess, "run", fake_run)
    res = guardrail_runner.run_mutation_delegate("telegram_signal_processor", 1, guardrail_runner.find_repo_root())
    assert res["ok"] is False
    assert "timeout" in res["error"]


def test_delegate_missing_script(tmp_path: Path) -> None:
    repo = tmp_path
    _mk_file(repo / ".git")
    res = guardrail_runner.run_mutation_delegate("mod", 1, repo)
    assert res["ok"] is False
    assert "Missing delegate script" in res["error"]


def test_delegate_invalid_json_output(monkeypatch: object) -> None:
    def fake_run(*args: object, **kwargs: object):
        out = Path(args[0][-1])
        out.write_text("", encoding="utf-8")
        class R: returncode = 0; stdout = ""; stderr = ""
        return R()
    monkeypatch.setattr(guardrail_runner.subprocess, "run", fake_run)
    res = guardrail_runner.run_mutation_delegate("telegram_signal_processor", 1, guardrail_runner.find_repo_root())
    assert res["ok"] is False
    assert "Failed reading mutation delegate output" in res["error"]


def test_delegate_nonzero_exit_with_total_is_warning(monkeypatch: object) -> None:
    def fake_run(*args: object, **kwargs: object):
        out = Path(args[0][-1])
        out.write_text(json.dumps({"summary": {"total": 2, "killed": 1, "survived": 1, "score": 50.0}}), encoding="utf-8")
        class R: returncode = 1; stdout = ""; stderr = "threshold"
        return R()

    monkeypatch.setattr(guardrail_runner.subprocess, "run", fake_run)
    res = guardrail_runner.run_mutation_delegate("telegram_signal_processor", 1, guardrail_runner.find_repo_root())
    assert res["ok"] is True
    assert res["total"] == 2
    assert "warning" in res


def test_no_repo_root_mutation_artifact(tmp_path: Path) -> None:
    repo_root = guardrail_runner.find_repo_root()
    root_artifact = repo_root / "mutation_guardrails_report.json"
    if root_artifact.exists():
        root_artifact.unlink()
    out = tmp_path / "guardrail_runner_test_output.json"
    subprocess.run([sys.executable, "scripts/guardrail_runner.py", "--module", "telegram_signal_processor", "--run-mutations", "--mutation-timeout-sec", "1", "--output", str(out)], cwd=repo_root, capture_output=True, text=True)
    assert out.exists()
    assert not root_artifact.exists()


def test_cli_fail_on_missing_tests_toggle(tmp_path: Path) -> None:
    repo_root = guardrail_runner.find_repo_root()
    repo = tmp_path / "repo"
    _mk_file(repo / ".git")
    for kind in ["specs", "contracts", "state_models", "mutations"]:
        _mk_guardrail(repo / "guardrails" / kind / "mod.json", "mod", ["scripts/guardrail_runner.py"], ["tests/missing.py"])
    out = tmp_path / "report.json"
    script = repo_root / "scripts" / "guardrail_runner.py"
    proc_fail = subprocess.run([sys.executable, str(script), "--module", "mod", "--guardrails-root", str(repo / "guardrails"), "--output", str(out)], cwd=repo_root, capture_output=True, text=True)
    proc_pass = subprocess.run([sys.executable, str(script), "--module", "mod", "--guardrails-root", str(repo / "guardrails"), "--output", str(out), "--fail-on-missing-tests", "false"], cwd=repo_root, capture_output=True, text=True)
    assert proc_fail.returncode != 0
    assert proc_pass.returncode == 0


def test_cli_help() -> None:
    repo_root = guardrail_runner.find_repo_root()
    proc = subprocess.run([sys.executable, "scripts/guardrail_runner.py", "--help"], cwd=repo_root, capture_output=True, text=True)
    assert proc.returncode == 0
