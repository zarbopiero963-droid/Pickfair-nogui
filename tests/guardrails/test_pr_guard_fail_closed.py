import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "guardrail_check.py"


def _run_guard(tmp_path: Path, title: str):
    (tmp_path / ".guardrails").mkdir()
    (tmp_path / "pr_meta.json").write_text(json.dumps({"title": title, "body": ""}), encoding="utf-8")
    (tmp_path / "pr_files_raw.json").write_text(
        json.dumps([
            {"filename": ".github/workflows/pr-guard.yml"},
            {"filename": "scripts/guardrail_check.py"},
            {"filename": ".guardrails/allowed_scope.json"},
            {"filename": "tests/guardrails/test_pr_guard_fail_closed.py"},
        ]),
        encoding="utf-8",
    )
    (tmp_path / ".guardrails" / "allowed_scope.json").write_text(
        json.dumps(
            {
                "default": {"max_files": 8, "allow_tests": True},
                "tasks": {
                    "pr_guard": {
                        "files": [
                            ".github/workflows/pr-guard.yml",
                            "scripts/guardrail_check.py",
                            ".guardrails/allowed_scope.json",
                            "tests/guardrails/test_pr_guard_fail_closed.py",
                        ],
                        "max_files": 4,
                        "allow_tests": False,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    return subprocess.run(
        [sys.executable, str(SCRIPT)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )


def test_missing_task_fails(tmp_path: Path):
    result = _run_guard(tmp_path, title="Guardrail update")
    assert result.returncode != 0
    assert "Missing [TASK: ...]" in result.stdout


def test_unknown_task_fails(tmp_path: Path):
    result = _run_guard(tmp_path, title="[TASK: not_real] Guardrail update")
    assert result.returncode != 0
    assert "Unknown task" in result.stdout


def test_valid_task_passes(tmp_path: Path):
    result = _run_guard(tmp_path, title="[TASK: pr_guard] Guardrail update")
    assert result.returncode == 0
    assert "✅ Scope valid" in result.stdout
import runpy
from pathlib import Path

import pytest


SCRIPT_PATH = Path("scripts/guardrail_check.py")


def _write_json(path: Path, payload):
    path.write_text(json.dumps(payload), encoding="utf-8")


def _run_guardrail_in_tmp(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    return runpy.run_path(str(SCRIPT_PATH.resolve()), run_name="__main__")


def test_guardrail_fails_when_task_tag_is_missing(tmp_path, monkeypatch, capsys):
    guardrails_dir = tmp_path / ".guardrails"
    guardrails_dir.mkdir(parents=True)

    _write_json(
        tmp_path / "pr_meta.json",
        {
            "title": "Fix something without task tag",
            "body": "No explicit task here",
        },
    )
    _write_json(
        tmp_path / "pr_files_raw.json",
        [
            {"filename": "core/trading_engine.py"},
            {"filename": "tests/core/test_trading_engine.py"},
        ],
    )
    _write_json(
        guardrails_dir / "allowed_scope.json",
        {
            "default": {"max_files": 8, "allow_tests": True},
            "tasks": {
                "trading_engine": {
                    "files": ["core/trading_engine.py", "database.py"],
                    "max_files": 12,
                    "allow_tests": True,
                }
            },
        },
    )

    with pytest.raises(SystemExit) as exc:
        _run_guardrail_in_tmp(tmp_path, monkeypatch)

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "Cannot determine TASK" in out or "Missing [TASK:" in out


def test_guardrail_fails_when_task_tag_is_unknown(tmp_path, monkeypatch, capsys):
    guardrails_dir = tmp_path / ".guardrails"
    guardrails_dir.mkdir(parents=True)

    _write_json(
        tmp_path / "pr_meta.json",
        {
            "title": "[TASK: unknown_task] Some change",
            "body": "",
        },
    )
    _write_json(
        tmp_path / "pr_files_raw.json",
        [
            {"filename": "core/trading_engine.py"},
        ],
    )
    _write_json(
        guardrails_dir / "allowed_scope.json",
        {
            "default": {"max_files": 8, "allow_tests": True},
            "tasks": {
                "trading_engine": {
                    "files": ["core/trading_engine.py", "database.py"],
                    "max_files": 12,
                    "allow_tests": True,
                }
            },
        },
    )

    with pytest.raises(SystemExit) as exc:
        _run_guardrail_in_tmp(tmp_path, monkeypatch)

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "Unknown task: unknown_task" in out


def test_guardrail_passes_for_valid_task_and_in_scope_files(tmp_path, monkeypatch, capsys):
    guardrails_dir = tmp_path / ".guardrails"
    guardrails_dir.mkdir(parents=True)

    _write_json(
        tmp_path / "pr_meta.json",
        {
            "title": "[TASK: trading_engine] Fix semantic failure handling",
            "body": "",
        },
    )
    _write_json(
        tmp_path / "pr_files_raw.json",
        [
            {"filename": "core/trading_engine.py"},
            {"filename": "database.py"},
            {"filename": "tests/core/test_trading_engine.py"},
            {"filename": "tests/integration/test_trading_engine_failed_payload_semantics.py"},
        ],
    )
    _write_json(
        guardrails_dir / "allowed_scope.json",
        {
            "default": {"max_files": 8, "allow_tests": True},
            "tasks": {
                "trading_engine": {
                    "files": ["core/trading_engine.py", "database.py"],
                    "max_files": 12,
                    "allow_tests": True,
                }
            },
        },
    )

    _run_guardrail_in_tmp(tmp_path, monkeypatch)

    out = capsys.readouterr().out
    assert "Detected task: trading_engine" in out
    assert "Scope valid" in out
