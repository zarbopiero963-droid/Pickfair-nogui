import json
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