from __future__ import annotations

import importlib.util
import os
from pathlib import Path


SCRIPT_PATH = Path(".github/scripts/complete_current_task.py").resolve()


def _load_module_with_pr_body(pr_body: str):
    spec = importlib.util.spec_from_file_location(
        "complete_current_task_test_module",
        SCRIPT_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    os.environ["PR_BODY"] = pr_body
    spec.loader.exec_module(module)
    return module


def test_no_task_file_marker_skips_with_success(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)

    mod = _load_module_with_pr_body("Summary only; no marker")
    rc = mod.main()

    out = capsys.readouterr()
    assert rc == 0
    assert "Skipping task completion: no Task-File marker found in PR body" in out.out
    assert not (tmp_path / "ops" / "tasks_done").exists()


def test_marker_present_but_file_missing_fails(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)

    mod = _load_module_with_pr_body("Task-File: ops/tasks/missing-task.md")
    rc = mod.main()

    out = capsys.readouterr()
    assert rc == 1
    assert "Task file does not exist: ops/tasks/missing-task.md" in out.err


def test_marker_present_and_file_exists_moves_and_succeeds(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    src = tmp_path / "ops" / "tasks" / "task-a.md"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("# task", encoding="utf-8")

    mod = _load_module_with_pr_body("Task-File: ops/tasks/task-a.md")
    rc = mod.main()

    out = capsys.readouterr()
    dst = tmp_path / "ops" / "tasks_done" / "task-a.md"
    assert rc == 0
    assert not src.exists()
    assert dst.exists()
    assert f"Moved ops/tasks/task-a.md -> ops/tasks_done/task-a.md" in out.out
