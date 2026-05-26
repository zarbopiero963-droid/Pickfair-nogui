"""Tests for complete-task post-merge task-file handling."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from types import ModuleType
from unittest import TestCase


ASSERTIONS = TestCase()
REPO_ROOT = Path(__file__).resolve().parents[2]


def load_module(pr_body: str, repo_root: Path) -> ModuleType:
    """Load complete_current_task with an isolated PR_BODY value."""
    os.environ["PR_BODY"] = pr_body
    module_path = repo_root / ".github" / "scripts" / "complete_current_task.py"
    spec = importlib.util.spec_from_file_location("complete_current_task_under_test", module_path)
    ASSERTIONS.assertIsNotNone(spec)
    ASSERTIONS.assertIsNotNone(spec.loader)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_missing_marker_skips(tmp_path: Path, capsys) -> None:
    """No Task-File marker should skip successfully without moving files."""
    module = load_module("No task marker here", REPO_ROOT)
    return_code = module.main()

    captured = capsys.readouterr()
    ASSERTIONS.assertEqual(return_code, 0)
    ASSERTIONS.assertIn("Skip", captured.out)
    ASSERTIONS.assertFalse((tmp_path / "ops" / "tasks_done").exists())


def test_missing_file_fails(tmp_path: Path, monkeypatch, capsys) -> None:
    """A Task-File marker pointing to a missing file should fail closed."""
    monkeypatch.chdir(tmp_path)
    module = load_module("Task-File: ops/tasks/missing.md", REPO_ROOT)
    return_code = module.main()

    captured = capsys.readouterr()
    ASSERTIONS.assertEqual(return_code, 1)
    ASSERTIONS.assertIn("Task file does not exist", captured.err)


def test_existing_file_moves(tmp_path: Path, monkeypatch) -> None:
    """A valid Task-File marker should move the task file to tasks_done."""
    monkeypatch.chdir(tmp_path)
    task_dir = tmp_path / "ops" / "tasks"
    task_dir.mkdir(parents=True)
    source = task_dir / "task.md"
    source.write_text("task body", encoding="utf-8")

    module = load_module("Task-File: ops/tasks/task.md", REPO_ROOT)
    return_code = module.main()

    destination = tmp_path / "ops" / "tasks_done" / "task.md"
    ASSERTIONS.assertEqual(return_code, 0)
    ASSERTIONS.assertFalse(source.exists())
    ASSERTIONS.assertEqual(destination.read_text(encoding="utf-8"), "task body")
