"""Tests for complete-task post-merge task-file handling."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from unittest import TestCase


ASSERTIONS = TestCase()
REPO_ROOT = Path(__file__).resolve().parents[2]


def load_module(repo_root: Path) -> ModuleType:
    """Load complete_current_task module under test."""
    module_path = repo_root / ".github" / "scripts" / "complete_current_task.py"
    spec = importlib.util.spec_from_file_location("complete_current_task_under_test", module_path)
    ASSERTIONS.assertIsNotNone(spec)
    ASSERTIONS.assertIsNotNone(spec.loader)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_output_flag(output_file: Path) -> str:
    """Read the task_moved output line from a GitHub output file."""
    return output_file.read_text(encoding="utf-8").strip()


def test_missing_marker_skips(tmp_path: Path, monkeypatch, capsys) -> None:
    """No Task-File marker should skip successfully without moving files."""
    output_file = tmp_path / "github_output.txt"
    monkeypatch.setenv("PR_BODY", "No task marker here")
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_file))
    module = load_module(REPO_ROOT)
    return_code_value = module.main()

    captured = capsys.readouterr()
    ASSERTIONS.assertEqual(return_code_value, 0)
    ASSERTIONS.assertIn("Skip", captured.out)
    ASSERTIONS.assertFalse((tmp_path / "ops" / "tasks_done").exists())
    ASSERTIONS.assertEqual(read_output_flag(output_file), "task_moved=false")


def test_missing_file_fails(tmp_path: Path, monkeypatch, capsys) -> None:
    """A Task-File marker pointing to a missing file should fail closed."""
    monkeypatch.chdir(tmp_path)
    output_file = tmp_path / "github_output.txt"
    monkeypatch.setenv("PR_BODY", "Task-File: ops/tasks/missing.md")
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_file))
    module = load_module(REPO_ROOT)
    return_code_value = module.main()

    captured = capsys.readouterr()
    ASSERTIONS.assertEqual(return_code_value, 1)
    ASSERTIONS.assertIn("Task file does not exist", captured.err)
    ASSERTIONS.assertEqual(read_output_flag(output_file), "task_moved=false")


def test_existing_file_moves(tmp_path: Path, monkeypatch) -> None:
    """A valid Task-File marker should move the task file to tasks_done."""
    monkeypatch.chdir(tmp_path)
    task_dir = tmp_path / "ops" / "tasks"
    task_dir.mkdir(parents=True)
    source = task_dir / "task.md"
    source.write_text("task body", encoding="utf-8")
    output_file = tmp_path / "github_output.txt"
    monkeypatch.setenv("PR_BODY", "Task-File: ops/tasks/task.md")
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_file))

    module = load_module(REPO_ROOT)
    return_code_value = module.main()

    destination = tmp_path / "ops" / "tasks_done" / "task.md"
    ASSERTIONS.assertEqual(return_code_value, 0)
    ASSERTIONS.assertFalse(source.exists())
    ASSERTIONS.assertEqual(destination.read_text(encoding="utf-8"), "task body")
    ASSERTIONS.assertEqual(read_output_flag(output_file), "task_moved=true")
