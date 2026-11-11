"""Gate A batch C: .github task-queue automation (scripts + workflows).

Offline only: the gh CLI is never invoked (subprocess.run is monkeypatched);
workflows are read statically with yaml, and the one shell guard is executed
locally with bash exactly as written in the workflow.
"""
from __future__ import annotations

import importlib.util
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"


def _load_script(name: str, module_name: str):
    path = REPO_ROOT / ".github" / "scripts" / name
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _workflow(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# TRG-628-B-1052: create_next_task_issue.py
# ---------------------------------------------------------------------------
def test_trg_628_b_1052(tmp_path, monkeypatch):
    """ID: TRG-628-B-1052 / PF-B1-R2-QUEUE-ISSUE

    Builds the issue body (header + Task-File marker + task body) and invokes
    `gh issue create --title ... --body ...` with check=True (a gh failure
    fails the step). gh itself is stubbed: no network.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / "ops" / "templates").mkdir(parents=True)
    (tmp_path / "ops" / "templates" / "task_header.md").write_text("HEADER-LINE\n", encoding="utf-8")
    (tmp_path / "ops" / "tasks").mkdir(parents=True)
    (tmp_path / "ops" / "tasks" / "next.md").write_text("\nTASK-BODY\n", encoding="utf-8")
    monkeypatch.setenv("TASK_PATH", "ops/tasks/next.md")

    module = _load_script("create_next_task_issue.py", "gate_a_create_next_task_issue")
    calls = []

    def _fake_run(args, **kwargs):
        calls.append((list(args), dict(kwargs)))
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(module.subprocess, "run", _fake_run)
    assert module.main() == 0

    assert len(calls) == 1
    args, kwargs = calls[0]
    assert args[:3] == ["gh", "issue", "create"]
    assert args[3] == "--title" and args[4] == "Next Codex task ready: next.md"
    assert args[5] == "--body"
    body = args[6]
    assert "HEADER-LINE" in body
    assert "Task-File: ops/tasks/next.md" in body
    assert "TASK-BODY" in body
    assert body.index("HEADER-LINE") < body.index("Task-File:") < body.index("TASK-BODY")
    assert kwargs.get("check") is True

    # gh failure propagates (check=True) instead of being swallowed.
    def _failing_run(args, **kwargs):
        if kwargs.get("check"):
            raise subprocess.CalledProcessError(1, args)
        return subprocess.CompletedProcess(args, 1)

    monkeypatch.setattr(module.subprocess, "run", _failing_run)
    with pytest.raises(subprocess.CalledProcessError):
        module.main()

    # missing task file => exit 1, gh never called.
    calls.clear()
    monkeypatch.setattr(module.subprocess, "run", _fake_run)
    (tmp_path / "ops" / "tasks" / "next.md").unlink()
    assert module.main() == 1
    assert calls == []


# ---------------------------------------------------------------------------
# TRG-628-B-1053: queue-on-pr-closed.yml manual recovery input guard
# ---------------------------------------------------------------------------
def _recovery_guard_script() -> str:
    wf = _workflow("queue-on-pr-closed.yml")
    steps = wf["jobs"]["advance"]["steps"]
    (step,) = [s for s in steps if s.get("name") == "Validate manual recovery task_file input"]
    return step["run"]


def _run_guard(task_file: str) -> int:
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    proc = subprocess.run([bash, "-c", _recovery_guard_script()],
                          env={"TASK_FILE": task_file, "PATH": "/usr/bin:/bin"},
                          capture_output=True, text=True, timeout=10, check=False)
    return proc.returncode


@pytest.mark.parametrize("bad", ["", "ops/tasks/../x.md", "ops/tasks/a/b.md", "other/x.md",
                                 "ops/tasks/x.txt", "x.md"])
def test_trg_628_b_1053_rejects_invalid(bad):
    """ID: TRG-628-B-1053 / PF-B1-R2-QUEUE-RECOVERY

    The recovery guard rejects empty, '..', nested and non-ops/tasks/*.md inputs.
    """
    assert _run_guard(bad) != 0


@pytest.mark.xfail(strict=True, reason="TRG-628-B-1053: case */*/* matches ops/tasks/<name>.md before "
                                       "the ops/tasks/*.md allowlist; every valid recovery input is rejected")
def test_trg_628_b_1053():
    """ID: TRG-628-B-1053 / PF-B1-R2-QUEUE-RECOVERY

    The workflow's own contract (input description 'e.g. ops/tasks/example.md',
    error 'task_file must match ops/tasks/<name>.md') says a flat
    ops/tasks/<name>.md is the accepted form. In bash `case`, `*` also matches
    '/', so the earlier `*/*/*` arm rejects exactly that form.
    """
    assert _run_guard("ops/tasks/example.md") == 0


# ---------------------------------------------------------------------------
# TRG-628-B-1054: complete-task-on-merge.yml (declarative wiring) + script output
# ---------------------------------------------------------------------------
def test_trg_628_b_1054(tmp_path, monkeypatch):
    """ID: TRG-628-B-1054 / PF-B1-R2-QUEUE-WORKFLOW

    Static (declarative) check of the workflow: complete-task runs the move
    step (id move_task) then commit+push, exports task_moved from that step;
    prepare-next needs complete-task and runs only when it succeeded AND
    task_moved == 'true'. Plus the producer side: the script writes
    task_moved=false when nothing is moved and task_moved=true after a move.
    """
    wf = _workflow("complete-task-on-merge.yml")
    complete = wf["jobs"]["complete-task"]
    assert complete["if"].strip() == "github.event.pull_request.merged == true"
    assert complete["outputs"]["task_moved"].replace(" ", "") == "${{steps.move_task.outputs.task_moved}}"
    names = [s.get("name") for s in complete["steps"]]
    move_idx = next(i for i, s in enumerate(complete["steps"]) if s.get("id") == "move_task")
    assert "complete_current_task.py" in complete["steps"][move_idx]["run"]
    commit_idx = names.index("Commit queue update")
    assert commit_idx > move_idx
    commit_run = complete["steps"][commit_idx]["run"]
    assert "git commit" in commit_run and "git push" in commit_run

    nxt = wf["jobs"]["prepare-next"]
    assert nxt["needs"] == "complete-task"
    cond = nxt["if"].replace(" ", "")
    assert "needs.complete-task.result=='success'" in cond
    assert "needs.complete-task.outputs.task_moved=='true'" in cond
    assert "&&" in cond and "||" not in cond

    # Producer side of the output.
    module = _load_script("complete_current_task.py", "gate_a_complete_current_task")
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "gh_output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))
    monkeypatch.setenv("PR_BODY", "no marker")
    assert module.main() == 0
    assert out.read_text(encoding="utf-8").strip() == "task_moved=false"
    (tmp_path / "ops" / "tasks").mkdir(parents=True)
    (tmp_path / "ops" / "tasks" / "t.md").write_text("x", encoding="utf-8")
    out.unlink()
    monkeypatch.setenv("PR_BODY", "Task-File: ops/tasks/t.md")
    assert module.main() == 0
    assert out.read_text(encoding="utf-8").strip() == "task_moved=true"
    assert (tmp_path / "ops" / "tasks_done" / "t.md").exists()
