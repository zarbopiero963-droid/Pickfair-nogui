"""Gate A T2b batch b4 — .github task queue scripts.

Offline: gh is never executed; the two scripts are pure-Python file operations, loaded from
.github/scripts and run against a tmp_path working directory (chdir), GITHUB_OUTPUT redirected."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load(name: str):
    path = REPO_ROOT / ".github" / "scripts" / name
    spec = importlib.util.spec_from_file_location(f"b4_{path.stem}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_trg_628_b_1050(tmp_path, monkeypatch):
    """ID: TRG-628-B-1050 / PF-B1-R2-QUEUE-MOVE

    Con marker Task-File valido sposta il file per basename in ops/tasks_done e scrive task_moved=true;
    senza marker task_moved=false; path fuori ops/tasks => exit 1, nessuno spostamento."""
    mod = _load("complete_current_task.py")
    monkeypatch.chdir(tmp_path)
    (tmp_path / "ops" / "tasks").mkdir(parents=True)
    task = tmp_path / "ops" / "tasks" / "010-foo.md"
    task.write_text("body", encoding="utf-8")
    out = tmp_path / "gh_out"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))

    monkeypatch.setenv("PR_BODY", "x\nTask-File: ops/tasks/010-foo.md\n")
    assert mod.main() == 0
    assert not task.exists()
    assert (tmp_path / "ops" / "tasks_done" / "010-foo.md").read_text(encoding="utf-8") == "body"
    assert out.read_text(encoding="utf-8").splitlines()[-1] == "task_moved=true"

    monkeypatch.setenv("PR_BODY", "no marker")
    assert mod.main() == 0
    assert out.read_text(encoding="utf-8").splitlines()[-1] == "task_moved=false"

    (tmp_path / "ops" / "secret.md").write_text("s", encoding="utf-8")
    monkeypatch.setenv("PR_BODY", "Task-File: ops/tasks/../secret.md")
    assert mod.main() == 1
    assert (tmp_path / "ops" / "secret.md").exists()
    assert not (tmp_path / "ops" / "tasks_done" / "secret.md").exists()
    assert out.read_text(encoding="utf-8").splitlines()[-1] == "task_moved=false"


def test_trg_628_b_1051(tmp_path, monkeypatch, capsys):
    """ID: TRG-628-B-1051 / PF-B1-R2-QUEUE-NEXT

    Sceglie il primo file .md immediato (non ricorsivo) in ordine lessicografico; ignora non-.md e
    sottocartelle; coda vuota => found false."""
    mod = _load("next_task.py")
    monkeypatch.chdir(tmp_path)
    tasks = tmp_path / "ops" / "tasks"
    tasks.mkdir(parents=True)
    assert mod.main() == 0
    assert json.loads(capsys.readouterr().out) == {"found": False}

    for name in ("020-b.md", "010-a.md", "005-z.txt", ".gitkeep"):
        (tasks / name).write_text("x", encoding="utf-8")
    (tasks / "000-sub").mkdir()
    (tasks / "000-sub" / "000-nested.md").write_text("x", encoding="utf-8")
    assert mod.main() == 0
    got = json.loads(capsys.readouterr().out)
    assert got == {"found": True, "path": str(Path("ops/tasks/010-a.md")), "name": "010-a.md"}
