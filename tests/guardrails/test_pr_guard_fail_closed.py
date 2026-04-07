import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "guardrail_check.py"


def _run_guard(tmp_path: Path, title: str) -> subprocess.CompletedProcess[str]:
    (tmp_path / ".guardrails").mkdir()
    (tmp_path / "pr_meta.json").write_text(
        json.dumps({"title": title, "body": ""}),
        encoding="utf-8",
    )
    (tmp_path / "pr_files_raw.json").write_text(
        json.dumps(
            [
                {"filename": ".github/workflows/pr-guard.yml"},
                {"filename": "scripts/guardrail_check.py"},
                {"filename": ".guardrails/allowed_scope.json"},
                {"filename": "tests/guardrails/test_pr_guard_fail_closed.py"},
            ]
        ),
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
        [sys.executable, str(SCRIPT_PATH)],
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
