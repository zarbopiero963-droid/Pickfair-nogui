import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "guardrail_check.py"


def _run_guard(
    tmp_path: Path,
    title: str,
    *,
    changed_files: list[dict] | None = None,
) -> subprocess.CompletedProcess[str]:
    (tmp_path / ".guardrails").mkdir()
    (tmp_path / "pr_meta.json").write_text(
        json.dumps({"title": title, "body": ""}),
        encoding="utf-8",
    )
    files = changed_files or [
        {"filename": ".github/workflows/pr-guard.yml"},
        {"filename": "scripts/guardrail_check.py"},
        {"filename": ".guardrails/allowed_scope.json"},
        {"filename": "tests/guardrails/test_pr_guard_fail_closed.py"},
    ]
    (tmp_path / "pr_files_raw.json").write_text(
        json.dumps(files),
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


def test_missing_task_non_critical_fails_closed(tmp_path: Path):
    result = _run_guard(tmp_path, title="Guardrail update")
    assert result.returncode != 0
    assert "TASK validation is fail-closed" in result.stdout


def test_missing_task_critical_fails(tmp_path: Path):
    result = _run_guard(
        tmp_path,
        title="Critical runtime update",
        changed_files=[{"filename": "core/runtime_controller.py"}],
    )
    assert result.returncode != 0


def test_unknown_task_non_critical_fails_closed(tmp_path: Path):
    result = _run_guard(tmp_path, title="[TASK: not_real] Guardrail update")
    assert result.returncode != 0
    assert "Unknown TASK tag" in result.stdout


def test_valid_task_passes(tmp_path: Path):
    result = _run_guard(tmp_path, title="[TASK: pr_guard] Guardrail update")
    assert result.returncode == 0
    assert "✅ PR guard completed" in result.stdout


def test_workflow_hygiene_task_passes(tmp_path: Path):
    result = _run_guard(tmp_path, title="[TASK: workflow_hygiene_pr1_comment_noise] Guardrail update")
    assert result.returncode == 0
    assert "TASK source found" in result.stdout


def test_workflow_hygiene_task_mixed_case_passes(tmp_path: Path):
    result = _run_guard(tmp_path, title="[TASK: WorkFlow_Hygiene_PR1_Comment_Noise] Guardrail update")
    assert result.returncode == 0


def test_unknown_non_workflow_hygiene_task_still_fails_closed(tmp_path: Path):
    result = _run_guard(tmp_path, title="[TASK: workflow_hygieneX_pr1] Guardrail update")
    assert result.returncode != 0
    assert "Unknown TASK tag" in result.stdout


def test_claude_bug_task_passes(tmp_path: Path):
    result = _run_guard(
        tmp_path,
        title="[TASK: claude_bug_pr1a_telegram_sender_escape_queue] Guardrail update",
    )
    assert result.returncode == 0
    assert "TASK source found" in result.stdout


def test_claude_bug_task_mixed_case_passes(tmp_path: Path):
    result = _run_guard(
        tmp_path,
        title="[TASK: Claude_Bug_PR1A_Telegram_Sender_Escape_Queue] Guardrail update",
    )
    assert result.returncode == 0


def test_unknown_claude_bug_like_task_fails_closed(tmp_path: Path):
    result = _run_guard(
        tmp_path,
        title="[TASK: claude_bug_prx_telegram_sender_escape_queue] Guardrail update",
    )
    assert result.returncode != 0
    assert "Unknown TASK tag" in result.stdout
