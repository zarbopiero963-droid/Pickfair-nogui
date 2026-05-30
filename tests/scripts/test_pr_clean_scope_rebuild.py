"""Tests for clean-scope rebuild post-fix gate behavior."""

from __future__ import annotations

from unittest import TestCase

from scripts import pr_clean_scope_rebuild as rebuild

ASSERTIONS = TestCase()


def _args(ctx: dict | None, decision_out: str) -> rebuild.RebuildArgs:
    return rebuild.RebuildArgs(
        repo="owner/repo",
        pr_number="249",
        branch="chore/pr3h-post-fix-audit-gate",
        allowlist=["scripts/pr_clean_scope_rebuild.py"],
        forbidden=[],
        decision_out=decision_out,
        post_fix_gate_context=ctx,
        dry_run=False,
    )


def _good_ctx(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "automation_mode": "live",
        "post_fix_audit": "PASS",
        "validation_passed": True,
        "current_head_matches": True,
        "scope_allowed": True,
        "rollback_attempted": False,
        "rollback_succeeded": True,
        "dirty_worktree": False,
        "task_no_commit_push": False,
        "can_commit": True,
        "can_push": True,
    }
    base.update(overrides)
    return base


def _run_calls(monkeypatch, head: str = "abc123\n") -> list[list[str]]:
    calls: list[list[str]] = []

    def fake_run(cmd, check=True):
        ASSERTIONS.assertTrue(check)
        calls.append(list(cmd))
        if cmd[:3] == ["git", "rev-parse", "HEAD"]:
            return 0, head
        return 0, ""

    monkeypatch.setattr(rebuild, "run", fake_run)
    return calls


def _stub_execute_rebuild(monkeypatch, calls: list[list[str]], head: str = "newhead123\n") -> None:
    monkeypatch.setattr(rebuild, "prepare_scope", lambda _args, _decision: (["scripts/pr_clean_scope_rebuild.py"], []))
    monkeypatch.setattr(rebuild, "stop_for_scope_blockers", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        rebuild,
        "fetch_heads",
        lambda _args, _decision: rebuild.CleanBranches("abc123", "backup/pr-249-before-clean", "clean/pr-249-scope"),
    )

    def fake_run(cmd, check=True):
        ASSERTIONS.assertTrue(check)
        calls.append(list(cmd))
        return 0, head if cmd[:3] == ["git", "rev-parse", "HEAD"] else ""

    monkeypatch.setattr(rebuild, "run", fake_run)
    monkeypatch.setattr(rebuild, "restore_files", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(rebuild, "run_diff_guard", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(rebuild, "run_compile_guard", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(rebuild, "run_focused_tests", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(rebuild, "stop_if_forbidden_remains", lambda *_args, **_kwargs: False)


def test_commit_blocked_no_gate(monkeypatch, tmp_path):
    """Missing invocation context blocks commit and push."""
    calls = _run_calls(monkeypatch)
    decision_out = str(tmp_path / "decision.json")
    decision = rebuild.initial_decision(_args(None, decision_out))
    rebuild.commit_and_push(_args(None, decision_out), decision, ["scripts/pr_clean_scope_rebuild.py"])
    ASSERTIONS.assertNotIn(["git", "commit", "-m", "Clean rebuild PR 249 scope"], calls)
    ASSERTIONS.assertEqual(decision["final_status"], "blocked_post_fix_audit_gate")


def test_push_blocked_gate(monkeypatch, tmp_path):
    """Push gate denial prevents force-with-lease."""
    calls = _run_calls(monkeypatch)
    decision_out = str(tmp_path / "decision.json")
    decision = rebuild.initial_decision(_args(_good_ctx(can_push=False), decision_out))
    rebuild.commit_and_push(
        _args(_good_ctx(can_push=False), decision_out), decision, ["scripts/pr_clean_scope_rebuild.py"]
    )
    ASSERTIONS.assertNotIn(["git", "commit", "-m", "Clean rebuild PR 249 scope"], calls)
    ASSERTIONS.assertFalse(any(cmd[:3] == ["git", "push", "--force-with-lease"] for cmd in calls))


def test_commit_push_allowed(monkeypatch, tmp_path):
    """Full explicit context allows mocked commit and push."""
    calls = _run_calls(monkeypatch, head="def456\n")
    decision_out = str(tmp_path / "decision.json")
    decision = rebuild.initial_decision(_args(_good_ctx(), decision_out))
    rebuild.commit_and_push(_args(_good_ctx(), decision_out), decision, ["scripts/pr_clean_scope_rebuild.py"])
    ASSERTIONS.assertIn(["git", "commit", "-m", "Clean rebuild PR 249 scope"], calls)
    ASSERTIONS.assertTrue(any(cmd[:3] == ["git", "push", "--force-with-lease"] for cmd in calls))
    ASSERTIONS.assertEqual(decision["final_status"], "success")


def test_context_beats_ambient_env(monkeypatch, tmp_path):
    """Explicit invocation context has precedence over ambient environment."""
    _ = _run_calls(monkeypatch, head="aaa111\n")
    monkeypatch.setenv("POST_FIX_AUDIT", "FAIL")
    decision_out = str(tmp_path / "decision.json")
    decision = rebuild.initial_decision(_args(_good_ctx(), decision_out))
    rebuild.commit_and_push(_args(_good_ctx(), decision_out), decision, ["scripts/pr_clean_scope_rebuild.py"])
    ASSERTIONS.assertEqual(decision["final_status"], "success")


def test_backup_push_denied(monkeypatch, tmp_path):
    """Backup branch push is skipped when push gate is denied."""
    decision_out, calls = tmp_path / "decision.json", []
    args = _args(_good_ctx(can_push=False), str(decision_out))
    decision = rebuild.initial_decision(args)
    _stub_execute_rebuild(monkeypatch, calls, head="abc123\n")
    monkeypatch.setattr(rebuild, "commit_and_push", lambda *_args, **_kwargs: None)
    ASSERTIONS.assertEqual(rebuild.execute_rebuild(args, decision, decision_out), 0)
    ASSERTIONS.assertEqual(decision["final_status"], "blocked_post_fix_audit_gate")
    ASSERTIONS.assertFalse(any(cmd[:3] == ["git", "push", "origin"] for cmd in calls))


def test_backup_push_allowed_gate(monkeypatch, tmp_path):
    """Backup branch push runs when gate conditions allow push."""
    decision_out, calls = tmp_path / "decision.json", []
    args = _args(_good_ctx(), str(decision_out))
    decision = rebuild.initial_decision(args)
    _stub_execute_rebuild(monkeypatch, calls)
    monkeypatch.setattr(rebuild, "ensure_git_identity", lambda: None)
    ASSERTIONS.assertEqual(rebuild.execute_rebuild(args, decision, decision_out), 0)
    ASSERTIONS.assertEqual(decision["final_status"], "success")
    ASSERTIONS.assertIn(
        ["git", "push", "origin", "origin/chore/pr3h-post-fix-audit-gate:refs/heads/backup/pr-249-before-clean"],
        calls,
    )
