from __future__ import annotations

import pytest

from shutdown_manager import ShutdownManager


def test_shutdown_executes_hooks_by_priority_then_name_deterministically():
    manager = ShutdownManager()
    calls: list[str] = []

    manager.register("zeta", lambda: calls.append("zeta"), priority=20)
    manager.register("alpha", lambda: calls.append("alpha"), priority=10)
    manager.register("beta", lambda: calls.append("beta"), priority=10)

    result = manager.shutdown()

    assert calls == ["alpha", "beta", "zeta"]
    assert [row["name"] for row in result] == ["alpha", "beta", "zeta"]
    assert all(row["ok"] for row in result)


def test_shutdown_is_idempotent_after_first_run():
    manager = ShutdownManager()
    calls: list[str] = []
    manager.register("once", lambda: calls.append("once"))

    first = manager.shutdown()
    second = manager.shutdown()

    assert calls == ["once"]
    assert len(first) == 1
    assert second == []


def test_shutdown_with_no_hooks_is_noop_and_marks_run_state():
    manager = ShutdownManager()

    result = manager.shutdown()

    assert result == []
    status = manager.status()
    assert status["registered_hooks"] == 0
    assert status["has_run"] is True
    assert status["hooks"] == []


def test_shutdown_contains_exception_and_continues_next_hooks():
    manager = ShutdownManager()
    calls: list[str] = []

    def failing():
        calls.append("fail")
        raise RuntimeError("boom")

    manager.register("a_ok", lambda: calls.append("a_ok"), priority=5)
    manager.register("b_fail", failing, priority=10)
    manager.register("c_ok", lambda: calls.append("c_ok"), priority=20)

    result = manager.shutdown()

    assert calls == ["a_ok", "fail", "c_ok"]
    assert result == [
        {"name": "a_ok", "ok": True, "error": ""},
        {"name": "b_fail", "ok": False, "error": "boom"},
        {"name": "c_ok", "ok": True, "error": ""},
    ]


def test_register_rejects_non_callable_and_rich_api_rejects_invalid_signature():
    manager = ShutdownManager()

    with pytest.raises(TypeError):
        manager.register("bad", "not-callable")

    with pytest.raises(TypeError):
        manager.register_shutdown_hook()

    with pytest.raises(TypeError):
        manager.register_shutdown_hook("name-only")


def test_register_shutdown_hook_accepts_callable_form_and_same_name_replaces_old_hook():
    manager = ShutdownManager()
    calls: list[str] = []

    def hook():
        calls.append("new")

    manager.register("hook", lambda: calls.append("old"), priority=100)
    manager.register_shutdown_hook("hook", hook, priority=5)

    snapshot = manager.snapshot()
    assert snapshot == [{"name": "hook", "priority": 5}]

    manager.shutdown()
    assert calls == ["new"]


def test_register_shutdown_hook_accepts_required_arg_callable_and_fails_at_shutdown():
    manager = ShutdownManager()
    calls: list[str] = []

    def hook_with_required_arg(arg):
        calls.append(arg)

    manager.register_shutdown_hook(hook_with_required_arg)
    result = manager.shutdown()

    assert calls == []
    assert len(result) == 1
    assert result[0]["name"] == "hook_with_required_arg"
    assert result[0]["ok"] is False
    assert "required positional argument" in result[0]["error"]


def test_clear_resets_has_run_and_allows_fresh_shutdown_without_cross_instance_leakage():
    manager_a = ShutdownManager()
    calls_a: list[str] = []
    manager_a.register("a", lambda: calls_a.append("a"))

    manager_b = ShutdownManager()
    calls_b: list[str] = []
    manager_b.register("b", lambda: calls_b.append("b"))

    manager_a.shutdown()
    manager_a.clear()
    manager_a.register("a2", lambda: calls_a.append("a2"))

    a_second = manager_a.shutdown()
    b_first = manager_b.shutdown()

    assert calls_a == ["a", "a2"]
    assert [row["name"] for row in a_second] == ["a2"]
    assert calls_b == ["b"]
    assert [row["name"] for row in b_first] == ["b"]
