from observability.invariant_guard import evaluate_invariants, has_invariant_violations


def test_evaluate_invariants_disabled_by_default():
    state = {"runtime": {"status": "BROKEN"}, "metrics": {"inflight_count": -1}}

    violations = evaluate_invariants(state)

    assert violations == []
    assert has_invariant_violations(state) is False


def test_evaluate_invariants_enabled_reports_default_violations():
    state = {"runtime": {"status": "BROKEN"}, "metrics": {"inflight_count": -1}}

    violations = evaluate_invariants(state, enabled=True)

    assert [item.code for item in violations] == ["runtime_status_known", "metrics_non_negative"]
    assert has_invariant_violations(state, enabled=True) is True


def test_evaluate_invariants_accepts_custom_callable_checks_without_side_effects():
    calls = {"count": 0}

    def _always_fail(snapshot):
        calls["count"] += 1
        return snapshot.get("ok") is True

    state = {"ok": False}

    violations = evaluate_invariants(
        state,
        enabled=True,
        checks=(("custom_invariant", "custom invariant must pass", _always_fail),),
    )

    assert calls["count"] == 1
    assert len(violations) == 1
    assert violations[0].code == "custom_invariant"
