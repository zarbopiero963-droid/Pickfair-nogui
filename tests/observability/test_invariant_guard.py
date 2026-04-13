from observability.invariant_guard import evaluate_invariants, has_invariant_violations, DEFAULT_INVARIANT_CHECKS


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


def test_default_invariant_checks_exported():
    """DEFAULT_INVARIANT_CHECKS is exported and is the same as the internal default."""
    assert DEFAULT_INVARIANT_CHECKS is not None
    assert len(DEFAULT_INVARIANT_CHECKS) >= 9
    codes = {code for code, _, _ in DEFAULT_INVARIANT_CHECKS}
    assert "runtime_status_known" in codes
    assert "terminal_to_nonterminal_regression" in codes
    assert "failed_local_remote_exists" in codes


def test_failed_local_remote_exists_catches_ghost():
    """failed_local_remote_exists fires when a FAILED order has a remote_bet_id."""
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 0},
        "recent_orders": [
            {"order_id": "o1", "status": "FAILED", "remote_bet_id": "ext-1"},
        ],
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    assert "failed_local_remote_exists" in codes


def test_failed_local_remote_exists_pass_on_clean_state():
    """failed_local_remote_exists does not fire for normal failed orders without remote id."""
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 0},
        "recent_orders": [
            {"order_id": "o1", "status": "FAILED", "remote_bet_id": None},
        ],
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    assert "failed_local_remote_exists" not in codes


def test_terminal_to_nonterminal_regression_catches_bad_transition():
    """terminal_to_nonterminal_regression fires when a completed order reverts to non-terminal."""
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 0},
        "recent_orders": [
            {"order_id": "o1", "prev_status": "COMPLETED", "status": "PENDING"},
        ],
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    assert "terminal_to_nonterminal_regression" in codes


def test_terminal_to_nonterminal_regression_pass_on_normal_transition():
    """terminal_to_nonterminal_regression does not fire for normal terminal→terminal."""
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 0},
        "recent_orders": [
            {"order_id": "o1", "prev_status": "COMPLETED", "status": "FAILED"},
        ],
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    assert "terminal_to_nonterminal_regression" not in codes


def test_inflight_too_old_fires():
    """inflight_too_old fires when an INFLIGHT order exceeds the max age."""
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 1},
        "max_inflight_age_sec": 300,
        "recent_orders": [
            {"order_id": "o1", "status": "INFLIGHT", "age_sec": 400},
        ],
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    assert "inflight_too_old" in codes


def test_local_exposure_remote_exposure_mismatch_fires():
    """local_exposure_remote_exposure_mismatch fires when exposure delta exceeds tolerance."""
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 0},
        "risk": {"local_exposure": 100.0, "remote_exposure": 200.0, "exposure_tolerance": 0.01},
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    assert "local_exposure_remote_exposure_mismatch" in codes


def test_duplicate_blocked_but_remote_executed_fires():
    """duplicate_blocked_but_remote_executed fires when a blocked order has a remote id."""
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 0},
        "recent_orders": [
            {"order_id": "o1", "status": "DUPLICATE_BLOCKED", "remote_bet_id": "ext-77"},
        ],
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    assert "duplicate_blocked_but_remote_executed" in codes


def test_finalized_state_inconsistent_fires():
    """finalized_state_inconsistent_with_audit_or_exchange_evidence fires on audit mismatch."""
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 0},
        "recent_orders": [
            {"order_id": "o1", "status": "COMPLETED", "audit_status": "FAILED"},
        ],
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    assert "finalized_state_inconsistent_with_audit_or_exchange_evidence" in codes


def test_ambiguous_local_remote_inconsistency_fires():
    """ambiguous_local_remote_inconsistency fires when an AMBIGUOUS order has a definitive remote state."""
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 0},
        "recent_orders": [
            {"order_id": "o1", "status": "AMBIGUOUS", "remote_final_status": "SETTLED_WIN"},
        ],
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    assert "ambiguous_local_remote_inconsistency" in codes


def test_new_checks_are_fail_safe_on_empty_orders():
    """All new invariant checks that use recent_orders pass (no violation) when orders list is empty."""
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 0},
        "recent_orders": [],
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    # None of the order-based checks should fire on empty orders
    assert "failed_local_remote_exists" not in codes
    assert "terminal_to_nonterminal_regression" not in codes
    assert "inflight_too_old" not in codes
    assert "ambiguous_local_remote_inconsistency" not in codes
    assert "duplicate_blocked_but_remote_executed" not in codes
    assert "finalized_state_inconsistent_with_audit_or_exchange_evidence" not in codes
