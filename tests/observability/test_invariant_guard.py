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
    # Required runtime-reviewer canonical codes must also be present.
    assert "FAILED_LOCAL_REMOTE_EXISTS" in codes
    assert "STATE_REGRESSION" in codes
    assert "INFLIGHT_STUCK" in codes
    assert "INVARIANT_EXPOSURE_MISMATCH" in codes


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
    # Required canonical codes must also not fire on empty orders
    assert "FAILED_LOCAL_REMOTE_EXISTS" not in codes
    assert "STATE_REGRESSION" not in codes
    assert "INFLIGHT_STUCK" not in codes
    assert "INVARIANT_EXPOSURE_MISMATCH" not in codes


# ---------------------------------------------------------------------------
# Required runtime-reviewer invariant codes — explicit assertion tests
# ---------------------------------------------------------------------------

def test_FAILED_LOCAL_REMOTE_EXISTS_fires_on_ghost_order():
    """FAILED_LOCAL_REMOTE_EXISTS fires when a FAILED order has a remote_bet_id."""
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 0},
        "recent_orders": [
            {"order_id": "o1", "status": "FAILED", "remote_bet_id": "ext-ghost-1"},
        ],
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    assert "FAILED_LOCAL_REMOTE_EXISTS" in codes


def test_FAILED_LOCAL_REMOTE_EXISTS_no_false_positive():
    """FAILED_LOCAL_REMOTE_EXISTS does not fire when no FAILED order has a remote id."""
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 0},
        "recent_orders": [
            {"order_id": "o1", "status": "FAILED", "remote_bet_id": None},
            {"order_id": "o2", "status": "COMPLETED", "remote_bet_id": "ext-ok"},
        ],
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    assert "FAILED_LOCAL_REMOTE_EXISTS" not in codes


def test_STATE_REGRESSION_fires_on_terminal_to_nonterminal():
    """STATE_REGRESSION fires when an order regresses from a terminal to a non-terminal state."""
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 0},
        "recent_orders": [
            {"order_id": "o1", "prev_status": "COMPLETED", "status": "PENDING"},
        ],
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    assert "STATE_REGRESSION" in codes


def test_STATE_REGRESSION_no_false_positive():
    """STATE_REGRESSION does not fire on valid terminal-to-terminal transitions."""
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 0},
        "recent_orders": [
            {"order_id": "o1", "prev_status": "COMPLETED", "status": "FAILED"},
        ],
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    assert "STATE_REGRESSION" not in codes


def test_INFLIGHT_STUCK_fires_when_inflight_order_exceeds_max_age():
    """INFLIGHT_STUCK fires when an INFLIGHT order exceeds the max_inflight_age_sec threshold."""
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
    assert "INFLIGHT_STUCK" in codes


def test_INFLIGHT_STUCK_no_false_positive():
    """INFLIGHT_STUCK does not fire when inflight orders are within allowed age."""
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 1},
        "max_inflight_age_sec": 300,
        "recent_orders": [
            {"order_id": "o1", "status": "INFLIGHT", "age_sec": 100},
        ],
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    assert "INFLIGHT_STUCK" not in codes


def test_EXPOSURE_MISMATCH_fires_on_exposure_delta_exceeding_tolerance():
    """INVARIANT_EXPOSURE_MISMATCH fires when local and remote exposure differ beyond tolerance.
    """
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 0},
        "risk": {"local_exposure": 100.0, "remote_exposure": 200.0, "exposure_tolerance": 0.01},
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    assert "INVARIANT_EXPOSURE_MISMATCH" in codes
    assert "EXPOSURE_MISMATCH" not in codes


def test_EXPOSURE_MISMATCH_no_false_positive():
    """INVARIANT_EXPOSURE_MISMATCH does not fire when local and remote exposure match within tolerance."""
    state = {
        "runtime": {"status": "READY"},
        "metrics": {"inflight_count": 0},
        "risk": {"local_exposure": 100.0, "remote_exposure": 100.0, "exposure_tolerance": 0.01},
    }
    violations = evaluate_invariants(state, enabled=True)
    codes = {v.code for v in violations}
    assert "INVARIANT_EXPOSURE_MISMATCH" not in codes


def test_all_four_required_invariants_active_in_watchdog_path():
    """Proves all 4 required invariant codes are evaluated when watchdog calls evaluate_invariants.

    Specifically tests that the watchdog's _evaluate_invariants() path surfaces
    FAILED_LOCAL_REMOTE_EXISTS, STATE_REGRESSION, INFLIGHT_STUCK, INVARIANT_EXPOSURE_MISMATCH
    as operational alerts when violations are present.
    """
    from observability.alerts_manager import AlertsManager
    from observability.health_registry import HealthRegistry
    from observability.incidents_manager import IncidentsManager
    from observability.metrics_registry import MetricsRegistry
    from observability.watchdog_service import WatchdogService

    class _SnapshotStub:
        def collect_and_store(self):
            return None

    alerts = AlertsManager()

    class _ViolatingProbe:
        def collect_health(self):
            return {"runtime": {"status": "READY", "reason": "ok", "details": {}}}

        def collect_metrics(self):
            return {}

        def collect_runtime_state(self):
            return {
                "runtime": {"status": "READY"},
                "metrics": {"inflight_count": 1},
                "max_inflight_age_sec": 300,
                "risk": {"local_exposure": 0.0, "remote_exposure": 999.0, "exposure_tolerance": 0.01},
                "recent_orders": [
                    # FAILED_LOCAL_REMOTE_EXISTS: failed order with remote bet id
                    {"order_id": "o1", "status": "FAILED", "remote_bet_id": "ext-ghost"},
                    # STATE_REGRESSION: completed order reverted to non-terminal
                    {"order_id": "o2", "prev_status": "COMPLETED", "status": "PENDING"},
                    # INFLIGHT_STUCK: inflight order older than max age
                    {"order_id": "o3", "status": "INFLIGHT", "age_sec": 999},
                ],
            }

    watchdog = WatchdogService(
        probe=_ViolatingProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )

    watchdog._evaluate_invariants()

    active_codes = {a["code"] for a in alerts.active_alerts() if a.get("source") == "invariant_reviewer"}
    assert "FAILED_LOCAL_REMOTE_EXISTS" in active_codes, (
        "FAILED_LOCAL_REMOTE_EXISTS must be raised by invariant reviewer when a FAILED order has remote_bet_id"
    )
    assert "STATE_REGRESSION" in active_codes, (
        "STATE_REGRESSION must be raised by invariant reviewer on terminal→non-terminal regression"
    )
    assert "INFLIGHT_STUCK" in active_codes, (
        "INFLIGHT_STUCK must be raised by invariant reviewer when inflight orders exceed max age"
    )
    assert "INVARIANT_EXPOSURE_MISMATCH" in active_codes, (
        "INVARIANT_EXPOSURE_MISMATCH must be raised by invariant reviewer when local/remote exposure differs"
    )
