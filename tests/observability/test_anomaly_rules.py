from observability.anomaly_config_builder import build_anomaly_rule_config
from observability.anomaly_rules import (
    db_contention_detected,
    event_fanout_incomplete,
    exposure_mismatch,
    financial_drift,
    ghost_order_detected,
)


def test_each_new_rule_triggers_on_bad_state():
    assert ghost_order_detected({"runtime_state": {"reconcile": {"ghost_orders_count": 1}}}, {})

    assert exposure_mismatch(
        {"risk": {"expected_exposure": 100.0, "actual_exposure": 110.0, "exposure_tolerance": 0.5}},
        {},
    )

    assert db_contention_detected(
        {"db": {"lock_wait_ms": 500.0, "contention_events": 0, "lock_wait_threshold_ms": 200.0}},
        {},
    )

    assert event_fanout_incomplete({"event_bus": {"expected_fanout": 5, "delivered_fanout": 3}}, {})

    assert financial_drift(
        {"financials": {"ledger_balance": 200.0, "venue_balance": 150.0, "drift_threshold": 1.0}},
        {},
    )


def test_each_new_rule_does_not_trigger_on_good_state():
    assert ghost_order_detected({"runtime_state": {"reconcile": {"ghost_orders_count": 0}}}, {}) is None

    assert exposure_mismatch(
        {"risk": {"expected_exposure": 100.0, "actual_exposure": 100.2, "exposure_tolerance": 0.5}},
        {},
    ) is None

    assert db_contention_detected(
        {"db": {"lock_wait_ms": 100.0, "contention_events": 0, "lock_wait_threshold_ms": 200.0}},
        {},
    ) is None

    assert event_fanout_incomplete({"event_bus": {"expected_fanout": 5, "delivered_fanout": 5}}, {}) is None

    assert financial_drift(
        {"financials": {"ledger_balance": 200.0, "venue_balance": 199.5, "drift_threshold": 1.0}},
        {},
    ) is None


def test_multiple_anomalies_can_be_returned_together():
    context = {
        "runtime_state": {"reconcile": {"ghost_orders_count": 2}},
        "risk": {"expected_exposure": 100.0, "actual_exposure": 120.0, "exposure_tolerance": 0.5},
        "db": {"lock_wait_ms": 250.0, "contention_events": 1, "lock_wait_threshold_ms": 200.0},
        "event_bus": {"expected_fanout": 4, "delivered_fanout": 1},
        "financials": {"ledger_balance": 1000.0, "venue_balance": 900.0, "drift_threshold": 1.0},
    }

    anomalies = [
        ghost_order_detected(context, {}),
        exposure_mismatch(context, {}),
        db_contention_detected(context, {}),
        event_fanout_incomplete(context, {}),
        financial_drift(context, {}),
    ]

    assert len([a for a in anomalies if a is not None]) == 5


def test_config_builder_generates_expected_rule_config_from_audit_input():
    audit_input = {
        "ghost_orders_count": 3,
        "risk": {"expected_exposure": "120.0", "actual_exposure": "119.8", "exposure_tolerance": "0.4"},
        "db": {"lock_wait_ms": "55.5", "contention_events": "2", "lock_wait_threshold_ms": "210"},
        "event_bus": {"expected_fanout": "7", "delivered_fanout": "6"},
        "financials": {"ledger_balance": "500.0", "venue_balance": "499.3", "drift_threshold": "1.2"},
    }

    assert build_anomaly_rule_config(audit_input) == {
        "ghost_order": {"enabled": False, "ghost_orders_count": 3},
        "exposure_mismatch": {
            "enabled": False,
            "expected_exposure": 120.0,
            "actual_exposure": 119.8,
            "exposure_tolerance": 0.4,
        },
        "db_contention": {
            "enabled": False,
            "lock_wait_ms": 55.5,
            "contention_events": 2,
            "lock_wait_threshold_ms": 210.0,
        },
        "event_fanout_incomplete": {"enabled": False, "expected_fanout": 7, "delivered_fanout": 6},
        "financial_drift": {
            "enabled": False,
            "ledger_balance": 500.0,
            "venue_balance": 499.3,
            "drift_threshold": 1.2,
        },
    }
