from __future__ import annotations

from queue import Queue

from observability.anomaly_engine import AnomalyEngine
from observability.anomaly_rules import DEFAULT_ANOMALY_RULES
from observability.cto_reviewer import CtoReviewer
from observability.runtime_probe import RuntimeProbe


class _Writer:
    def __init__(self, backlog: int, failed: int = 0, dropped: int = 0):
        self.queue = Queue()
        for _ in range(backlog):
            self.queue.put(1)
        self._failed = failed
        self._dropped = dropped
        self._written = 0


def test_writer_backlog_growth_is_observable_and_anomalous():
    probe = RuntimeProbe(async_db_writer=_Writer(backlog=80, failed=1))
    ctx = probe.collect_reviewer_context()
    assert ctx["db"]["db_writer_backlog"] >= 80
    anomalies = AnomalyEngine(DEFAULT_ANOMALY_RULES).evaluate(ctx)
    assert any(a["code"] == "DB_CONTENTION_DETECTED" for a in anomalies)


def test_writer_stall_with_runtime_continuing_surfaces_non_healthy_signal():
    metrics = {"gauges": {"writer_backlog": 75, "memory_growth_mb": 120, "stalled_ticks": 3, "completed_delta": 0}}
    reviewer = CtoReviewer(history_window=5, cooldown_sec=0)
    findings = reviewer.evaluate(
        {
            "metrics_snapshot": metrics,
            "anomaly_alerts": [{"code": "DB_CONTENTION_DETECTED", "severity": "high"}, {"code": "STUCK_INFLIGHT", "severity": "high"}],
            "forensics_alerts": [],
            "incidents_snapshot": {"open_count": 1},
            "runtime_probe_state": {"alert_pipeline": {"enabled": True, "deliverable": False}},
            "diagnostics_bundle": {"available": False},
        }
    )
    names = {f["rule_name"] for f in findings}
    assert "MEMORY_GROWTH_TREND" in names
    assert "STALLED_SYSTEM_DETECTED" in names
