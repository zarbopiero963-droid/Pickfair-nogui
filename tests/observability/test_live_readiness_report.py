import pytest

from observability.runtime_probe import RuntimeProbe


class _RuntimeReady:
    def evaluate_live_readiness(self, **_kwargs):
        return {
            "ready": True,
            "level": "READY",
            "blockers": [],
            "details": {"runtime_state_ok": True},
        }


class _RuntimeDegraded:
    def evaluate_live_readiness(self, **_kwargs):
        return {
            "ready": False,
            "level": "DEGRADED",
            "blockers": [],
            "details": {"runtime_state_ok": False},
        }


@pytest.mark.observability
def test_live_readiness_report_shape_is_stable():
    probe = RuntimeProbe(runtime_controller=_RuntimeReady())
    report = probe.get_live_readiness_report(context={"execution_mode": "LIVE"})
    assert set(report.keys()) == {"ready", "level", "blockers", "details"}
    assert isinstance(report["blockers"], list)
    assert isinstance(report["details"], dict)


@pytest.mark.observability
def test_readiness_levels_are_distinguishable():
    ready_probe = RuntimeProbe(runtime_controller=_RuntimeReady())
    not_ready_probe = RuntimeProbe(runtime_controller=_RuntimeDegraded())
    unknown_probe = RuntimeProbe(runtime_controller=None)

    ready = ready_probe.get_live_readiness_report(context={"execution_mode": "LIVE"})
    degraded = not_ready_probe.get_live_readiness_report(context={"execution_mode": "LIVE"})
    unknown = unknown_probe.get_live_readiness_report(context={"execution_mode": "LIVE"})

    assert ready["level"] == "READY"
    assert degraded["level"] == "DEGRADED"
    assert unknown["level"] == "NOT_READY"


@pytest.mark.observability
def test_unknown_is_not_promoted_to_ready():
    probe = RuntimeProbe(runtime_controller=None)
    report = probe.get_live_readiness_report(context={"execution_mode": "LIVE"})
    assert report["ready"] is False
    assert report["level"] != "READY"
    assert "READINESS_SIGNAL_UNKNOWN" in report["blockers"]
