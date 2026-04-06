import re
from pathlib import Path

from observability.diagnostic_bundle_builder import DiagnosticBundleBuilder


def _payload():
    return {
        "health": {"overall_status": "READY"},
        "metrics": {"gauges": {}},
        "alerts": {"alerts": []},
        "incidents": {"incidents": []},
        "runtime_state": {},
        "safe_mode_state": {"enabled": False},
        "recent_orders": [],
        "recent_audit": [],
    }


def test_bundle_name_is_collision_safe_same_second(tmp_path, monkeypatch):
    monkeypatch.setattr("time.strftime", lambda fmt: "20260101_120000")

    builder = DiagnosticBundleBuilder(export_dir=str(tmp_path / "exports"))
    p = _payload()

    path1 = Path(builder.build(**p))
    path2 = Path(builder.build(**p))

    assert path1.name != path2.name
    assert re.match(r"^diagnostics_bundle_\d{8}_\d{6}_[0-9a-f]{8}\.zip$", path1.name)
    assert re.match(r"^diagnostics_bundle_\d{8}_\d{6}_[0-9a-f]{8}\.zip$", path2.name)
