"""
Tests that previously raw export/log bypass paths now route through sanitizer.

Verifies:
- export_json() redacts sensitive fields in the written file
- export_csv() redacts sensitive fields in each row
- snapshot_service.collect_and_store() stores sanitized payload in DB
- Redacted fields: app_key, password, session_token, api_hash, api_id,
  session_string, private_key, certificate
- Non-sensitive fields are NOT redacted (event_name, market_id, etc.)
"""

import json
import tempfile
from pathlib import Path
from typing import Any, Dict

import pytest

from observability.export_helpers import ExportHelpers
from observability.snapshot_service import SnapshotService


_REDACTED = "***REDACTED***"

# ===========================================================================
# ExportHelpers.export_json — sensitive fields redacted
# ===========================================================================

@pytest.mark.unit
@pytest.mark.guardrail
def test_export_json_redacts_app_key(tmp_path):
    helpers = ExportHelpers(export_dir=str(tmp_path))
    payload = {
        "event_name": "Chelsea vs Arsenal",
        "app_key": "REAL_APP_KEY_SECRET",
        "market_id": "1.999",
    }
    path = helpers.export_json("test_export", payload)
    written = json.loads(Path(path).read_text(encoding="utf-8"))
    assert written["app_key"] == _REDACTED, "app_key must be redacted in exported JSON"
    assert written["event_name"] == "Chelsea vs Arsenal", "non-sensitive fields must pass through"
    assert written["market_id"] == "1.999"


@pytest.mark.unit
@pytest.mark.guardrail
def test_export_json_redacts_password(tmp_path):
    helpers = ExportHelpers(export_dir=str(tmp_path))
    payload = {"username": "myuser", "password": "S3cr3tP@ss"}
    path = helpers.export_json("test_password", payload)
    written = json.loads(Path(path).read_text(encoding="utf-8"))
    assert written["password"] == _REDACTED
    assert written["username"] == "myuser"


@pytest.mark.unit
@pytest.mark.guardrail
def test_export_json_redacts_session_and_api_fields(tmp_path):
    helpers = ExportHelpers(export_dir=str(tmp_path))
    payload = {
        "session_token": "tok_ABCDEF",
        "api_hash": "abc123secret",
        "api_id": "12345678",
        "session_string": "1BAAAA_FAKE",
        "private_key": "-----BEGIN RSA PRIVATE KEY-----",
        "certificate": "-----BEGIN CERTIFICATE-----",
        "market_id": "1.111",
    }
    path = helpers.export_json("test_secrets", payload)
    written = json.loads(Path(path).read_text(encoding="utf-8"))
    for field in ("session_token", "api_hash", "api_id", "session_string", "private_key", "certificate"):
        assert written[field] == _REDACTED, f"{field} must be redacted in exported JSON"
    assert written["market_id"] == "1.111"


@pytest.mark.unit
@pytest.mark.guardrail
def test_export_json_redacts_nested_secrets(tmp_path):
    helpers = ExportHelpers(export_dir=str(tmp_path))
    payload = {
        "runtime_state": {
            "telegram": {
                "api_hash": "nested_secret",
                "phone_number": "+391234567",
            }
        }
    }
    path = helpers.export_json("test_nested", payload)
    written = json.loads(Path(path).read_text(encoding="utf-8"))
    assert written["runtime_state"]["telegram"]["api_hash"] == _REDACTED
    assert written["runtime_state"]["telegram"]["phone_number"] == "+391234567"


# ===========================================================================
# ExportHelpers.export_csv — sensitive fields redacted per row
# ===========================================================================

@pytest.mark.unit
@pytest.mark.guardrail
def test_export_csv_redacts_sensitive_fields(tmp_path):
    helpers = ExportHelpers(export_dir=str(tmp_path))
    rows = [
        {"market_id": "1.111", "app_key": "SECRET_KEY", "price": 2.0},
        {"market_id": "1.222", "password": "hunter2", "price": 3.5},
    ]
    path = helpers.export_csv("test_csv", rows)
    content = Path(path).read_text(encoding="utf-8")
    assert "SECRET_KEY" not in content, "app_key must be redacted in CSV"
    assert "hunter2" not in content, "password must be redacted in CSV"
    assert "1.111" in content
    assert "1.222" in content


# ===========================================================================
# SnapshotService.collect_and_store — sanitized before DB persist
# ===========================================================================

class _FakeHealthRegistry:
    def snapshot(self):
        return {"overall_status": "READY", "app_key": "REAL_KEY_LEAKED"}


class _FakeMetricsRegistry:
    def snapshot(self):
        return {"bets_placed": 10}


class _FakeAlertsManager:
    def snapshot(self):
        return {"alerts": []}


class _FakeIncidentsManager:
    def snapshot(self):
        return {"incidents": []}


class _FakeProbe:
    def collect_runtime_state(self):
        return {
            "mode": "LIVE",
            "session_token": "RAW_TOKEN_SHOULD_BE_REDACTED",
        }


class _CapturingDb:
    def __init__(self):
        self.saved: Dict[str, Any] = {}

    def save_observability_snapshot(self, payload):
        self.saved = payload


@pytest.mark.unit
@pytest.mark.guardrail
def test_snapshot_service_sanitizes_before_db_persist():
    db = _CapturingDb()
    svc = SnapshotService(
        db=db,
        probe=_FakeProbe(),
        health_registry=_FakeHealthRegistry(),
        metrics_registry=_FakeMetricsRegistry(),
        alerts_manager=_FakeAlertsManager(),
        incidents_manager=_FakeIncidentsManager(),
    )
    svc.collect_and_store()

    # session_token in runtime_state must be redacted
    runtime_state = db.saved.get("runtime_state", {})
    assert runtime_state.get("session_token") == _REDACTED, \
        "session_token must be redacted before DB persist"

    # app_key nested inside health snapshot must be redacted
    health = db.saved.get("health", {})
    assert health.get("app_key") == _REDACTED, \
        "app_key in health snapshot must be redacted before DB persist"


@pytest.mark.unit
@pytest.mark.guardrail
def test_snapshot_service_returns_raw_payload_to_caller():
    """collect_and_store() must return the raw (unsanitized) payload to the caller —
    only the DB-persisted copy is sanitized."""
    db = _CapturingDb()
    svc = SnapshotService(
        db=db,
        probe=_FakeProbe(),
        health_registry=_FakeHealthRegistry(),
        metrics_registry=_FakeMetricsRegistry(),
        alerts_manager=_FakeAlertsManager(),
        incidents_manager=_FakeIncidentsManager(),
    )
    result = svc.collect_and_store()

    # The returned value is the raw in-memory payload — callers get full context
    runtime_state = result.get("runtime_state", {})
    assert runtime_state.get("session_token") == "RAW_TOKEN_SHOULD_BE_REDACTED", \
        "collect_and_store() must return raw payload to caller (DB copy is sanitized separately)"
