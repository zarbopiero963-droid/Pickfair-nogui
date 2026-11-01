"""F-33 — riconciliazione tracker (47 righe) <-> catalogo harness (43 ID): ogni riga ha una destinazione."""
import json
import os

import pytest

from tests.fullscope.requirements import tracker as T

pytestmark = [pytest.mark.unit, pytest.mark.fullscope]
HERE = os.path.dirname(os.path.abspath(__file__))
STATES = {"COVERED", "PARTIAL", "XFAIL", "UNVERIFIED", "DEFERRED", "NOT_APPLICABLE"}


def _rec():
    return json.load(open(os.path.join(HERE, "reconciliation.json"), encoding="utf-8"))


def test_47_tracker_rows_plus_k6_each_map_to_a_catalog_id():
    rec = _rec()
    rows = rec["rows"]
    tracker_rows = [r for r in rows if not r["tracker_id"].startswith("K-6")]
    assert len(tracker_rows) == 47, len(tracker_rows)
    assert len(rows) == 48  # 47 + K-6
    catalog_ids = {r["id"] for r in T.load_catalog()["requirements"]}
    assert catalog_ids == {r["harness_id"] for r in rows}, "catalogo e riconciliazione divergono"
    assert len(catalog_ids) == 43
    for r in rows:
        assert r["state"] in STATES, r
        assert r["primary_pr"], r


def test_aggregations_and_duplicates_are_explicit():
    rows = _rec()["rows"]
    proxy = [r for r in rows if r.get("aggregated_into") == "R2-proxy"]
    assert len(proxy) == 6
    sig02 = next(r for r in rows if r["tracker_id"] == "SIG-02")
    assert "L6" in sig02.get("note", ""), "il duplicato L6 = SIG-02 deve restare esplicito"
    deferred = sorted(r["harness_id"] for r in rows if r["state"] == "DEFERRED")
    assert deferred == ["CTL-01", "MCP-01", "QA-04"]


def test_covered_state_requires_evidence_and_is_not_certification():
    for r in _rec()["rows"]:
        if r["state"] in ("COVERED", "PARTIAL", "XFAIL"):
            assert r.get("evidence"), r
        if r["state"] == "COVERED":
            assert "NON certificato" in r["evidence"] or "non certificato" in r["evidence"].lower(), r
