"""F-33 — il tracciatore dei requisiti conta solo i test passati; xfail/skip/fail non coprono."""
import pytest

from tests.fullscope.requirements import tracker as T

pytestmark = [pytest.mark.unit, pytest.mark.fullscope]

CATALOG = {"source": "test", "requirements": [
    {"id": "A-1", "primary_pr": "F-01", "status_initial": "UNVERIFIED"},
    {"id": "B-2", "primary_pr": "F-02", "status_initial": "UNVERIFIED"},
    {"id": "C-3", "primary_pr": "F-03", "status_initial": "UNVERIFIED"},
]}


def _t():
    return T.RequirementTracker(CATALOG)


def test_passed_covers_and_missing_listed():
    t = _t()
    t.register("tests/x.py::test_a", ["A-1"])
    t.record("tests/x.py::test_a", "setup", "passed")
    t.record("tests/x.py::test_a", "call", "passed")
    t.record("tests/x.py::test_a", "teardown", "passed")
    r = t.report("abc1234", 7)
    assert r["covered"] == ["A-1"] and r["counts"]["covered"] == 1
    assert [m["id"] for m in r["missing"]] == ["B-2", "C-3"]
    assert r["seed"] == 7 and r["repo_sha"] == "abc1234"


@pytest.mark.parametrize("phase,outcome,xfail", [("call", "failed", False), ("call", "skipped", True),
                                                  ("call", "skipped", False), ("setup", "failed", False)])
def test_non_passed_outcomes_do_not_cover(phase, outcome, xfail):
    t = _t()
    t.register("tests/x.py::test_b", ["B-2"])
    t.record("tests/x.py::test_b", "setup", "passed" if phase != "setup" else outcome)
    if phase == "call":
        t.record("tests/x.py::test_b", "call", outcome, xfail=xfail)
    r = t.report("sha", 0)
    assert r["covered"] == []
    assert r["attempted_not_covered"] == ["B-2"]


def test_teardown_error_after_pass_is_not_covered():
    t = _t()
    t.register("n", ["A-1"])
    t.record("n", "call", "passed")
    t.record("n", "teardown", "failed")
    assert t.report("s", 0)["covered"] == []


def test_unknown_id_is_reported_not_dropped():
    t = _t()
    t.register("n", ["ZZ-9"])
    t.record("n", "call", "passed")
    r = t.report("s", 0)
    assert r["unknown_ids"] == {"ZZ-9": ["n"]} and r["counts"]["unknown_ids"] == 1


def test_real_catalog_loads_and_ids_unique():
    cat = T.load_catalog()
    ids = [r["id"] for r in cat["requirements"]]
    assert len(ids) == len(set(ids)) >= 43
    assert all(r["primary_pr"] for r in cat["requirements"])


def test_tracker_row_markdown():
    t = _t()
    t.register("tests/x.py::test_a", ["A-1"])
    t.record("tests/x.py::test_a", "call", "passed")
    r = t.report("abcdef1234", 0)
    row = T.tracker_row(r, "A-1")
    assert row.startswith("| A-1 |") and "COVERED" in row and "abcdef1" in row
    assert "MISSING" in T.tracker_row(r, "B-2")


def test_merge_records_from_workers_keeps_all_and_is_fail_closed():
    """xdist: i record dei worker si uniscono; uno stesso nodeid passed su un worker e failed su un altro non copre."""
    a, b = _t(), _t()
    a.register("w0::t_a", ["A-1"])
    a.record("w0::t_a", "call", "passed")
    b.register("w1::t_b", ["B-2"])
    b.record("w1::t_b", "call", "passed")
    b.register("dup::t", ["C-3"])
    b.record("dup::t", "call", "failed")
    main = _t()
    main.register("dup::t", ["C-3"])
    main.record("dup::t", "call", "passed")
    main.merge_records(a.records)
    main.merge_records(b.records)
    r = main.report("s", 0)
    assert r["covered"] == ["A-1", "B-2"]
    assert r["attempted_not_covered"] == ["C-3"]


def test_worker_partial_path_and_id(monkeypatch, tmp_path):
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")
    assert T.worker_id() == "gw3"
    assert T.partial_records_path(str(tmp_path), "gw3").endswith("requirements_records.gw3.json")
    monkeypatch.delenv("PYTEST_XDIST_WORKER")
    assert T.worker_id() == ""


def test_merge_ignores_not_executed_records_from_other_workers():
    """xdist reale: il worker gw1 raccoglie ma non esegue il test che gw0 ha eseguito e passato."""
    gw0, gw1 = _t(), _t()
    for t in (gw0, gw1):
        t.register("s::t_rst", ["A-1"])
    gw0.record("s::t_rst", "call", "passed")
    main = _t()
    main.merge_records(gw1.records)   # prima il worker che non l'ha eseguito
    main.merge_records(gw0.records)
    assert main.report("s", 0)["covered"] == ["A-1"]
    main2 = _t()
    main2.merge_records(gw0.records)  # ordine inverso
    main2.merge_records(gw1.records)
    assert main2.report("s", 0)["covered"] == ["A-1"]
