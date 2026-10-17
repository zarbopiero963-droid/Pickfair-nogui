"""PF-TRACEABILITY-ALL-01 — validatore del registro atomico: ogni controllo richiesto dall'owner produce FAIL (BLOCK)
e il registro reale del repository valida PASS."""
import copy
import json
import os
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "traceability"))
import pf_trace  # noqa: E402

REG = os.path.join(ROOT, "docs", "traceability", "requirements.json")


def _good(tmp_path):
    test_file = tmp_path / "tests" / "test_x.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("def test_ok():\n    assert True\n")
    req = {"id": "R-1", "text": "t", "source": "s", "owner_decision": "D", "activity": "ACTIVE", "status": "TESTED",
           "issue": ["#1"], "component": ["x.py"], "code": ["x.py::f"], "oracle": "o", "tests": ["tests/test_x.py::test_ok"],
           "evidence": [{"sha": "8ffb4f3", "command": "pytest", "environment": "venv", "timestamp": "2026-10-17T00:00:00Z", "result": "PASS"}],
           "findings": [], "reviewer": None, "gate": "A", "limitations": []}
    return {"schema": "pf-traceability/1", "requirements": [req]}


def _errs(data, tmp_path, previous=None):
    errors, _ = pf_trace.validate(data, str(tmp_path), previous)
    return errors


def test_valid_registry_passes(tmp_path):
    assert _errs(_good(tmp_path), tmp_path) == []


@pytest.mark.parametrize("mutate,needle", [
    (lambda r: r.__setitem__("tests", []), "senza test"),
    (lambda r: r.__setitem__("tests", ["tests/test_x.py::test_mancante"]), "test inesistente"),
    (lambda r: r["evidence"][0].__setitem__("sha", "nessuno"), "senza SHA"),
    (lambda r: r.__setitem__("evidence", []), "senza evidenza PASS"),
    (lambda r: r["evidence"].append({"sha": "8ffb4f3", "command": "pytest", "environment": "venv", "timestamp": "t", "result": "FAIL"}), "contraddizione"),
    (lambda r: r.__setitem__("activity", "SUSPENDED"), "contato come TESTED"),
    (lambda r: (r.__setitem__("status", "CERTIFIED"), r.__setitem__("evidence", [])), "CERTIFIED"),
    (lambda r: r.__setitem__("status", "VERIFIED"), "senza revisore"),
    (lambda r: r.__setitem__("status", "INVENTATO"), "stato non ammesso"),
    (lambda r: (r.__setitem__("status", "VERIFIED"), r.__setitem__("reviewer", "x"), r.__setitem__("findings", [{"id": "F", "state": "OPEN"}])), "finding aperti"),
    (lambda r: r.pop("oracle"), "campo mancante oracle"),
    (lambda r: r["evidence"][0].__setitem__("result", "OK"), "result non ammesso"),
])
def test_each_violation_is_rejected(tmp_path, mutate, needle):
    data = _good(tmp_path)
    mutate(data["requirements"][0])
    errors = _errs(data, tmp_path)
    assert any(needle in e for e in errors), errors


def test_duplicate_ids_are_rejected(tmp_path):
    data = _good(tmp_path)
    data["requirements"].append(copy.deepcopy(data["requirements"][0]))
    assert any("duplicato" in e for e in _errs(data, tmp_path))


def test_evidence_is_append_only_and_requirements_cannot_disappear(tmp_path):
    previous = _good(tmp_path)
    data = copy.deepcopy(previous)
    data["requirements"][0]["evidence"][0]["result"] = "PASS"
    data["requirements"][0]["evidence"][0]["note"] = "riscritta"         # modifica di un'evidenza precedente
    assert any("append-only" in e for e in _errs(data, tmp_path, previous))
    data2 = copy.deepcopy(previous)
    data2["requirements"][0]["evidence"].append({"sha": "abcdef1", "command": "pytest", "environment": "venv",
                                                 "timestamp": "2026-10-18T00:00:00Z", "result": "PASS"})
    assert _errs(data2, tmp_path, previous) == []                        # aggiungere e' sempre ammesso
    data3 = {"schema": "pf-traceability/1", "requirements": []}
    assert any("rimosso dal registro" in e for e in _errs(data3, tmp_path, previous))


def test_repository_registry_validates_against_real_tests():
    data = pf_trace.load(REG)
    errors, warnings = pf_trace.validate(data, ROOT, pf_trace.committed("docs/traceability/requirements.json", ROOT))
    assert errors == [], errors
    assert any(r["status"] == "PLANNED" for r in data["requirements"]), "nessun requisito dichiarato PLANNED: sospetto"
    assert not any(r["status"] == "CERTIFIED" for r in data["requirements"]), "Gate A NON PASS: nessun CERTIFIED"


def test_cli_exit_codes(tmp_path, capsys):
    good = _good(tmp_path)
    reg = tmp_path / "reg.json"
    reg.write_text(json.dumps(good))
    assert pf_trace.main(["validate", "--registry", str(reg), "--repo", str(tmp_path)]) == 0
    good["requirements"][0]["tests"] = []
    reg.write_text(json.dumps(good))
    assert pf_trace.main(["validate", "--registry", str(reg), "--repo", str(tmp_path)]) == 1
    assert pf_trace.main(["report", "--registry", str(reg)]) == 0
    assert "R-1" in capsys.readouterr().out
