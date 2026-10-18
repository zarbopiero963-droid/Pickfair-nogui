"""PR #512 ciclo 8 — rilievo Codex su 034af59 (pf_trace): per TESTED/VERIFIED/CERTIFIED conta l'ULTIMA evidenza
applicabile, che deve essere PASS. Un BLOCKED / NOT RUN / NOT VERIFIED successivo a un PASS invalida lo stato,
come gia' faceva un FAIL. Git reale su tmp_path, nessun mock."""
import os
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "traceability"))
import pf_trace  # noqa: E402


def _repo(tmp_path):
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_x.py").write_text("def test_ok():\n    assert True\n")
    env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t", GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t")
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "add", "-A"], check=True, env=env)
    subprocess.run(["git", "-C", str(tmp_path), "commit", "-qm", "c"], check=True, env=env)
    return subprocess.run(["git", "-C", str(tmp_path), "rev-parse", "--short=7", "HEAD"],
                          capture_output=True, text=True, check=True).stdout.strip()


def _req(sha, status, results, **over):
    ev = [{"sha": sha, "command": "pytest", "environment": "venv", "timestamp": f"2026-10-17T00:00:0{i}Z", "result": r}
          for i, r in enumerate(results)]
    req = {"id": "R-1", "text": "t", "source": "s", "owner_decision": "D", "activity": "ACTIVE", "status": status,
           "issue": ["#1"], "component": ["x.py"], "code": ["x.py::f"], "oracle": "o",
           "tests": ["tests/test_x.py::test_ok"], "evidence": ev, "findings": [], "reviewer": "rev", "gate": "A",
           "limitations": []}
    req.update(over)
    return {"schema": "pf-traceability/1", "requirements": [req]}


@pytest.mark.parametrize("status", ["TESTED", "VERIFIED", "CERTIFIED"])
@pytest.mark.parametrize("later", ["BLOCKED", "NOT RUN", "NOT VERIFIED", "FAIL"])
def test_latest_non_pass_evidence_invalidates_a_tested_status(tmp_path, status, later):
    sha = _repo(tmp_path)
    errors, _ = pf_trace.validate(_req(sha, status, ["PASS", later]), str(tmp_path))
    assert any("ultima evidenza" in e for e in errors), (status, later, errors)


@pytest.mark.parametrize("status", ["TESTED", "VERIFIED", "CERTIFIED"])
def test_pass_after_a_non_pass_is_valid(tmp_path, status):
    sha = _repo(tmp_path)
    # (senza FAIL: una FAIL senza finding tracciato e' gia' un errore distinto per VERIFIED/CERTIFIED)
    errors, _ = pf_trace.validate(_req(sha, status, ["BLOCKED", "NOT RUN", "PASS"]), str(tmp_path))
    assert errors == [], errors


def test_blocked_status_may_end_with_a_blocked_evidence(tmp_path):
    sha = _repo(tmp_path)
    errors, _ = pf_trace.validate(_req(sha, "BLOCKED", ["PASS", "BLOCKED"]), str(tmp_path))
    assert errors == [], errors


# ------------------------------------------------------------------ ciclo 9 (Codex r4307328211): associazione evidenza-test
def _req_cmd(sha, command, tests_field=None, declared=("tests/test_x.py::test_ok",)):
    data = _req(sha, "TESTED", ["PASS"], tests=list(declared))
    ev = data["requirements"][0]["evidence"][0]
    ev["command"] = command
    if tests_field is not None:
        ev["tests"] = tests_field
    return data


def _uncovered(warnings):
    return [w for w in warnings if "senza evidenza che lo esegua" in w]


def test_evidence_without_tests_and_unrelated_command_does_not_cover_the_declared_test(tmp_path):
    sha = _repo(tmp_path)
    _, warnings = pf_trace.validate(_req_cmd(sha, "pytest -q tests/test_other.py"), str(tmp_path))
    assert _uncovered(warnings), "un PASS di un comando qualsiasi non deve coprire tutti i test dichiarati"


def test_strict_validation_fails_when_a_declared_test_is_never_executed(tmp_path):
    sha = _repo(tmp_path)
    errors, warnings = pf_trace.validate(_req_cmd(sha, "python3 tools/qualcosa.py"), str(tmp_path))
    assert errors == [] and _uncovered(warnings)          # --strict trasforma il warning in FAIL (main)


@pytest.mark.parametrize("command", [
    "pytest -q tests/test_x.py::test_ok",                 # node-id esatto
    "pytest -q tests/test_x.py",                          # file del test
    "venv/bin/python -m pytest -q -p no:cacheprovider tests",   # directory che lo contiene
    "pytest -q tests/ -x",
])
def test_command_naming_the_test_its_file_or_directory_covers_it(tmp_path, command):
    sha = _repo(tmp_path)
    _, warnings = pf_trace.validate(_req_cmd(sha, command), str(tmp_path))
    assert not _uncovered(warnings), (command, warnings)


def test_command_naming_another_node_of_the_same_file_does_not_cover_it(tmp_path):
    sha = _repo(tmp_path)
    _, warnings = pf_trace.validate(_req_cmd(sha, "pytest -q tests/test_x.py::test_other"), str(tmp_path))
    assert _uncovered(warnings)


def test_explicit_tests_field_is_authoritative(tmp_path):
    sha = _repo(tmp_path)
    _, ok = pf_trace.validate(_req_cmd(sha, "make check", tests_field=["tests/test_x.py::test_ok"]), str(tmp_path))
    assert not _uncovered(ok)
    _, ko = pf_trace.validate(_req_cmd(sha, "pytest -q tests/test_x.py", tests_field=["tests/test_y.py"]), str(tmp_path))
    assert _uncovered(ko), "un campo tests esplicito che non nomina il test non lo copre (anche se il comando si')"


def test_real_registry_validates_strict():
    root = ROOT
    reg = os.path.join(root, "docs", "traceability", "requirements.json")
    errors, warnings = pf_trace.validate(pf_trace.load(reg), root)
    assert errors == [] and warnings == [], (errors, warnings)
