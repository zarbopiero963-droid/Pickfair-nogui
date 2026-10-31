"""F-33 — il report dei requisiti viene aggregato anche quando il controller xdist non carica il conftest annidato.

Rilievo Codex su a721e09: con ``pytest -n auto ... tests`` (come ``pr-check.yml``) il controller carica solo i
conftest dei path passati e dei genitori; gli hook nel conftest di ``tests/fullscope`` non giravano e nessun
``requirements_report.json`` veniva scritto. Il test lancia un pytest reale con due worker su un progetto minimo
il cui ``conftest.py`` di root e' IL TESTO del conftest di root del repository, con i test in una sottodirectory
annidata: PASS = report aggregato dai worker; BLOCK = senza la registrazione di root nessun report.
"""
from __future__ import annotations

import ast
import json
import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
PLUGIN = "tests.fullscope.requirements.pytest_plugin"

PROBE = '''
import pytest

@pytest.mark.pf_requirement("RST-03")
@pytest.mark.parametrize("i", range(4))
def test_probe(i):
    assert i >= 0

@pytest.mark.pf_requirement("SIG-02")
def test_probe_deselected():
    assert False, "deselezionato da -k: non deve entrare nel report"
'''

INI = """[pytest]
addopts = -p no:cacheprovider
markers =
    pf_requirement(*ids): requisiti
"""


def _project(tmp_path, root_conftest: str):
    (tmp_path / "pytest.ini").write_text(INI)
    (tmp_path / "conftest.py").write_text(root_conftest)
    nested = tmp_path / "probe" / "deep"
    nested.mkdir(parents=True)
    (nested / "test_probe_plugin.py").write_text(PROBE)
    art = tmp_path / "art"
    env = {k: v for k, v in os.environ.items() if not k.startswith(("PYTEST_", "PF_FULLSCOPE_ART"))}
    env["PYTHONPATH"] = REPO + os.pathsep + env.get("PYTHONPATH", "")
    env["PF_FULLSCOPE_ART"] = str(art)
    proc = subprocess.run([sys.executable, "-m", "pytest", "-q", "-n", "2", "-k", "not deselected", "probe"],
                          cwd=str(tmp_path), env=env, capture_output=True, text=True, timeout=300, check=False)
    return proc, art


def _root_conftest_text() -> str:
    with open(os.path.join(REPO, "conftest.py"), "r", encoding="utf-8") as fh:
        return fh.read()


def test_root_conftest_registers_the_plugin_and_nested_conftest_defines_no_hooks():
    tree = ast.parse(_root_conftest_text())
    plugins = [ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign)
               and any(getattr(t, "id", "") == "pytest_plugins" for t in n.targets)]
    assert plugins and PLUGIN in plugins[0]
    with open(os.path.join(REPO, "tests", "fullscope", "conftest.py"), "r", encoding="utf-8") as fh:
        nested = ast.parse(fh.read())
    hooks = [n.name for n in nested.body if isinstance(n, ast.FunctionDef) and n.name.startswith("pytest_")]
    assert hooks == [], f"hook nel conftest annidato (il controller xdist non li carica): {hooks}"


def test_xdist_run_from_parent_dir_writes_aggregated_report(tmp_path):
    pytest.importorskip("xdist")
    proc, art = _project(tmp_path, _root_conftest_text())
    assert proc.returncode == 0, proc.stdout[-3000:] + proc.stderr[-3000:]
    report_path = art / "requirements_report.json"
    assert report_path.exists(), "controller xdist: nessun report aggregato\n" + proc.stdout[-3000:]
    report = json.loads(report_path.read_text())
    assert report["workers_merged"], "il report non viene dall'aggregazione dei worker"
    assert not list(art.glob("requirements_records.*.json")), "parziali dei worker non rimossi"
    assert report["covered"] == ["RST-03"]
    tests = report["requirements"]["RST-03"]["tests"]
    assert len(tests) == 4 and all(t["outcome"] == "passed" for t in tests)
    assert report["attempted_not_covered"] == [], "un test deselezionato e' finito nel report"
    assert len(report["workers_merged"]) == 2


def test_block_without_root_registration_no_report_is_written(tmp_path):
    pytest.importorskip("xdist")
    proc, art = _project(tmp_path, '"""conftest senza plugin"""\n')
    # senza plugin il marker e' comunque registrato in pytest.ini: la sessione passa ma nessuno aggrega
    assert proc.returncode == 0, proc.stdout[-3000:] + proc.stderr[-3000:]
    assert not (art / "requirements_report.json").exists()
