"""PFC-002: il check `full-suite` non puo' essere verde per finta.

Il gate giudica il report del plugin, non l'exit code. Qui ogni modo noto di un
falso verde e' un caso BLOCK: exit code diverso da 0, raccolta in errore, test
deselezionati, raccolta sotto il contratto, test raccolti e mai eseguiti,
fallimenti/errori/xpass, skipped oltre il contratto, argomenti che restringono
la selezione, report assente. Il plugin VERO gira su una mini-suite in un
sottoprocesso, cosi' si prova anche la registrazione degli esiti.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CI = ROOT / "scripts" / "ci"
sys.path.insert(0, str(CI))

import full_suite_gate as gate  # noqa: E402

CONTRATTO = {"min_collected": 3, "max_skipped": 1, "max_xfailed": 1,
             "forbidden_pytest_args": ["-k", "-m", "--deselect", "-x", "--lf"]}


def _report(**over):
    base = {
        "collected": ["t.py::a", "t.py::b", "t.py::c", "t.py::d"],
        "outcomes": {"t.py::a": "passed", "t.py::b": "passed", "t.py::c": "skipped", "t.py::d": "xfailed"},
        "deselected": [],
        "collect_errors": [],
        "exitstatus": 0,
    }
    base.update(over)
    return base


def test_pass_report_completo():
    v = gate.evaluate(_report(), CONTRATTO)
    assert v["ok"] is True, v["reasons"]
    assert v["counts"] == {"collected": 4, "passed": 2, "skipped": 1, "xfailed": 1}


@pytest.mark.parametrize("modifica,frammento", [
    ({"exitstatus": 1}, "uscito con 1"),
    ({"exitstatus": 5}, "uscito con 5"),  # 5 = nessun test raccolto
    ({"collect_errors": ["tests/x.py"]}, "errori di raccolta"),
    ({"deselected": ["t.py::e"]}, "deselezionati"),
    ({"collected": ["t.py::a", "t.py::b"], "outcomes": {"t.py::a": "passed", "t.py::b": "passed"}}, "almeno 3"),
    ({"outcomes": {"t.py::a": "passed", "t.py::b": "passed", "t.py::c": "skipped"}}, "mai eseguiti"),
    ({"outcomes": {"t.py::a": "failed", "t.py::b": "passed", "t.py::c": "skipped", "t.py::d": "xfailed"}}, "falliti"),
    ({"outcomes": {"t.py::a": "error", "t.py::b": "passed", "t.py::c": "skipped", "t.py::d": "xfailed"}}, "errore"),
    ({"outcomes": {"t.py::a": "xpassed", "t.py::b": "passed", "t.py::c": "skipped", "t.py::d": "xfailed"}}, "xpassed"),
    ({"outcomes": {"t.py::a": "skipped", "t.py::b": "passed", "t.py::c": "skipped", "t.py::d": "xfailed"}}, "skipped"),
    ({"outcomes": {"t.py::a": "passed", "t.py::b": "passed", "t.py::c": "xfailed", "t.py::d": "xfailed"}}, "xfailed"),
    ({"outcomes": {"t.py::a": "skipped", "t.py::b": "skipped", "t.py::c": "skipped", "t.py::d": "skipped"}},
     "nessun test passato"),
    ({"collected": ["t.py::a", "t.py::a", "t.py::b", "t.py::c"]}, "duplicati"),
])
def test_block_ogni_falso_verde(modifica, frammento):
    v = gate.evaluate(_report(**modifica), CONTRATTO)
    assert v["ok"] is False
    assert any(frammento in m for m in v["reasons"]), v["reasons"]


@pytest.mark.parametrize("arg", ["-k", "-m", "--deselect=t.py::a", "-x", "--lf"])
def test_block_argomenti_che_restringono_la_selezione(arg):
    v = gate.evaluate(_report(), CONTRATTO, [arg, "qualcosa"])
    assert v["ok"] is False and any("restringono" in m for m in v["reasons"])


@pytest.mark.parametrize("report", [None, "testo", 3, []])
def test_block_report_assente_o_illeggibile(report):
    v = gate.evaluate(report, CONTRATTO)
    assert v["ok"] is False and any("assente" in m for m in v["reasons"])


def test_contratto_del_repository_valido_e_non_vuoto():
    c = gate.load_contract()
    assert c["min_collected"] >= 8916  # baseline CI di #538: non si abbassa in silenzio
    assert c["max_skipped"] <= 15 and c["max_xfailed"] <= 5
    for vietato in ("-k", "-m", "--deselect", "-x"):
        assert vietato in c["forbidden_pytest_args"]


@pytest.mark.parametrize("dati", [{}, {"min_collected": 0, "max_skipped": 0, "max_xfailed": 0},
                                   {"min_collected": "10", "max_skipped": 0, "max_xfailed": 0}])
def test_contratto_malformato_rifiutato(tmp_path, dati):
    p = tmp_path / "c.json"
    p.write_text(json.dumps(dati))
    with pytest.raises(ValueError):
        gate.load_contract(p)


MINI_SUITE = '''
import pytest

def test_ok():
    assert True

@pytest.mark.skip(reason="ambiente")
def test_saltato():
    pass

@pytest.mark.xfail(reason="noto", strict=True)
def test_atteso_fallire():
    assert False

@pytest.fixture
def rotta():
    raise RuntimeError("setup rotto")

def test_con_setup_rotto(rotta):
    pass

def test_fallito():
    assert 1 == 2
'''


def _plugin(tmp_path, suite: str):
    d = tmp_path / "suite"
    d.mkdir()
    (d / "test_mini.py").write_text(suite)
    report = tmp_path / "r.json"
    env = dict(os.environ, FULL_SUITE_REPORT=str(report),
               PYTHONPATH=os.pathsep.join([str(CI), os.environ.get("PYTHONPATH", "")]))
    rc = subprocess.call([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-p", "full_suite_plugin",
                          "-c", os.devnull, "--rootdir", str(d), str(d)], cwd=str(d), env=env,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return rc, json.loads(report.read_text())


def test_il_plugin_registra_ogni_esito_reale(tmp_path):
    rc, r = _plugin(tmp_path, MINI_SUITE)
    assert rc == 1 and r["exitstatus"] == 1
    esiti = {k.split("::")[1]: v for k, v in r["outcomes"].items()}
    assert esiti == {"test_ok": "passed", "test_saltato": "skipped", "test_atteso_fallire": "xfailed",
                     "test_con_setup_rotto": "error", "test_fallito": "failed"}
    v = gate.evaluate(r, {**CONTRATTO, "min_collected": 5})
    assert v["ok"] is False


def test_il_plugin_vede_la_raccolta_rotta(tmp_path):
    rc, r = _plugin(tmp_path, "import modulo_che_non_esiste\n\ndef test_x():\n    pass\n")
    assert rc != 0 and r["collect_errors"]
    assert gate.evaluate(r, CONTRATTO)["ok"] is False


def test_il_plugin_su_suite_sana_da_verde(tmp_path):
    rc, r = _plugin(tmp_path, "def test_a():\n    pass\n\ndef test_b():\n    pass\n\ndef test_c():\n    pass\n")
    assert rc == 0
    assert gate.evaluate(r, CONTRATTO)["ok"] is True
