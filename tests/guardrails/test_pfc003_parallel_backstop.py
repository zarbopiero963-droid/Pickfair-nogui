"""PFC-003 (M157): suite completa in parallelo senza perdere nessun test.

I job che eseguono l'intera suite la dividono in due comandi: in parallelo con
xdist (`--dist worksteal`) i test che lo permettono, poi in serie quelli marcati
`serial` (widget Tk sul display X condiviso, PFC-001). Qui si prende la coppia
di comandi DAL WORKFLOW e la si esegue su un mini-repository: insieme devono
eseguire ogni test raccolto, una volta sola. Se uno dei due comandi sparisce o
cambia selezione, il conteggio non torna.
"""
from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WF = ROOT / ".github" / "workflows"
# job che eseguono l'intera suite su main (la deduplica e' PFC-002, non mergiata)
SUITE_COMPLETA = {
    "_module-ultra-check.yml": "Run full test suite (backstop)",
    "merge-simulation-hard.yml": "Run full pytest after merge simulation",
    "pr-check.yml": "Run tests",
}
PARALLELO = re.compile(r'^pytest -q -n auto --dist worksteal -m "not serial"( -x)?$')
SERIALE = re.compile(r"^pytest -q( -x)? -m serial$")


def _passo(nome_file: str, nome_passo: str) -> dict:
    wf = yaml.safe_load((WF / nome_file).read_text(encoding="utf-8"))
    for job in wf["jobs"].values():
        for step in job.get("steps", []):
            if step.get("name") == nome_passo:
                return step
    raise AssertionError(f"{nome_file}: passo {nome_passo!r} assente")


def _comandi_pytest(step: dict) -> list[str]:
    return [r.strip() for r in step["run"].splitlines() if r.strip().startswith("pytest")]


@pytest.mark.parametrize("nome_file", sorted(SUITE_COMPLETA))
def test_la_suite_completa_e_divisa_in_parallelo_piu_serial(nome_file):
    step = _passo(nome_file, SUITE_COMPLETA[nome_file])
    assert "set -euo pipefail" in step["run"], "un comando fallito deve fermare il passo"
    comandi = _comandi_pytest(step)
    assert len(comandi) == 2, comandi
    assert PARALLELO.match(comandi[0]), comandi[0]
    assert SERIALE.match(comandi[1]), comandi[1]
    assert "if" not in step, "il passo della suite completa non deve poter essere saltato"


def test_il_marker_serial_e_dichiarato_e_i_marker_sono_stretti():
    ini = (ROOT / "pytest.ini").read_text(encoding="utf-8")
    assert re.search(r"^\s*serial:", ini, re.M)
    assert "--strict-markers" in ini  # un marker scritto male non sparisce in silenzio


def _mini_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "mini"
    (repo / "tests").mkdir(parents=True)
    (repo / "pytest.ini").write_text(
        "[pytest]\ntestpaths = tests\naddopts = --strict-markers\nmarkers =\n    serial: non in parallelo\n",
        encoding="utf-8",
    )
    corpo = ["import os, pytest", "LOG = os.environ['PF_LOG']", "",
             "def _segna(n):", "    with open(LOG, 'a') as fh:", "        fh.write(n + '\\n')", ""]
    for i in range(6):
        corpo += [f"def test_p{i}():", f"    _segna('p{i}')", ""]
    corpo += ["@pytest.mark.serial", "def test_s0():", "    _segna('s0')", "",
              "@pytest.mark.serial", "def test_s1():", "    _segna('s1')", ""]
    (repo / "tests" / "test_mini.py").write_text("\n".join(corpo), encoding="utf-8")
    return repo


@pytest.mark.parametrize("nome_file", sorted(SUITE_COMPLETA))
def test_i_due_comandi_del_workflow_eseguono_ogni_test_una_volta(nome_file, tmp_path):
    pytest.importorskip("xdist")
    repo = _mini_repo(tmp_path)
    log = tmp_path / "eseguiti.log"
    env = {**os.environ, "PF_LOG": str(log), "PYTHONDONTWRITEBYTECODE": "1"}
    for comando in _comandi_pytest(_passo(nome_file, SUITE_COMPLETA[nome_file])):
        argv = [sys.executable, "-m"] + shlex.split(comando) + ["-p", "no:cacheprovider"]
        argv = [a.replace("auto", "2") if a == "auto" else a for a in argv]  # 2 worker bastano
        proc = subprocess.run(argv, cwd=repo, env=env, capture_output=True, text=True, timeout=120)
        assert proc.returncode == 0, proc.stdout + proc.stderr
    eseguiti = sorted(log.read_text(encoding="utf-8").split())
    attesi = sorted([f"p{i}" for i in range(6)] + ["s0", "s1"])
    assert eseguiti == attesi, f"eseguiti {eseguiti}, attesi {attesi}: test persi o ripetuti"


def test_la_mutation_gira_con_una_sandbox_per_worker():
    wf = (WF / "mutation-guardrails.yml").read_text(encoding="utf-8")
    assert re.search(r"run_mutation_guardrails\.py --fail-under 80 --jobs [2-9]", wf)
