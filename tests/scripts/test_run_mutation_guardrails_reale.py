"""#525 — il mutation testing deve mutare davvero il codice e non dare PASS falsi.

Ogni test costruisce un mini-repository in `tmp_path` (un modulo, un file di
test, un catalogo) ed esegue il runner reale `scripts/run_mutation_guardrails.py`
con pytest vero. L'ultimo blocco controlla il catalogo reale del repository.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "run_mutation_guardrails_reale", ROOT / "scripts" / "run_mutation_guardrails.py"
)
runner = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = runner
_spec.loader.exec_module(runner)

MODULO = """\
def liability(stake, price):
    return stake * (price - 1.0)


def is_lay(side):
    return side == "LAY"
"""

TEST_FORTE = """\
from calc import liability


def test_liability_lay():
    assert liability(10.0, 3.0) == 20.0
"""


def _repo(tmp_path: Path, mutations, test_src=TEST_FORTE, areas=("lay_liability",)) -> Path:
    repo = tmp_path / "repo"
    (repo / "tests").mkdir(parents=True)
    (repo / "guardrails").mkdir()
    (repo / "calc.py").write_text(MODULO, encoding="utf-8")
    (repo / "tests" / "test_calc.py").write_text(test_src, encoding="utf-8")
    catalog = {"required_areas": list(areas), "optional_areas": [], "mutations": mutations}
    (repo / "guardrails" / "mutation_catalog.json").write_text(json.dumps(catalog), encoding="utf-8")
    return repo


def _mut(mid, original, mutated, area="lay_liability", **extra):
    entry = {
        "id": mid, "module": "calc", "area": area, "file": "calc.py",
        "original": original, "mutated": mutated,
        "tests": ["tests/test_calc.py"], "rationale": "test",
    }
    entry.update(extra)
    return entry


def _run(repo: Path, tmp_path: Path, *extra) -> tuple[int, dict]:
    out = tmp_path / "report.json"
    rc = runner.main(["--repo-root", str(repo), "--output", str(out), "--timeout-sec", "60", *extra])
    report = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    return rc, report


LIAB = "    return stake * (price - 1.0)"


def test_una_mutazione_intercettata_fa_passare_il_gate(tmp_path):
    repo = _repo(tmp_path, [_mut("liab_stake", LIAB, "    return stake")])
    rc, report = _run(repo, tmp_path)
    assert report["results"][0]["status"] == runner.KILLED
    assert report["summary"]["valid"] == 1 and report["summary"]["score"] == 100.0
    assert rc == 0 and report["summary"]["gate_passed"] is True


def test_una_mutazione_sopravvissuta_fa_fallire_il_gate(tmp_path):
    repo = _repo(
        tmp_path,
        [_mut("lay_inverted", '    return side == "LAY"', '    return side != "LAY"', area="side")],
        areas=("side",),
    )
    rc, report = _run(repo, tmp_path)
    assert report["results"][0]["status"] == runner.SURVIVED
    assert report["summary"]["score"] == 0.0
    assert rc == 1 and report["summary"]["gate_passed"] is False


def test_il_repository_non_viene_mai_modificato_e_il_file_torna_originale(tmp_path):
    # Due mutazioni sullo stesso file: la seconda trova la riga originale solo se
    # la prima e' stata ripristinata nella sandbox.
    repo = _repo(tmp_path, [
        _mut("liab_stake", LIAB, "    return stake"),
        _mut("liab_zero", LIAB, "    return 0.0"),
    ])
    before = hashlib.sha256((repo / "calc.py").read_bytes()).hexdigest()
    rc, report = _run(repo, tmp_path)
    assert [r["status"] for r in report["results"]] == [runner.KILLED, runner.KILLED]
    assert hashlib.sha256((repo / "calc.py").read_bytes()).hexdigest() == before
    assert rc == 0


def test_il_timeout_non_e_mai_un_pass(tmp_path):
    lento = TEST_FORTE + "\n\nimport os, time\n\ndef test_lento():\n    if os.path.exists('MUTATED'):\n        time.sleep(30)\n"
    repo = _repo(tmp_path, [_mut("liab_slow", LIAB, "    open('MUTATED', 'w').close(); return stake * (price - 1.0)")],
                 test_src=lento)
    out = tmp_path / "report.json"
    rc = runner.main(["--repo-root", str(repo), "--output", str(out), "--timeout-sec", "3"])
    report = json.loads(out.read_text(encoding="utf-8"))
    assert report["results"][0]["status"] == runner.INCONCLUSIVE
    assert "timeout" in report["results"][0]["detail"]
    assert rc == 1 and report["summary"]["gate_passed"] is False


def test_un_errore_di_pytest_non_conta_come_mutazione_uccisa(tmp_path):
    # La mutazione rompe l'import del modulo: pytest esce con rc=2 (errore di
    # collezione), che non e' un test fallito.
    repo = _repo(tmp_path, [_mut("liab_import", LIAB, "    return stake\nimport modulo_inesistente_525")])
    rc, report = _run(repo, tmp_path)
    assert report["results"][0]["status"] == runner.INCONCLUSIVE
    assert rc == 1 and report["summary"]["gate_passed"] is False


def test_baseline_rossa_rende_la_mutazione_inconcludente(tmp_path):
    rotto = "from calc import liability\n\ndef test_rotto():\n    assert liability(10.0, 3.0) == 99.0\n"
    repo = _repo(tmp_path, [_mut("liab_stake", LIAB, "    return stake")], test_src=rotto)
    rc, report = _run(repo, tmp_path)
    assert report["results"][0]["status"] == runner.INCONCLUSIVE
    assert "baseline" in report["results"][0]["detail"]
    assert rc == 1


@pytest.mark.parametrize("original", ["    return stake * (price - 9.0)", "    return"])
def test_riga_assente_o_ripetuta_e_not_applicable_e_blocca(tmp_path, original):
    repo = _repo(tmp_path, [_mut("liab_na", original, "    return stake")])
    rc, report = _run(repo, tmp_path)
    assert report["results"][0]["status"] == runner.NOT_APPLICABLE
    assert rc == 1 and report["summary"]["gate_passed"] is False


def test_sorgente_mutato_che_non_compila_e_invalid_non_killed(tmp_path):
    repo = _repo(tmp_path, [_mut("liab_syntax", LIAB, "    return stake *(")])
    rc, report = _run(repo, tmp_path)
    assert report["results"][0]["status"] == runner.INVALID
    assert rc == 1 and report["summary"]["valid"] == 0


def test_nessuna_mutazione_selezionata_non_e_pass(tmp_path):
    repo = _repo(tmp_path, [_mut("liab_stake", LIAB, "    return stake")])
    rc, _ = _run(repo, tmp_path, "--module", "modulo_che_non_esiste")
    assert rc == 2


def test_solo_equivalenti_non_e_pass(tmp_path):
    repo = _repo(tmp_path, [_mut("liab_eq", LIAB, "    return (price - 1.0) * stake", equivalent="commutativita'")])
    rc, report = _run(repo, tmp_path)
    assert report["results"][0]["status"] == runner.EQUIVALENT
    assert report["summary"]["valid"] == 0
    assert rc == 1


def test_area_obbligatoria_senza_mutazione_valida_blocca(tmp_path):
    repo = _repo(tmp_path, [_mut("liab_stake", LIAB, "    return stake")], areas=("lay_liability", "dedupe"))
    rc, report = _run(repo, tmp_path)
    assert report["summary"]["uncovered_required_areas"] == ["dedupe"]
    assert rc == 1


def test_soglia_80_applicata_sul_punteggio_delle_sole_valide(tmp_path):
    # 4 uccise + 1 sopravvissuta = 80%: passa esattamente alla soglia;
    # con --fail-under 81 deve fallire.
    muts = [
        _mut(f"liab_{i}", LIAB, f"    return {v}") for i, v in enumerate(["stake", "0.0", "price", "stake * price"])
    ] + [_mut("lay_inv", '    return side == "LAY"', '    return side != "LAY"')]
    repo = _repo(tmp_path, muts)
    rc, report = _run(repo, tmp_path, "--fail-under", "80")
    assert report["summary"]["score"] == 80.0 and rc == 0
    rc81, _ = _run(repo, tmp_path, "--fail-under", "81")
    assert rc81 == 1


# --------------------------------------------------------------------------
# Catalogo reale del repository
# --------------------------------------------------------------------------

def test_il_catalogo_reale_e_applicabile_e_copre_le_aree_critiche():
    catalog = runner.load_catalog(ROOT)
    required = set(catalog["required_areas"])
    assert {
        "side", "lay_liability", "exposure", "dedupe", "ambiguous",
        "recovery", "reconciliation", "cashout", "funds", "money_management",
    } <= required
    covered = set()
    for entry in catalog["mutations"]:
        source = (ROOT / entry["file"]).read_text(encoding="utf-8")
        assert source.count(entry["original"]) == 1, f"{entry['id']}: riga originale non unica o assente"
        compile(source.replace(entry["original"], entry["mutated"], 1), entry["file"], "exec")
        for test in entry["tests"]:
            assert (ROOT / test.split("::")[0]).exists(), f"{entry['id']}: test mancante {test}"
        covered.add(entry["area"])
    assert required <= covered


def test_al_timeout_non_restano_processi_orfani(tmp_path):
    import os
    import time

    pidfile = tmp_path / "child.pid"
    orfano = (
        "import os, subprocess, time\n"
        "def test_figlio():\n"
        "    if os.path.exists('MUTATED'):\n"
        f"        p = subprocess.Popen(['sleep', '60'])\n"
        f"        open({str(pidfile)!r}, 'w').write(str(p.pid))\n"
        "        time.sleep(60)\n"
    )
    repo = _repo(tmp_path, [_mut("liab_child", LIAB, "    open('MUTATED', 'w').close(); return stake * (price - 1.0)")],
                 test_src=TEST_FORTE + "\n" + orfano)
    out = tmp_path / "report.json"
    rc = runner.main(["--repo-root", str(repo), "--output", str(out), "--timeout-sec", "4"])
    assert rc == 1
    pid = int(pidfile.read_text())
    time.sleep(0.5)
    try:
        os.kill(pid, 0)
        vivo = True
    except ProcessLookupError:
        vivo = False
    if vivo:  # zombie gia' terminato ma non raccolto: controlla lo stato
        stato = Path(f"/proc/{pid}/stat").read_text().split()[2] if Path(f"/proc/{pid}/stat").exists() else "X"
        vivo = stato not in ("Z", "X")
    assert not vivo, "il processo lanciato dal test e' sopravvissuto al timeout"


def test_un_run_che_fallisce_non_lascia_un_vecchio_report_pass(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    out = tmp_path / "report.json"
    out.write_text(json.dumps({"summary": {"gate_passed": True}}), encoding="utf-8")
    with pytest.raises(runner.MutationRunnerError):
        runner.main(["--repo-root", str(repo), "--output", str(out)])  # catalogo assente
    assert json.loads(out.read_text(encoding="utf-8"))["summary"]["gate_passed"] is False


def test_il_report_elenca_i_moduli_con_specifica_ma_senza_mutazioni(tmp_path):
    repo = _repo(tmp_path, [_mut("liab_stake", LIAB, "    return stake")])
    (repo / "guardrails" / "mutations").mkdir()
    (repo / "guardrails" / "mutations" / "pnl_engine.json").write_text(json.dumps({"module": "pnl_engine"}), encoding="utf-8")
    (repo / "guardrails" / "mutations" / "calc.json").write_text(json.dumps({"module": "calc"}), encoding="utf-8")
    rc, report = _run(repo, tmp_path)
    assert rc == 0
    assert report["summary"]["spec_modules_without_mutations"] == ["pnl_engine"]


@pytest.mark.parametrize("campo,valore", [
    ("file", "/etc/hostname"), ("file", "../fuori.py"), ("file", "sub/../../fuori.py"),
    ("tests", ["/tmp/test_x.py"]), ("tests", ["../tests/test_x.py::test_a"]),
])
def test_percorsi_fuori_dal_repository_sono_rifiutati(tmp_path, campo, valore):
    entry = _mut("liab_stake", LIAB, "    return stake")
    entry[campo] = valore
    repo = _repo(tmp_path, [entry])
    with pytest.raises(runner.MutationRunnerError, match="outside the repository"):
        runner.main(["--repo-root", str(repo), "--output", str(tmp_path / "r.json")])


def test_il_catalogo_attiva_la_lane_mutation_nel_dynamic():
    sys.path.insert(0, str(ROOT / "scripts"))
    import ci_changed_modules  # noqa: E402

    paths = next(r["paths"] for r in ci_changed_modules.MODULE_RULES if r["name"] == "mutation-guardrails")
    catalog = runner.load_catalog(ROOT)
    for f in {"guardrails/mutation_catalog.json", *(e["file"] for e in catalog["mutations"])}:
        assert any(f.startswith(p) for p in paths), f"{f} non attiva mutation-guardrails"


def test_il_timeout_funziona_anche_senza_killpg(tmp_path, monkeypatch):
    # Windows non ha os.killpg ne' signal.SIGKILL: il cleanup non deve sollevare
    # AttributeError e il timeout resta INCONCLUSIVE (rilievo GPT-6.1 Sol su #527).
    monkeypatch.delattr(runner.os, "killpg", raising=False)
    lento = TEST_FORTE + "\n\nimport os, time\n\ndef test_lento():\n    if os.path.exists('MUTATED'):\n        time.sleep(30)\n"
    repo = _repo(tmp_path, [_mut("liab_slow", LIAB, "    open('MUTATED', 'w').close(); return stake * (price - 1.0)")],
                 test_src=lento)
    out = tmp_path / "report.json"
    rc = runner.main(["--repo-root", str(repo), "--output", str(out), "--timeout-sec", "3"])
    report = json.loads(out.read_text(encoding="utf-8"))
    assert report["results"][0]["status"] == runner.INCONCLUSIVE
    assert rc == 1


# ---------------------------------------------------------------------------
# PFC-003 (M157): mutation in parallelo con --jobs, stesso esito del seriale
# ---------------------------------------------------------------------------
def _quattro_mutazioni():
    return [
        _mut("liab_stake", LIAB, "    return stake"),
        _mut("lay_inverted", '    return side == "LAY"', '    return side != "LAY"', area="side"),
        _mut("liab_zero", LIAB, "    return 0.0"),
        _mut("liab_double", LIAB, "    return stake * (price - 1.0) * 2"),
    ]


def test_jobs_danno_lo_stesso_report_del_seriale_nello_stesso_ordine(tmp_path):
    repo = _repo(tmp_path, _quattro_mutazioni(), areas=("lay_liability", "side"))
    rc1, seriale = _run(repo, tmp_path / "s", "--jobs", "1")
    rc3, parallelo = _run(repo, tmp_path / "p", "--jobs", "3")
    chiave = [(r["mutation_id"], r["status"]) for r in seriale["results"]]
    assert chiave == [(r["mutation_id"], r["status"]) for r in parallelo["results"]]
    assert chiave == [("liab_stake", runner.KILLED), ("lay_inverted", runner.SURVIVED),
                      ("liab_zero", runner.KILLED), ("liab_double", runner.KILLED)]
    assert seriale["summary"] == parallelo["summary"] and rc1 == rc3 == 1


def test_jobs_usano_sandbox_separate_e_lavorano_insieme(tmp_path, monkeypatch):
    # Tre mutazioni sullo stesso file in tre sandbox diverse, nello stesso
    # momento: la barriera si rompe (timeout) se non girano insieme, e una
    # sandbox condivisa vedrebbe il file mutato da un altro worker.
    import threading
    repo = _repo(tmp_path, _quattro_mutazioni()[:1] + [
        _mut("liab_zero", LIAB, "    return 0.0"), _mut("liab_double", LIAB, "    return stake * (price - 1.0) * 2")])
    originale = runner._run_pytest
    barriera = threading.Barrier(3, timeout=20)
    sandbox_viste = []

    def in_parallelo(sandbox, tests, pytest_args, timeout_sec):
        mutato = (sandbox / "calc.py").read_text(encoding="utf-8") != MODULO
        if mutato:
            sandbox_viste.append(str(sandbox))
            barriera.wait()
        return originale(sandbox, tests, pytest_args, timeout_sec)

    monkeypatch.setattr(runner, "_run_pytest", in_parallelo)
    rc, report = _run(repo, tmp_path, "--jobs", "3")
    assert len(set(sandbox_viste)) == 3
    assert [r["status"] for r in report["results"]] == [runner.KILLED] * 3 and rc == 0


@pytest.mark.parametrize("jobs", ["0", "-2"])
def test_jobs_non_validi_rifiutati(tmp_path, jobs):
    repo = _repo(tmp_path, _quattro_mutazioni()[:1])
    with pytest.raises(SystemExit):
        runner.main(["--repo-root", str(repo), "--output", str(tmp_path / "r.json"), "--jobs", jobs])


def test_con_i_jobs_il_repository_non_viene_modificato(tmp_path):
    repo = _repo(tmp_path, _quattro_mutazioni(), areas=("lay_liability", "side"))
    before = hashlib.sha256((repo / "calc.py").read_bytes()).hexdigest()
    _run(repo, tmp_path, "--jobs", "4")
    assert hashlib.sha256((repo / "calc.py").read_bytes()).hexdigest() == before
