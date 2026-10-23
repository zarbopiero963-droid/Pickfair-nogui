"""PFC-002: contratto della CI dopo la deduplica.

Ogni test qui sotto fallisce se torna uno dei difetti misurati il 22/10/2026:
17 suite complete per push, workflow doppi push+pull_request sullo stesso
contenuto, orchestratori annidati, run superati non cancellati, e check
obbligatori che possono risultare verdi senza aver verificato nulla (job
saltato da `if:`, reviewer senza segreto "saltato", copertura non attestata).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WF = ROOT / ".github" / "workflows"
REVIEWERS = {
    "pr-review-openrouter-gpt56-sol.yml": "GPT-6.1 Sol push-range review",
    "pr-review-xai-grok46.yml": "Grok 4.7 push-range review",
}


def _wf(nome: str) -> dict:
    return yaml.safe_load((WF / nome).read_text(encoding="utf-8"))


def _on(d: dict) -> dict:
    blocco = d.get("on", d.get(True))
    if isinstance(blocco, str):
        return {blocco: None}
    if isinstance(blocco, list):
        return {k: None for k in blocco}
    return blocco or {}


def _tutti():
    return sorted(p.name for p in WF.glob("*.y*ml"))


def _run_di(d: dict) -> str:
    out = []
    for job in (d.get("jobs") or {}).values():
        for step in (job or {}).get("steps") or []:
            out.append(str((step or {}).get("run", "")))
    return "\n".join(out)


_FULL_PYTEST = re.compile(r"(?m)^\s*(?:python\s+-m\s+)?pytest(?:\s+-q)?(?:\s+-x)?\s*$")


def test_la_suite_completa_gira_in_un_solo_workflow_automatico():
    """Un `pytest` senza percorsi = l'intera suite. Fuori dai workflow solo manuali,
    lo si trova solo nel gate di full-suite (che passa da full_suite_gate.py)."""
    trovati = []
    for nome in _tutti():
        d = _wf(nome)
        if set(_on(d)) <= {"workflow_dispatch"}:
            continue  # diagnostica manuale (merge-simulation.yml)
        if _FULL_PYTEST.search(_run_di(d)):
            trovati.append(nome)
    assert trovati == [], f"suite completa ancora eseguita in: {trovati}"
    assert "full_suite_gate.py run" in _run_di(_wf("full-suite.yml"))


def test_nessun_workflow_gira_due_volte_su_push_e_pull_request():
    doppi = []
    for nome in _tutti():
        on = _on(_wf(nome))
        if "push" in on and "pull_request" in on:
            push = on["push"] or {}
            if "branches" not in push:
                doppi.append(nome)
            elif "main" not in push["branches"]:
                doppi.append(nome)
    assert doppi == [], f"push senza filtro branch accanto a pull_request: {doppi}"


def test_orchestratori_non_annidati_e_mutation_una_volta():
    master = (WF / "ci-master-gate.yml").read_text(encoding="utf-8")
    assert "ci-dynamic-intelligent.yml" not in master
    assert "merge-simulation-hard.yml" not in master
    dynamic = _wf("ci-dynamic-intelligent.yml")
    assert "workflow_call" not in _on(dynamic)
    chiamate = []
    for nome in _tutti():
        d = _wf(nome)
        on = _on(d)
        if "pull_request" not in on:
            continue
        for job in (d.get("jobs") or {}).values():
            uses = str((job or {}).get("uses", ""))
            if uses.endswith("mutation-guardrails.yml"):
                chiamate.append(nome)
    assert chiamate == ["ci-master-gate.yml"], chiamate
    # nessun altro workflow chiama ci-dynamic-intelligent o merge-simulation-hard
    for nome in _tutti():
        testo = (WF / nome).read_text(encoding="utf-8")
        assert "uses: ./.github/workflows/ci-dynamic-intelligent.yml" not in testo, nome
        assert "uses: ./.github/workflows/merge-simulation-hard.yml" not in testo, nome


def test_ultra_check_non_riesegue_la_suite_completa():
    d = _wf("_module-ultra-check.yml")
    nomi = [s.get("name", "") for s in d["jobs"]["ultra-check"]["steps"]]
    assert "Run full test suite (backstop)" not in nomi
    assert not _FULL_PYTEST.search(_run_di(d))


@pytest.mark.parametrize("nome", ["full-suite.yml", "ci-master-gate.yml", "ci-dynamic-intelligent.yml",
                                  "merge-simulation-hard.yml"])
def test_run_superati_cancellati_solo_sulle_pr(nome):
    c = _wf(nome).get("concurrency")
    assert isinstance(c, dict), f"{nome}: concurrency assente"
    assert str(c.get("cancel-in-progress")).replace(" ", "") == "${{github.event_name=='pull_request'}}", (
        f"{nome}: la cancellazione deve valere solo per pull_request, mai per push su main"
    )


def test_i_reviewer_non_cancellano_mai_un_run():
    for nome in REVIEWERS:
        assert _wf(nome)["concurrency"]["cancel-in-progress"] is False


# ---------------------------------------------------------------------------
# Check obbligatori: nomi reali, nessun verde senza verifica
# ---------------------------------------------------------------------------

def _required() -> list[str]:
    return json.loads((ROOT / "scripts" / "ci" / "required_checks.json").read_text(encoding="utf-8"))["required_checks"]


def test_i_check_obbligatori_corrispondono_a_job_reali():
    req = set(_required())
    assert _wf("full-suite.yml")["jobs"]["full-suite"]["name"] == "full-suite"
    master = _wf("ci-master-gate.yml")["jobs"]
    mut = _wf("mutation-guardrails.yml")["jobs"]
    nome_mutation = f"mutation-guardrails / {mut['mutation'].get('name', 'mutation')}"
    assert "mutation-guardrails" in master
    reali = {"full-suite", nome_mutation} | {_wf(n)["jobs"]["review"]["name"] for n in REVIEWERS}
    assert req == reali, f"manifest {sorted(req)} diverso dai job reali {sorted(reali)}"


def test_nessun_check_obbligatorio_puo_risultare_skipped_da_un_if():
    assert "if" not in _wf("full-suite.yml")["jobs"]["full-suite"]
    assert "if" not in _wf("ci-master-gate.yml")["jobs"]["mutation-guardrails"]
    assert "if" not in _wf("mutation-guardrails.yml")["jobs"]["mutation"]
    for nome in REVIEWERS:
        assert "if" not in _wf(nome)["jobs"]["review"], (
            f"{nome}: un job saltato da `if:` risulta skipped, e GitHub conta skipped come required soddisfatto"
        )


def test_full_suite_parte_su_ogni_pr_senza_filtri_e_senza_dispatch():
    on = _on(_wf("full-suite.yml"))
    pr = on["pull_request"] or {}
    assert not {"paths", "paths-ignore", "branches", "branches-ignore"} & set(pr)
    assert {"opened", "synchronize", "reopened"} <= set(pr.get("types") or [])
    assert "workflow_dispatch" not in on


@pytest.mark.parametrize("nome", sorted(REVIEWERS))
def test_reviewer_check_verde_solo_con_copertura_verificata(nome):
    d = _wf(nome)
    raw = (WF / nome).read_text(encoding="utf-8")
    assert d["permissions"].get("actions") == "read"
    steps = d["jobs"]["review"]["steps"]
    nomi = [s.get("name") for s in steps]
    assert nomi[-1] == "Verify review coverage of the whole PR"
    verifica = steps[-1]
    assert verifica.get("if") == "always()"
    assert "_base/scripts/ci/review_coverage.py verify" in verifica["run"]
    assert "set -euo pipefail" in verifica["run"]
    assert "|| true" not in verifica["run"]
    upload = next(s for s in steps if s.get("name") == "Upload review coverage attestation")
    assert upload["with"]["if-no-files-found"] == "error"
    # niente piu' "reviewer saltato, nessun check rosso"
    assert "nessun check rosso" not in raw
    # il codice del verificatore viene dal branch BASE, mai dalla PR
    checkout = steps[0]
    assert checkout["uses"].startswith("actions/checkout@")
    assert checkout["with"]["ref"] == "${{ github.event.pull_request.base.sha }}"
    assert checkout["with"]["persist-credentials"] is False
    assert checkout["with"]["sparse-checkout"].strip() == "scripts/ci/review_coverage.py"
    assert "github.event.pull_request.head.sha }}\n          path" not in raw
    assert sum(1 for s in steps if str(s.get("uses", "")).startswith("actions/checkout@")) == 1
    assert _on(d) == {"pull_request_target": {"types": ["opened", "synchronize", "reopened", "ready_for_review"]}}


@pytest.mark.parametrize("nome", sorted(REVIEWERS))
def test_reviewer_il_diff_non_viene_piu_troncato(nome):
    raw = (WF / nome).read_text(encoding="utf-8")
    assert "patch[:MAX_PATCH_PER_FILE_CHARS]" not in raw
    assert "rc.plan_review(" in raw and "rc.batch_outcome(" in raw and "rc.build_attestation(" in raw


# ---------------------------------------------------------------------------
# I file che definiscono i nuovi gate sono a merge owner (RED su main: solo
# scripts/ci/check_ci_quarantine.py lo era, e una PR poteva indebolire
# full_suite_gate.py facendo girare su di se' il check gia' indebolito).
# ---------------------------------------------------------------------------
GATE_PFC002 = [
    ".github/workflows/full-suite.yml",
    "scripts/ci/full_suite_gate.py",
    "scripts/ci/full_suite_plugin.py",
    "scripts/ci/full_suite_contract.json",
    "scripts/ci/required_checks.json",
    "scripts/ci/review_coverage.py",
    "scripts/ci/qualunque_nuovo_gate.py",
]


@pytest.mark.parametrize("path", GATE_PFC002)
def test_i_file_dei_gate_pfc002_sono_a_merge_owner(path):
    import sys
    sys.path.insert(0, str(ROOT / "scripts"))
    import pr_autonomy_policy as pol

    assert pol.is_owner_manual_path(path), f"{path}: deve chiedere il merge manuale dell'owner"


@pytest.mark.parametrize("nome", sorted(REVIEWERS))
@pytest.mark.parametrize("path", GATE_PFC002)
def test_i_reviewer_mettono_la_label_sui_gate_pfc002(nome, path):
    import ast
    import textwrap
    codice = _wf(nome)["jobs"]["review"]["steps"][1]["run"].split("python3 <<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
    albero = ast.parse(textwrap.dedent(codice))
    nodo = next(n for n in albero.body if isinstance(n, ast.Assign)
                and getattr(n.targets[0], "id", "") == "MANUAL_AUTHORITY_PATTERNS")
    patterns = ast.literal_eval(nodo.value)
    assert any(re.search(p, path) for p in patterns), f"{nome}: {path} senza manual-review-required"


def test_full_suite_prova_il_merge_con_il_main_corrente():
    # Grok 4.7 su #539: il pytest tolto da merge-simulation-hard provava il merge
    # con il main PIU' RECENTE. GPT-6.1 Sol su #539: il merge si costruisce
    # dall'head della PR (non da refs/pull/N/merge) e le dipendenze si
    # installano DOPO il merge.
    steps = _wf("full-suite.yml")["jobs"]["full-suite"]["steps"]
    nomi = [s.get("name") for s in steps]
    checkout = steps[nomi.index("Checkout")]
    assert checkout["with"]["ref"] == "${{ github.event.pull_request.head.sha || github.sha }}"
    assert checkout["with"]["fetch-depth"] == 0
    i_merge = nomi.index("Merge the current main into the PR head")
    i_install = nomi.index("Install dependencies")
    i_run = nomi.index("Run the full suite and judge the report")
    assert nomi.index("Checkout") < i_merge < i_install < i_run
    merge = steps[i_merge]
    assert merge.get("if") == "github.event_name == 'pull_request'"
    assert "git fetch --no-tags origin main" in merge["run"]
    assert re.search(r"merge --no-ff --no-edit origin/main \|\| \{", merge["run"])
    assert "exit 1" in merge["run"] and "set -euo pipefail" in merge["run"]
    assert "if" not in steps[i_run] and "if" not in steps[i_install]


@pytest.mark.parametrize("nome", sorted(REVIEWERS))
def test_il_timeout_del_job_regge_review_e_attesa_dei_predecessori(nome):
    # Rilievo Codex su #539: la verifica aspetta fino a --wait-seconds i run
    # ufficiali ancora in corso. Il caso peggiore della review (stesso calcolo di
    # test_ai_review_timeout) piu' l'attesa deve stare nel timeout del job.
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "timeout_reviewer", ROOT / "tests" / "guardrails" / "test_ai_review_timeout.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    workflow = f".github/workflows/{nome}"
    tentativi, limite, backoff = mod._attesa_modello(workflow)
    peggiore = tentativi * limite + backoff + mod._attesa_github(workflow) + mod.AVVIO_RUNNER
    verifica = _wf(nome)["jobs"]["review"]["steps"][-1]["run"]
    attesa = int(re.search(r"--wait-seconds (\d+)", verifica).group(1))
    assert 0 < attesa and peggiore + attesa + 120 <= mod._timeout_job(workflow), (nome, peggiore, attesa)
