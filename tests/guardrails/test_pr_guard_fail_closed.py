import copy
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "guardrail_check.py"


def _run_guard(
    tmp_path: Path,
    title: str,
    *,
    changed_files: list[dict] | None = None,
) -> subprocess.CompletedProcess[str]:
    (tmp_path / ".guardrails").mkdir()
    (tmp_path / "pr_meta.json").write_text(
        json.dumps({"title": title, "body": ""}),
        encoding="utf-8",
    )
    files = changed_files or [
        {"filename": ".github/workflows/pr-guard.yml"},
        {"filename": "scripts/guardrail_check.py"},
        {"filename": ".guardrails/allowed_scope.json"},
        {"filename": "tests/guardrails/test_pr_guard_fail_closed.py"},
    ]
    (tmp_path / "pr_files_raw.json").write_text(
        json.dumps(files),
        encoding="utf-8",
    )
    # `files` di ogni task registrato = i file cambiati sopra: dall'invariante
    # di scope (guardrail_check.validate_declared_scope) una dichiarazione che
    # non coincide col diff blocca, e questi test non parlano di quello —
    # parlano di QUALI marker vengono riconosciuti come task key valide.
    scope_files = [f["filename"] for f in files]
    # Le chiavi sono normalizzate in minuscolo da resolve_task, quindi le
    # varianti mixed-case dei test risolvono a queste stesse entry. Registrarle
    # e' ora obbligatorio: un task senza entry non ha scope dichiarato.
    task_entry = {"files": scope_files, "max_files": len(scope_files), "allow_tests": False}
    scope = {
        "default": {"max_files": 8, "allow_tests": True},
        "tasks": {
            "pr_guard": dict(task_entry),
            "workflow_hygiene_pr1_comment_noise": dict(task_entry),
            "claude_bug_pr1a_telegram_sender_escape_queue": dict(task_entry),
        },
    }
    (tmp_path / ".guardrails" / "allowed_scope.json").write_text(
        json.dumps(scope), encoding="utf-8"
    )
    # Copia del registro dal branch base: come il head MA SENZA la entry del
    # task sotto test, cioe' lo stato normale — la PR ha registrato la propria
    # chiave. Una entry identica al base significherebbe "riuso di
    # un'autorizzazione altrui" e bloccherebbe (P1 Codex, terzo giro). La
    # manomissione del registro e' coperta in tests/scripts/test_guardrail_check.py.
    base = copy.deepcopy(scope)
    marker = re.search(r"\[TASK:\s*([^\]]+)\]", title, re.I)
    if marker:
        base["tasks"].pop(marker.group(1).strip().lower(), None)
    (tmp_path / "allowed_scope_base.json").write_text(
        json.dumps(base), encoding="utf-8"
    )
    return subprocess.run(
        [sys.executable, str(SCRIPT_PATH)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )


def test_missing_task_non_critical_fails_closed(tmp_path: Path):
    result = _run_guard(tmp_path, title="Guardrail update")
    assert result.returncode != 0
    assert "TASK validation is fail-closed" in result.stdout


def test_missing_task_critical_fails(tmp_path: Path):
    result = _run_guard(
        tmp_path,
        title="Critical runtime update",
        changed_files=[{"filename": "core/runtime_controller.py"}],
    )
    assert result.returncode != 0


def test_unknown_task_non_critical_fails_closed(tmp_path: Path):
    result = _run_guard(tmp_path, title="[TASK: not_real] Guardrail update")
    assert result.returncode != 0
    assert "Unknown TASK tag" in result.stdout


def test_valid_task_passes(tmp_path: Path):
    result = _run_guard(tmp_path, title="[TASK: pr_guard] Guardrail update")
    assert result.returncode == 0
    assert "✅ PR guard completed" in result.stdout


def test_workflow_hygiene_task_passes(tmp_path: Path):
    result = _run_guard(tmp_path, title="[TASK: workflow_hygiene_pr1_comment_noise] Guardrail update")
    assert result.returncode == 0
    assert "TASK source found" in result.stdout


def test_workflow_hygiene_task_mixed_case_passes(tmp_path: Path):
    result = _run_guard(tmp_path, title="[TASK: WorkFlow_Hygiene_PR1_Comment_Noise] Guardrail update")
    assert result.returncode == 0


def test_unknown_non_workflow_hygiene_task_still_fails_closed(tmp_path: Path):
    result = _run_guard(tmp_path, title="[TASK: workflow_hygieneX_pr1] Guardrail update")
    assert result.returncode != 0
    assert "Unknown TASK tag" in result.stdout


def test_claude_bug_task_passes(tmp_path: Path):
    result = _run_guard(
        tmp_path,
        title="[TASK: claude_bug_pr1a_telegram_sender_escape_queue] Guardrail update",
    )
    assert result.returncode == 0
    assert "TASK source found" in result.stdout


def test_claude_bug_task_mixed_case_passes(tmp_path: Path):
    result = _run_guard(
        tmp_path,
        title="[TASK: Claude_Bug_PR1A_Telegram_Sender_Escape_Queue] Guardrail update",
    )
    assert result.returncode == 0


def test_unknown_claude_bug_like_task_fails_closed(tmp_path: Path):
    result = _run_guard(
        tmp_path,
        title="[TASK: claude_bug_prx_telegram_sender_escape_queue] Guardrail update",
    )
    assert result.returncode != 0
    assert "Unknown TASK tag" in result.stdout


# ---------------------------------------------------------------------------
# La guardia sul ref di base, ESEGUITA davvero (non asserita a stringhe)
#
# Rilievo di Claude Fable 5 sulla #470: tutto lo step di metadata confronta
# contro `origin/$PR_BASE_REF`, e i `:-main` sparsi al suo interno farebbero
# ricadere il confronto su `main` anche per una PR con base diversa —
# indebolendo in silenzio l'anti-tampering sul registro.
#
# Il test estrae la guardia dal workflow REALE e la manda in esecuzione a bash:
# se un giorno qualcuno la toglie, il test non "non trova piu' la stringa" —
# esegue quel che resta e vede che non blocca piu'.
# ---------------------------------------------------------------------------

WORKFLOW = REPO_ROOT / ".github" / "workflows" / "pr-guard.yml"
SENTINELLA = "# --- fine guardia base_ref ---"


def _guardia_base_ref() -> str:
    """Le righe dello step di metadata fino alla sentinella, dal workflow vero."""
    wf = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    for step in wf["jobs"]["guard"]["steps"]:
        run = step.get("run", "")
        if SENTINELLA in run:
            return run.split(SENTINELLA)[0]
    raise AssertionError(
        f"nessuno step di pr-guard.yml contiene {SENTINELLA!r}: la guardia "
        "sul ref di base e' stata rimossa o rinominata"
    )


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash non disponibile")
@pytest.mark.parametrize(
    "base_ref, base_sha, atteso_blocca",
    [
        ("", "abc123", True),
        ("main", "", True),
        ("", "", True),
        ("   ", "abc123", False),
        ("main", "abc123", False),
        ("release/2.0", "abc123", False),
    ],
    ids=["ref-vuoto-blocca", "sha-vuoto-blocca", "entrambi-vuoti-blocca",
         "spazi-passa", "main-passa", "base-diversa-passa"],
)
def test_guardia_base_ref_blocca_solo_il_valore_vuoto(tmp_path, base_ref, base_sha, atteso_blocca):
    script = tmp_path / "guardia.sh"
    script.write_text(_guardia_base_ref() + "\necho OK\n", encoding="utf-8")
    env = {"PATH": os.environ.get("PATH", ""),
           "PR_BASE_REF": base_ref, "PR_BASE_SHA": base_sha}
    res = subprocess.run(
        ["bash", str(script)], capture_output=True, text=True, env=env, check=False
    )
    if atteso_blocca:
        assert res.returncode != 0, f"la guardia NON ha bloccato con PR_BASE_REF={base_ref!r}"
        assert "fail-closed" in res.stderr
    else:
        assert res.returncode == 0, f"la guardia ha bloccato a torto: {res.stderr}"
        assert "OK" in res.stdout


def test_la_guardia_precede_ogni_uso_del_ref_di_base():
    """La guardia e' inutile se arriva dopo il primo uso del base."""
    wf = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    run = next(s["run"] for s in wf["jobs"]["guard"]["steps"] if SENTINELLA in s.get("run", ""))
    dopo = run.split(SENTINELLA, 1)[1]
    assert "${PR_BASE_SHA}" in dopo, (
        "nessun uso di $PR_BASE_SHA dopo la guardia: o il confronto col base e' "
        "sparito, o e' tornato a un riferimento mobile"
    )
    prima = run.split(SENTINELLA, 1)[0]
    for riferimento in ("${PR_BASE_SHA}", "origin/${PR_BASE_REF"):
        righe_vive = [r for r in prima.splitlines() if not r.strip().startswith("#")]
        assert riferimento not in "\n".join(righe_vive), (
            f"{riferimento} e' usato PRIMA della guardia: la guardia non protegge nulla"
        )


# ---------------------------------------------------------------------------
# Il guard non deve poter essere scavalcato da un modulo fratello
#
# P1 di Codex sulla #470, riprodotto prima di essere corretto: il workflow
# lanciava `python scripts/guardrail_check.py`, che mette `scripts/` in
# sys.path[0]. Una PR altrimenti auto-mergiabile poteva aggiungere
# `scripts/json.py` — dichiarandolo regolarmente nei propri `files`, quindi
# passando l'invariante di scope — e quel file veniva importato al posto dello
# stdlib al primo `import json`, con facolta' di uscire 0 prima di qualunque
# validazione.
#
#     ### il guard non ha mai girato: sono json.py di scripts/ ###
#     exit=0
#
# Mettere `guardrail_check.py` nell'esclusione a merge manuale non bastava:
# proteggeva il file, non le sue dipendenze.
#
# Il test lancia il guard con l'invocazione REALE estratta dal workflow, dentro
# un albero avvelenato. Se qualcuno toglie l'isolamento, il test non cerca una
# stringa: vede il guard non girare.
# ---------------------------------------------------------------------------

def _comando_del_guard() -> list[str]:
    """La riga con cui il workflow lancia davvero guardrail_check.py."""
    wf = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    for step in wf["jobs"]["guard"]["steps"]:
        for riga in step.get("run", "").splitlines():
            riga = riga.strip()
            # I commenti nominano lo script per spiegarsi: non sono il comando.
            if riga.startswith("#") or "guardrail_check.py" not in riga:
                continue
            return riga.split()
    raise AssertionError("nessuno step di pr-guard.yml lancia scripts/guardrail_check.py")


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash non disponibile")
def test_un_modulo_fratello_ostile_non_scavalca_il_guard(tmp_path):
    (tmp_path / "scripts").mkdir()
    (tmp_path / ".guardrails").mkdir()
    shutil.copy(REPO_ROOT / "scripts" / "guardrail_check.py", tmp_path / "scripts")

    # Il modulo ostile: si chiama come una dipendenza del guard e sta accanto a lui.
    (tmp_path / "scripts" / "json.py").write_text(
        "import sys\nprint('SCAVALCATO')\nsys.exit(0)\n", encoding="utf-8"
    )

    scope = {"tasks": {"k": {"files": ["scripts/json.py"], "description": "d"}}}
    (tmp_path / "pr_meta.json").write_text(json.dumps({"title": "[TASK: k] x"}), encoding="utf-8")
    (tmp_path / "pr_files_raw.json").write_text(json.dumps(["scripts/json.py"]), encoding="utf-8")
    (tmp_path / ".guardrails" / "allowed_scope.json").write_text(json.dumps(scope), encoding="utf-8")
    (tmp_path / "allowed_scope_base.json").write_text(json.dumps(scope), encoding="utf-8")

    comando = _comando_del_guard()
    comando[0] = sys.executable          # `python` del workflow -> quello dei test
    res = subprocess.run(comando, cwd=tmp_path, capture_output=True, text=True, check=False)

    assert "SCAVALCATO" not in res.stdout, (
        "il modulo fratello ostile ha rimpiazzato una dipendenza del guard: "
        f"l'invocazione {' '.join(comando[1:])} non isola sys.path"
    )
    assert "PR GUARD REPORT" in res.stdout, (
        f"il guard non ha prodotto il proprio report: stdout={res.stdout!r}"
    )


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash non disponibile")
def test_anche_lo_step_metadata_gira_isolato(tmp_path):
    """Rilievo di Fugu Ultra sulla #470: l'isolamento del solo guard non basta.

    Lo step che costruisce i metadati gira PRIMA del guard e importa `json`,
    `os`, `subprocess`. Con `python - <<'PY'` il cwd — cioe' il checkout della
    PR — finisce in sys.path, quindi un `json.py` alla ROOT del repository
    esegue codice prima ancora che il guard parta. Riprodotto:

        ### lo step metadata e' stato avvelenato: sono json.py alla root ###
        exit=0

    Avevo chiuso la porta sul guard lasciando aperta la finestra uno step prima.
    """
    wf = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    heredoc = [s["run"] for s in wf["jobs"]["guard"]["steps"]
               if "<<'PY'" in s.get("run", "")]
    assert heredoc, "nessuno step di pr-guard.yml usa un heredoc python"

    for run in heredoc:
        riga = next(r.strip() for r in run.splitlines()
                    if "<<'PY'" in r and not r.strip().startswith("#"))
        # L'isolamento e' vano se qualcosa ha gia' spostato il cwd o iniettato
        # un PYTHONPATH nello stesso blocco `run` (rilievo Claude Fable 5).
        prima = run.split("<<'PY'", 1)[0]
        vive = [r for r in prima.splitlines() if not r.strip().startswith("#")]
        for veleno in ("cd ", "PYTHONPATH=", "PYTHONHOME="):
            assert veleno not in "\n".join(vive), (
                f"{veleno!r} compare prima dell'heredoc: l'isolamento di -I "
                "non protegge da un cwd spostato o da un path iniettato"
            )
        assert re.search(r"python3?\s+-I\s+-\s*<<'PY'", riga), (
            f"lo step heredoc non gira isolato: {riga!r}. Senza -I il cwd "
            "(il checkout della PR) entra in sys.path e un json.py alla root "
            "esegue codice prima del guard."
        )
