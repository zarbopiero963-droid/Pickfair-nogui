import json
import os
import subprocess
import sys
from pathlib import Path


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
    # Copia del registro dal branch base. Identica al head: questi test non
    # riguardano la manomissione del registro (quella e' coperta in
    # tests/scripts/test_guardrail_check.py), ma senza la copia il confronto
    # e' impossibile e il gate blocca fail-closed.
    (tmp_path / "allowed_scope_base.json").write_text(
        json.dumps(scope), encoding="utf-8"
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

import shutil

import pytest

import yaml

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
    "base_ref, atteso_blocca",
    [("", True), ("   ", False), ("main", False), ("release/2.0", False)],
    ids=["vuoto-blocca", "spazi-passa", "main-passa", "base-diversa-passa"],
)
def test_guardia_base_ref_blocca_solo_il_valore_vuoto(tmp_path, base_ref, atteso_blocca):
    script = tmp_path / "guardia.sh"
    script.write_text(_guardia_base_ref() + "\necho OK\n", encoding="utf-8")
    env = {"PATH": os.environ.get("PATH", ""), "PR_BASE_REF": base_ref}
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
    """La guardia e' inutile se arriva dopo il primo `origin/$PR_BASE_REF`."""
    wf = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    run = next(s["run"] for s in wf["jobs"]["guard"]["steps"] if SENTINELLA in s.get("run", ""))
    assert run.index(SENTINELLA) < run.index("origin/${PR_BASE_REF"), (
        "la guardia sul ref di base arriva DOPO il primo uso di origin/$PR_BASE_REF"
    )
