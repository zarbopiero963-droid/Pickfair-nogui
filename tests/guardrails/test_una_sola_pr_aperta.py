"""Piu' di una PR aperta deve diventare rosso.

"UNA SOLA PR aperta / UN SOLO task attivo alla volta" sta fra le REGOLE NON
NEGOZIABILI di CLAUDE.md. E' stata violata due volte — #421 aperta mentre #420
era aperta, #422 con entrambe — senza che nulla in CI se ne accorgesse: il vecchio
`overlap-check` usciva 1 solo su cinque file critici scritti a mano.

Dal mandato PF-GOV-DOC-WORKFLOW-ALIGN-01 la regola non vive piu' inline nel
workflow: `.github/workflows/pr-overlap-guard.yml` chiama lo script canonico
`scripts/ci/pr_overlap_guard.sh` (lo stesso della lane Buildkite trusted, #563).
Questi test eseguono quel codice vero (`scripts/ci/trusted_pr_guard.py`) contro
un'API GitHub finta, e legano il workflow allo script: un workflow che tornasse a
una copia inline, o che eseguisse il codice della PR, fa fallire il test.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "ci"))

import trusted_pr_guard as g  # noqa: E402

WORKFLOW = ".github/workflows/pr-overlap-guard.yml"
REPO = g.TRUSTED_REPO
HEAD = "a" * 40
BASE = "b" * 40
TOKEN = "ghs_" + "x" * 36


def _pr(n: int, current: int, draft: bool = False) -> Dict[str, Any]:
    return {"number": n, "state": "open", "draft": draft, "labels": [], "changed_files": 1,
            "url": f"{g.API_ROOT}/repos/{REPO}/pulls/{n}",
            "head": {"sha": HEAD if n == current else "c" * 40, "repo": {"full_name": REPO}},
            "base": {"sha": BASE, "ref": "main", "repo": {"full_name": REPO}}}


def _transport(open_numbers: List[int], current: int, drafts: List[int]):
    """GitHub REST minimo: elenco delle PR aperte, singola PR, file, compare della base."""
    prs = {n: _pr(n, current, n in drafts) for n in open_numbers}

    def call(url: str, headers: Dict[str, str]):
        bare = url[len(g.API_ROOT):].split("?")[0]
        if bare == f"/repos/{REPO}":
            return 200, {}, b"{}"
        if bare == f"/repos/{REPO}/pulls":
            return 200, {}, json.dumps(list(prs.values())).encode()
        if bare.startswith(f"/repos/{REPO}/compare/"):
            return 200, {}, b'{"status": "ahead"}'
        m = re.fullmatch(rf"/repos/{re.escape(REPO)}/pulls/(\d+)(/files)?", bare)
        if m and int(m.group(1)) in prs:
            if m.group(2):
                return 200, {}, json.dumps([{"filename": f"docs/pr{m.group(1)}.md"}]).encode()
            return 200, {}, json.dumps(prs[int(m.group(1))]).encode()
        return 404, {}, b""
    return call


def _overlap(tmp_path: Path, open_numbers: List[int], current: int, drafts: List[int] | None = None) -> dict:
    """Esegue il controllo overlap reale (`trusted_pr_guard.main`) con `current` come PR valutata."""
    token = tmp_path / "tok"
    token.write_text(TOKEN)
    token.chmod(0o600)
    env = {"PF_PR_NUMBER": str(current), "PF_PR_HEAD_SHA": HEAD, "PF_GITHUB_TOKEN_FILE": str(token),
           "PF_RESULT_DIR": str(tmp_path / "out")}
    rc = g.main("overlap", [], env, transport=_transport(open_numbers, current, drafts or []))
    out = json.loads((tmp_path / "out" / "overlap.json").read_text())
    out["rc"] = rc
    return out


def _di_troppo(res: dict) -> List[int]:
    for r in res["reasons"]:
        m = re.search(r"di troppo (.+)$", r)
        if m:
            return sorted(int(x) for x in re.findall(r"#(\d+)", m.group(1)))
    return []


def test_pass_una_sola_pr_aperta_va_bene(tmp_path) -> None:
    res = _overlap(tmp_path, [420], 420)
    assert res["rc"] == 0 and res["verdict"] == "PASS", res


def test_block_nessuna_pr_aperta_e_fail_closed(tmp_path) -> None:
    """Prima: lista vuota = verde. Ora la PR valutata deve risultare aperta, altrimenti FAIL (fail-closed)."""
    res = _overlap(tmp_path, [], 420)
    assert res["rc"] == 1 and res["verdict"] == "FAIL"


def test_block_due_pr_aperte_sono_una_di_troppo(tmp_path) -> None:
    res = _overlap(tmp_path, [420, 421], 421)
    assert res["rc"] == 1 and _di_troppo(res) == [421]


def test_block_il_caso_reale_tre_pr_aperte(tmp_path) -> None:
    """Esattamente la situazione del 2026-08-19, passata verde."""
    res = _overlap(tmp_path, [420, 421, 422], 422)
    assert res["rc"] == 1 and _di_troppo(res) == [421, 422]


@pytest.mark.parametrize("current", [420, 421])
def test_block_di_troppo_sono_le_successive_alla_piu_vecchia(tmp_path, current) -> None:
    """La piu' vecchia e' quella legittima, a prescindere da quale sta girando: il check e' rosso su TUTTE."""
    res = _overlap(tmp_path, [420, 421, 422], current)
    assert res["rc"] == 1 and _di_troppo(res) == [421, 422]


def test_block_l_ordine_della_lista_non_conta(tmp_path) -> None:
    res = _overlap(tmp_path, [422, 420, 421], 422)
    assert res["rc"] == 1 and _di_troppo(res) == [421, 422]


def test_block_una_draft_conta_come_pr_aperta(tmp_path) -> None:
    res = _overlap(tmp_path, [420, 421], 421, drafts=[421])
    assert res["rc"] == 1 and _di_troppo(res) == [421]


# --------------------------------------------------------------------------------------------- workflow ↔ script
DIAGNOSTIC = ".github/workflows/pr-overlap-guard-diagnostic.yml"


def _load(path: str) -> dict:
    return yaml.safe_load((ROOT / path).read_text(encoding="utf-8"))


def _on(data: dict) -> dict:
    """YAML 1.1 legge la chiave `on` come True."""
    on = data.get("on", data.get(True))
    return on if isinstance(on, dict) else {k: None for k in ([on] if isinstance(on, str) else on)}


def _job() -> dict:
    return _load(WORKFLOW)["jobs"]["overlap-check"]


def _diag_job() -> dict:
    return _load(DIAGNOSTIC)["jobs"]["overlap-diagnostic"]


def _run_step(job: dict) -> dict:
    return next(s for s in job["steps"] if "pr_overlap_guard.sh" in s.get("run", ""))


def test_workflow_calls_the_canonical_trusted_script_and_keeps_the_check_name() -> None:
    """Il nome del job e' il nome del check (required checks invariati): resta `overlap-check`."""
    assert "name" not in _job(), "il check run prende il nome dal job id `overlap-check`"
    runs = "\n".join(s.get("run", "") for s in _job()["steps"])
    assert "./scripts/ci/pr_overlap_guard.sh" in runs
    assert "python" not in runs, "nessuna copia inline della logica nel workflow"


def test_required_check_runs_only_on_pull_request() -> None:
    """Il check obbligatorio nasce solo da `pull_request`: nessun dispatch manuale puo' pubblicare un secondo
    `overlap-check` sull'head della PR (su #564 un dispatch dal branch aveva messo un PASS accanto al FAIL)."""
    on = _on(_load(WORKFLOW))
    assert set(on) == {"pull_request"}
    assert set(on["pull_request"]["types"]) == {"opened", "synchronize", "reopened"}


def test_workflow_never_checks_out_or_runs_the_pr_code() -> None:
    checkout = next(s for s in _job()["steps"] if str(s.get("uses", "")).startswith("actions/checkout"))
    assert checkout["with"]["ref"] == "${{ github.event.pull_request.base.sha }}"
    assert checkout["with"]["persist-credentials"] is False
    text = (ROOT / WORKFLOW).read_text(encoding="utf-8")
    assert "pull_request_target" not in text and "refs/pull/" not in text and "/merge" not in text
    env = _run_step(_job())["env"]
    assert env["PF_PR_NUMBER"] == "${{ github.event.pull_request.number }}"
    assert env["PF_PR_HEAD_SHA"] == "${{ github.event.pull_request.head.sha }}"


@pytest.mark.parametrize("path,job", [(WORKFLOW, "overlap-check"), (DIAGNOSTIC, "overlap-diagnostic")])
def test_workflow_passes_the_token_only_as_a_0600_file(path, job) -> None:
    step = _run_step(_load(path)["jobs"][job])
    run = step["run"]
    assert "umask 077" in run and 'rm -f "$token_file"' in run and "unset GH_TOKEN" in run
    assert "PF_GITHUB_TOKEN_FILE=" in run
    assert not re.search(r"pr_overlap_guard\.sh\s+\S", run), "lo script non riceve argomenti"
    assert set(step["env"]) == {"GH_TOKEN", "PF_PR_NUMBER", "PF_PR_HEAD_SHA", "PF_RESULT_DIR"}
    lines = [ln.strip() for ln in run.splitlines() if "GH_TOKEN" in ln]
    assert lines == ["( umask 077 && printf '%s' \"$GH_TOKEN\" > \"$token_file\" )", "unset GH_TOKEN"]
    assert (ROOT / path).read_text(encoding="utf-8").count("github.token") == 1


@pytest.mark.parametrize("path", [WORKFLOW, DIAGNOSTIC])
def test_workflow_permissions_are_read_only(path) -> None:
    assert _load(path)["permissions"] == {"contents": "read", "pull-requests": "read"}


@pytest.mark.parametrize("path,job", [(WORKFLOW, "overlap-check"), (DIAGNOSTIC, "overlap-diagnostic")])
def test_workflow_cannot_turn_a_failure_green(path, job) -> None:
    """`continue-on-error` (job o step) renderebbe verde un FAIL dello script."""
    j = _load(path)["jobs"][job]
    assert "continue-on-error" not in j and all("continue-on-error" not in s for s in j["steps"])
    assert set(j) <= {"name", "runs-on", "timeout-minutes", "steps"}, "niente permessi, `if` o `needs` sul job"


def test_required_workflow_has_one_checkout_and_one_guard_step_only() -> None:
    """Un secondo checkout (es. dell'head) o uno step in piu' che usa il token aprirebbero il codice non fidato."""
    steps = _job()["steps"]
    assert [str(s.get("uses", "")).split("@")[0] for s in steps] == ["actions/checkout", "", "actions/upload-artifact"]
    assert sum(1 for s in steps if "run" in s) == 1


# ------------------------------------------------------------------- diagnostica manuale ≠ controllo obbligatorio
def _check_names(path: Path) -> List[str]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return [str(j.get("name", jid)) for jid, j in (data.get("jobs") or {}).items()]


def test_only_the_pull_request_workflow_can_publish_overlap_check() -> None:
    """Nessun altro workflow del repository puo' pubblicare un check di nome `overlap-check`: un PASS di un'altra
    fonte sullo stesso SHA soddisferebbe il required check e coprirebbe il FAIL della PR."""
    owners = [p.name for p in sorted((ROOT / ".github" / "workflows").glob("*.y*ml"))
              if "overlap-check" in _check_names(p)]
    assert owners == ["pr-overlap-guard.yml"]


def test_diagnostic_has_a_distinct_check_identity() -> None:
    data = _load(DIAGNOSTIC)
    assert set(_on(data)) == {"workflow_dispatch"}
    assert _check_names(ROOT / DIAGNOSTIC) == ["overlap-diagnostic"]
    assert "overlap-check" not in (ROOT / DIAGNOSTIC).read_text(encoding="utf-8").split("jobs:")[1]
    assert data["name"] != _load(WORKFLOW)["name"]


def test_diagnostic_refuses_the_ref_before_any_checkout() -> None:
    steps = _diag_job()["steps"]
    assert "refs/heads/main" in steps[0].get("run", "") and "uses" not in steps[0]
    assert [str(s.get("uses", "")).split("@")[0] for s in steps] == ["", "actions/checkout", "", "actions/upload-artifact"]
    assert steps[1]["with"] == {"ref": "${{ github.sha }}", "persist-credentials": False}


def _ref_gate_rc(ref: str) -> int:
    import subprocess
    gate = _diag_job()["steps"][0]["run"]
    env = {"PATH": "/usr/bin:/bin", "GITHUB_REF": ref}
    return subprocess.run(["bash", "-e", "-c", gate], env=env, capture_output=True).returncode


@pytest.mark.parametrize("ref,rc", [
    ("refs/heads/main", 0),
    ("refs/heads/ccr-764b0316-9r1a1d", 1),
    ("refs/heads/mainx", 1),
    ("refs/tags/main", 1),
    ("refs/pull/564/merge", 1),
    ("", 1),
])
def test_diagnostic_runs_only_from_main(ref, rc) -> None:
    """Da un branch girerebbe lo script del branch: il job deve fallire prima del checkout e del token."""
    assert _ref_gate_rc(ref) == rc


def _rollup_item(name: str, conclusion: str) -> Dict[str, Any]:
    return {"__typename": "CheckRun", "name": name, "status": "COMPLETED", "conclusion": conclusion,
            "detailsUrl": f"https://github.com/{REPO}/actions/runs/1/job/1"}


@pytest.mark.parametrize("extra", [
    _rollup_item("overlap-diagnostic", "SUCCESS"),
    _rollup_item("overlap-check", "SUCCESS"),
])
def test_block_a_manual_pass_never_masks_the_pull_request_failure(extra) -> None:
    """Merge readiness legge ogni check run dell'head: un PASS diagnostico, o perfino un secondo `overlap-check`
    verde (il caso reale di #564 su b4e6604, due check suite della stessa app), non cancella il FAIL."""
    sys.path.insert(0, str(ROOT / "scripts"))
    import pr_flow_automation as fa
    for rollup in ([_rollup_item("overlap-check", "FAILURE"), extra], [extra, _rollup_item("overlap-check", "FAILURE")]):
        res = fa.split_checks({"statusCheckRollup": rollup})
        assert [b["name"] for b in res["blockers"]] == ["overlap-check"]
