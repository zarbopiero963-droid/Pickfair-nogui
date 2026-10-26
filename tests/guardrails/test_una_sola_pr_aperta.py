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
def _job() -> dict:
    data = yaml.safe_load((ROOT / WORKFLOW).read_text(encoding="utf-8"))
    return data["jobs"]["overlap-check"]


def test_workflow_calls_the_canonical_trusted_script_and_keeps_the_check_name() -> None:
    """Il nome del job e' il nome del check (required checks invariati): resta `overlap-check`."""
    runs = "\n".join(s.get("run", "") for s in _job()["steps"])
    assert "./scripts/ci/pr_overlap_guard.sh" in runs
    assert "python" not in runs, "nessuna copia inline della logica nel workflow"


def test_workflow_never_checks_out_or_runs_the_pr_code() -> None:
    checkout = next(s for s in _job()["steps"] if str(s.get("uses", "")).startswith("actions/checkout"))
    ref = checkout["with"]["ref"]
    assert "base.sha" in ref and "head" not in ref, "il checkout deve essere il commit base, mai l'head della PR"
    assert checkout["with"]["persist-credentials"] is False
    text = (ROOT / WORKFLOW).read_text(encoding="utf-8")
    assert "pull_request_target" not in text and "refs/pull/" not in text and "/merge" not in text


def test_workflow_passes_the_token_only_as_a_0600_file() -> None:
    step = next(s for s in _job()["steps"] if "pr_overlap_guard.sh" in s.get("run", ""))
    run = step["run"]
    assert "umask 077" in run and 'rm -f "$token_file"' in run and "unset GH_TOKEN" in run
    assert "PF_GITHUB_TOKEN_FILE=" in run
    assert not re.search(r"pr_overlap_guard\.sh\s+\S", run), "lo script non riceve argomenti"
    env = step["env"]
    assert set(env) == {"GH_TOKEN", "PF_PR_NUMBER", "PF_PR_HEAD_SHA", "PF_RESULT_DIR"}


def test_workflow_permissions_are_read_only() -> None:
    data = yaml.safe_load((ROOT / WORKFLOW).read_text(encoding="utf-8"))
    assert data["permissions"] == {"contents": "read", "pull-requests": "read"}
