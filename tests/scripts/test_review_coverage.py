"""PFC-002 (M159): copertura attestata delle review AI — test del modulo.

RED -> GREEN sul difetto reale: il `build_patch_payload` di main tagliava ogni
patch oltre MAX_PATCH_PER_FILE_CHARS e saltava i file oltre il budget totale, e
il job restava verde. Il primo test esegue la funzione VERA presa da main (git)
e mostra la perdita; il secondo mostra che la segmentazione non perde nulla.
"""
from __future__ import annotations

import ast
import re
import io
import json
import subprocess
import sys
import textwrap
import urllib.parse
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "ci"))

import review_coverage as rc  # noqa: E402

SHA_BASE = "a" * 40
SHA_1 = "1" * 40
SHA_2 = "2" * 40
SHA_3 = "3" * 40
REPO = "owner/repo"


def _patch(hunks: int, righe_per_hunk: int, larghezza: int = 60) -> str:
    blocchi = []
    for h in range(hunks):
        blocchi.append(f"@@ -{h * 100 + 1},{righe_per_hunk} +{h * 100 + 1},{righe_per_hunk} @@ def f{h}():\n")
        blocchi.extend(f"+    riga_{h}_{i} = {'x' * larghezza}\n" for i in range(righe_per_hunk))
    return "".join(blocchi)


# ---------------------------------------------------------------------------
# RED -> GREEN: il troncamento silenzioso di main
# ---------------------------------------------------------------------------

# `main` su cui nasce la PFC-002 (merge della #538): l'ultimo stato con il
# troncamento. Uno SHA fisso e non `origin/main`, cosi' il test resta un RED vero
# anche dopo il merge (con `origin/main` diventerebbe uno skip, e il check
# full-suite conta gli skip).
SHA_PRE_PFC002 = "99250c57f9d58ba758b0e0655f941e74172048aa"


def _build_patch_payload_di_main(workflow: str):
    """Estrae build_patch_payload dal workflow com'era prima della PFC-002."""
    try:
        testo = subprocess.run(
            ["git", "show", f"{SHA_PRE_PFC002}:{workflow}"], cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        pytest.skip("storia git non disponibile (checkout shallow)")
    righe = testo.splitlines()
    inizio = next(i for i, r in enumerate(righe) if r.strip() == "python3 <<'PY'")
    fine = next(i for i in range(inizio + 1, len(righe)) if righe[i].strip() == "PY")
    albero = ast.parse(textwrap.dedent("\n".join(righe[inizio + 1:fine])))
    nodo = next(n for n in albero.body if isinstance(n, ast.FunctionDef) and n.name == "build_patch_payload")
    assert "TRONCATO" in ast.unparse(nodo), "lo SHA storico deve contenere la versione con il troncamento"
    spazio = {"MAX_PATCH_PER_FILE_CHARS": 7000, "MAX_TOTAL_PATCH_CHARS": 24000,
              "safe_display": lambda s: str(s), "redact": lambda s: s, "is_critical": lambda s: False}
    exec(compile(ast.Module(body=[nodo], type_ignores=[]), workflow, "exec"), spazio)  # skipcq: PY-W0122
    return spazio["build_patch_payload"]


def test_red_main_tagliava_e_saltava_codice_in_silenzio():
    vecchio = _build_patch_payload_di_main(".github/workflows/pr-review-openrouter-gpt56-sol.yml")
    lungo = _patch(10, 30)  # ~22 000 caratteri
    files = [{"filename": f"mod{i}.py", "patch": lungo, "changes": 300} for i in range(4)]
    diff_text, skipped, _critical = vecchio(files)
    visto = sum(diff_text.count(f"riga_{h}_{i} ") for h in range(10) for i in range(30))
    totale = 4 * 10 * 30
    assert "[PATCH FILE TRONCATO PER BUDGET TOKEN]" in diff_text
    assert visto < totale / 2, (visto, totale)  # RED: meno della meta' delle righe arrivava al modello
    assert skipped == ["mod3.py"]  # e il quarto file restava fuori per intero


def test_green_la_segmentazione_copre_ogni_carattere():
    lungo = _patch(10, 30)
    files = [{"filename": f"mod{i}.py", "patch": lungo, "changes": 300} for i in range(4)]
    plan = rc.plan_review(files, max_segment_chars=7000, max_batch_chars=24000, max_calls=8)
    for i in range(4):
        ricostruito = "".join(s.text for s in plan.segments if s.filename == f"mod{i}.py")
        assert ricostruito == lungo  # nessun carattere perso, ordine preservato
    assert all(len(s.text) <= 7000 for s in plan.segments)
    tutti = [sid for b in plan.batches for sid in b]
    assert sorted(tutti) == sorted(s.seg_id for s in plan.segments)  # ogni segmento in un solo lotto
    for b in range(len(plan.batches)):
        assert len(rc.render_batch(plan, b)) <= 24000
    assert not plan.budget_exceeded


# ---------------------------------------------------------------------------
# Segmentazione deterministica
# ---------------------------------------------------------------------------

def test_segmenti_deterministici_e_ai_confini_degli_hunk():
    patch = _patch(4, 10)
    a = rc.segment_patch(1, "x.py", patch, 1500)
    b = rc.segment_patch(1, "x.py", patch, 1500)
    assert [s.as_dict() for s in a] == [s.as_dict() for s in b]
    assert all(s.text.startswith("@@") for s in a)  # nessun hunk spezzato se ci sta
    assert [s.seg_id for s in a] == [f"F001S{n:02d}" for n in range(1, len(a) + 1)]


def test_hunk_e_riga_oltre_il_tetto_si_dividono_senza_perdite():
    enorme = "@@ -1,1 +1,1 @@\n+" + "y" * 5000 + "\n" + "".join(f"+r{i}\n" for i in range(400))
    segmenti = rc.segment_patch(7, "big.py", enorme, 1000)
    assert "".join(s.text for s in segmenti) == enorme
    assert all(len(s.text) <= 1000 for s in segmenti)
    assert segmenti[0].parts == len(segmenti)


def test_tetti_non_validi_rifiutati():
    with pytest.raises(ValueError):
        rc.plan_review([], max_segment_chars=0, max_batch_chars=10, max_calls=1)
    with pytest.raises(ValueError):  # un segmento che non entra in nessun lotto
        rc.plan_review([{"filename": "a.py", "patch": "x" * 500, "changes": 1}],
                       max_segment_chars=500, max_batch_chars=100, max_calls=3)


def test_oltre_il_tetto_di_chiamate_nessun_lotto_viene_speso():
    files = [{"filename": f"m{i}.py", "patch": _patch(10, 30), "changes": 300} for i in range(4)]
    plan = rc.plan_review(files, max_segment_chars=7000, max_batch_chars=24000, max_calls=2)
    assert plan.budget_exceeded
    att = rc.build_attestation(reviewer="gpt56sol", model="m", repo=REPO, pr=1, range_base=SHA_BASE,
                               range_head=SHA_1, run_id=10, run_attempt=1, workflow_file="w.yml",
                               plan=plan, outcomes=[])
    assert att["complete"] is False
    assert any("tetto" in m for m in att["reasons"])


def test_file_senza_patch_testo_e_binario_bloccano_rename_no():
    files = [
        {"filename": "img/logo.png", "patch": None, "changes": 10},
        {"filename": "rinominato.py", "patch": None, "changes": 0},
        {"filename": "enorme.py", "patch": None, "changes": 9000},
    ]
    plan = rc.plan_review(files, max_segment_chars=7000, max_batch_chars=24000, max_calls=8)
    assert plan.binary == ["img/logo.png"]
    assert plan.unchanged == ["rinominato.py"]
    assert plan.unreviewable == ["enorme.py"]
    att = rc.build_attestation(reviewer="r", model="m", repo=REPO, pr=1, range_base=SHA_BASE, range_head=SHA_1,
                               run_id=1, run_attempt=1, workflow_file="w.yml", plan=plan, outcomes=[])
    assert att["complete"] is False and any("enorme.py" in m for m in att["reasons"])
    # Rilievo Codex su #539: un binario non e' stato visto da nessuno => incompleta
    solo_binari = rc.plan_review(files[:2], max_segment_chars=7000, max_batch_chars=24000, max_calls=8)
    att2 = rc.build_attestation(reviewer="r", model="m", repo=REPO, pr=1, range_base=SHA_BASE, range_head=SHA_1,
                                run_id=1, run_attempt=1, workflow_file="w.yml", plan=solo_binari, outcomes=[])
    assert att2["complete"] is False and att2["binary_files"] == ["img/logo.png"]
    assert any("binari" in m and "img/logo.png" in m for m in att2["reasons"])
    # un rename senza modifiche resta neutro
    solo_rename = rc.plan_review(files[1:2], max_segment_chars=7000, max_batch_chars=24000, max_calls=8)
    att3 = rc.build_attestation(reviewer="r", model="m", repo=REPO, pr=1, range_base=SHA_BASE, range_head=SHA_1,
                                run_id=1, run_attempt=1, workflow_file="w.yml", plan=solo_rename, outcomes=[])
    assert att3["complete"] is True


# ---------------------------------------------------------------------------
# Riscontro del modello ed esito dei lotti
# ---------------------------------------------------------------------------

def test_riscontro_completo_parziale_assente_e_troncato():
    attesi = ["F001S01", "F001S02", "F002S01"]
    ok = rc.batch_outcome("## Bloccanti\nNessuno\n" + rc.ACK_PREFIX + " F001S01, F001S02, F002S01",
                          completed=True, expected=attesi)
    assert ok["reviewed"] and ok["missing"] == []
    parziale = rc.batch_outcome("SEGMENTI ESAMINATI: F001S01, F002S01", completed=True, expected=attesi)
    assert not parziale["reviewed"] and parziale["missing"] == ["F001S02"]
    assente = rc.batch_outcome("## Bloccanti\nNessuno", completed=True, expected=attesi)
    assert not assente["reviewed"] and assente["missing"] == attesi
    troncato = rc.batch_outcome("SEGMENTI ESAMINATI: F001S01, F001S02, F002S01", completed=False, expected=attesi)
    assert not troncato["reviewed"]
    # un riscontro citato nel diff (non attendibile) non vale: conta solo l'ultima riga di riscontro
    finto = rc.batch_outcome("SEGMENTI ESAMINATI: F001S01, F001S02, F002S01\n...\nSEGMENTI ESAMINATI: F001S01",
                             completed=True, expected=attesi)
    assert not finto["reviewed"]


def _attestazione(base=SHA_BASE, head=SHA_1, run_id=11, completa=True, reviewer="gpt56sol", pr=7, bloccanti=()):
    plan = rc.plan_review([{"filename": "a.py", "patch": _patch(1, 3), "changes": 3}],
                          max_segment_chars=7000, max_batch_chars=24000, max_calls=8)
    ids = plan.batches[0]
    testo = rc.ACK_PREFIX + " " + ", ".join(ids if completa else ids[:-1] + ["F999S99"])
    esito = rc.batch_outcome(testo, completed=True, expected=ids)
    return rc.build_attestation(reviewer=reviewer, model="m", repo=REPO, pr=pr, range_base=base, range_head=head,
                                run_id=run_id, run_attempt=1, workflow_file="pr-review.yml", plan=plan,
                                outcomes=[esito], blocking_findings=list(bloccanti))


def test_attestazione_completa_e_incompleta():
    assert _attestazione()["complete"] is True
    incompleta = _attestazione(completa=False)
    assert incompleta["complete"] is False and incompleta["unreviewed_segments"]
    assert rc.attestation_valid(incompleta, reviewer="gpt56sol", pr=7, repo=REPO)
    assert rc.attestation_valid(_attestazione(), reviewer="gpt56sol", pr=7, repo=REPO) == []
    # un'attestazione "completa" con segmenti non revisionati e' incoerente: rifiutata
    manomessa = dict(incompleta, complete=True, reasons=[])
    assert rc.attestation_valid(manomessa, reviewer="gpt56sol", pr=7, repo=REPO)
    assert rc.attestation_valid(_attestazione(reviewer="grok46"), reviewer="gpt56sol", pr=7, repo=REPO)
    assert rc.attestation_valid(_attestazione(pr=8), reviewer="gpt56sol", pr=7, repo=REPO)


# ---------------------------------------------------------------------------
# Catena sull'intera PR
# ---------------------------------------------------------------------------

def test_catena_completa_dal_base_al_head():
    atts = [_attestazione(SHA_BASE, SHA_1), _attestazione(SHA_1, SHA_2), _attestazione(SHA_2, SHA_3)]
    esito = rc.verify_chain(atts, head_sha=SHA_3, is_root=lambda s: s == SHA_BASE)
    assert esito["ok"] and len(esito["chain"]) == 3


def test_push_senza_review_spezza_la_catena():
    atts = [_attestazione(SHA_BASE, SHA_1), _attestazione(SHA_2, SHA_3)]  # manca 1...2
    esito = rc.verify_chain(atts, head_sha=SHA_3, is_root=lambda s: s == SHA_BASE)
    assert not esito["ok"] and "catena" in esito["reasons"][0]


def test_head_cambiato_senza_review_e_rosso():
    atts = [_attestazione(SHA_BASE, SHA_1), _attestazione(SHA_1, SHA_2)]
    esito = rc.verify_chain(atts, head_sha=SHA_3, is_root=lambda s: s == SHA_BASE)
    assert not esito["ok"] and "head corrente" in esito["reasons"][0]


def test_una_review_dell_intera_pr_basta_da_sola():
    esito = rc.verify_chain([_attestazione(SHA_BASE, SHA_3)], head_sha=SHA_3, is_root=lambda s: s == SHA_BASE)
    assert esito["ok"]


# ---------------------------------------------------------------------------
# Solo i run UFFICIALI contano: workflow alternativi, eventi diversi, forgiature
# ---------------------------------------------------------------------------

def _zip(*contenuti: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for i, c in enumerate(contenuti):
            zf.writestr(f"attestation{i}.json", json.dumps(c))
    return buf.getvalue()


class FakeGitHub(rc.GitHub):
    """GitHub in memoria, FEDELE ai filtri che il codice usa (ciclo 11 su #539).

    Prima ignorava `created`, `head_sha` e `branch` e restituiva tutti i run a
    ogni ricerca: un test poteva passare per un run che l'API vera non avrebbe
    mai restituito (verifica indipendente finale su #539). Ora:
    - `/actions/workflows/<f>/runs` applica `branch`, `head_sha`, `created`
      (`>=X` oppure `A..B`) e la paginazione; NON filtra per file ne' per
      evento, di proposito: i test di difesa vogliono vedere scartati dal
      codice anche i run che l'API non dovrebbe restituire;
    - le PR (`pulls`) hanno stato, merge, date, branch e commit propri;
      `/commits/<sha>/pulls` e `/pulls?state=closed&head=...` rispondono da li'.
    """

    def __init__(self, runs, artifacts, commits=(SHA_1, SHA_2, SHA_3), base_ancestors=(), comments=(),
                 expired=(), pulls=None, pr_commits=None, commit_pulls=None):
        super().__init__(REPO, "token")
        self.runs = runs
        self.artifacts = artifacts  # run_id -> list[(name, bytes)]
        self.commits = list(commits)
        self.base_ancestors = set(base_ancestors)
        self.comments = comments if isinstance(comments, dict) else {7: list(comments)}
        self.expired = set(expired)  # run_id con artifact scaduti
        self.pulls = {7: {"number": 7, "state": "open", "merged_at": None, "created_at": "2026-10-01T00:00:00Z",
                          "closed_at": None, "head": {"ref": "feature"}}}
        self.pulls.update(pulls or {})
        self.pr_commits = {7: self.commits}
        self.pr_commits.update(pr_commits or {})
        self.commit_pulls = {}  # sha -> numeri di PR (anche chiuse, come l'API vera)
        for numero, shas in self.pr_commits.items():
            for sha in shas:
                self.commit_pulls.setdefault(sha, set()).add(numero)
        for sha, numeri in (commit_pulls or {}).items():
            self.commit_pulls.setdefault(sha, set()).update(numeri)

    @staticmethod
    def _query(path):
        q = {}
        for parte in (path.split("?", 1)[1] if "?" in path else "").split("&"):
            if "=" in parte:
                k, v = parte.split("=", 1)
                q[k] = urllib.parse.unquote(v)  # come l'API vera
        return q

    @staticmethod
    def _pagina(items, q):
        per_page, page = int(q.get("per_page", 30)), int(q.get("page", 1))
        return items[(page - 1) * per_page: page * per_page]

    def _runs(self, q):
        sel = list(self.runs)
        if "branch" in q:
            sel = [r for r in sel if r.get("head_branch") == q["branch"]]
        if "head_sha" in q:
            sel = [r for r in sel if r.get("head_sha") == q["head_sha"]]
        if "created" in q:
            c = q["created"]
            if c.startswith(">="):
                sel = [r for r in sel if r.get("created_at", "") >= c[2:]]
            elif ".." in c:
                a, b = c.split("..", 1)
                sel = [r for r in sel if (a == "*" or r.get("created_at", "") >= a)
                       and (b == "*" or r.get("created_at", "") <= b)]
        return self._pagina(sel, q)

    def get(self, path):
        q = self._query(path)
        base = path.split("?")[0]
        if "/actions/workflows/" in base:
            return {"workflow_runs": self._runs(q)}
        m = re.search(r"/pulls/(\d+)/commits$", base)
        if m:
            return self._pagina([{"sha": s} for s in self.pr_commits.get(int(m.group(1)), [])], q)
        m = re.search(r"/pulls/(\d+)$", base)
        if m:
            return self.pulls[int(m.group(1))]
        if base.endswith("/pulls") and "/commits/" not in base:
            ref = q.get("head", ":").split(":", 1)[1]
            stato = q.get("state", "open")
            return [pr for pr in self.pulls.values() if pr["head"]["ref"] == ref
                    and (stato == "all" or pr["state"] == stato)]
        m = re.search(r"/commits/([0-9a-f]{40})/pulls$", base)
        if m:
            return [self.pulls[n] for n in sorted(self.commit_pulls.get(m.group(1), set())) if n in self.pulls]
        m = re.search(r"/issues/(\d+)/comments$", base)
        if m:
            return self._pagina(self.comments.get(int(m.group(1)), []), q)
        if base.endswith("/artifacts"):
            run_id = int(base.split("/runs/")[1].split("/")[0])
            return {"artifacts": [{"name": n, "archive_download_url": f"zip:{run_id}:{i}",
                                   "expired": run_id in self.expired}
                                  for i, (n, _b) in enumerate(self.artifacts.get(run_id, []))]}
        if "/compare/" in base:
            sha = base.split("/compare/")[1].split("...")[0]
            return {"status": "ahead" if sha in self.base_ancestors else "diverged"}
        raise AssertionError(path)

    def download(self, url):
        _z, run_id, idx = url.split(":")
        return self.artifacts[int(run_id)][int(idx)][1]


def _run(run_id, head, event="pull_request_target", path=".github/workflows/pr-review.yml",
         created="2026-10-10T00:00:00Z", branch="feature"):
    return {"id": run_id, "head_sha": head, "event": event, "path": path, "status": "completed",
            "created_at": created, "head_branch": branch}


def test_solo_i_run_ufficiali_del_workflow_del_revisore_contano():
    buona = _attestazione(SHA_BASE, SHA_1, run_id=11)
    da_pull_request = _attestazione(SHA_1, SHA_2, run_id=12)
    da_altro_file = _attestazione(SHA_1, SHA_2, run_id=13)
    run_sbagliato = _attestazione(SHA_1, SHA_2, run_id=999)
    head_sbagliato = _attestazione(SHA_1, SHA_3, run_id=15)
    incompleta = _attestazione(SHA_1, SHA_2, run_id=16, completa=False)
    runs = [
        _run(11, SHA_1),
        _run(12, SHA_2, event="pull_request"),
        _run(13, SHA_2, path=".github/workflows/pr-review-fake.yml"),
        _run(14, SHA_2),
        _run(15, SHA_2),
        _run(16, SHA_2),
        _run(17, "f" * 40),  # head non della PR: ignorato
    ]
    nome = "review-coverage-gpt56sol-x"
    gh = FakeGitHub(runs, {11: [(nome, _zip(buona))], 12: [(nome, _zip(da_pull_request))],
                           13: [(nome, _zip(da_altro_file))], 14: [(nome, _zip(run_sbagliato))],
                           15: [(nome, _zip(head_sbagliato))], 16: [(nome, _zip(incompleta))]})
    atts, scarti = rc.official_attestations(gh, pr=7, reviewer="gpt56sol", workflow_file="pr-review.yml",
                                            pr_commits=set(gh.commits))
    assert [a["run_id"] for a in atts] == [11]
    testo = " | ".join(scarti)
    assert "run 12" in testo and "run 13" in testo  # evento/percorso non ufficiale
    assert "run 14" in testo and "altro run" in testo
    assert "run 15" in testo and "head diverso" in testo
    assert "run 16" in testo and "incompleta" in testo


def test_artifact_con_piu_attestazioni_rifiutato():
    a = _attestazione(SHA_BASE, SHA_1, run_id=11)
    gh = FakeGitHub([_run(11, SHA_1)], {11: [("review-coverage-gpt56sol-x", _zip(a, a))]})
    atts, scarti = rc.official_attestations(gh, pr=7, reviewer="gpt56sol", workflow_file="pr-review.yml",
                                            pr_commits=set(gh.commits))
    assert atts == [] and "illeggibile" in scarti[0]


def _verify(monkeypatch, tmp_path, gh, head, locale=None):
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setattr(rc, "GitHub", lambda repo, token: gh)
    argv = ["verify", "--repo", REPO, "--pr", "7", "--reviewer", "gpt56sol", "--workflow-file", "pr-review.yml",
            "--base-sha", SHA_BASE, "--head-sha", head, "--output", str(tmp_path / "out.json")]
    if locale is not None:
        p = tmp_path / "local.json"
        p.write_text(json.dumps(locale))
        argv += ["--local-attestation", str(p)]
    rc_code = rc.main(argv)
    return rc_code, json.loads((tmp_path / "out.json").read_text())


def test_verify_verde_solo_con_la_catena_completa(monkeypatch, tmp_path):
    nome = "review-coverage-gpt56sol-x"
    runs = [_run(11, SHA_1), _run(12, SHA_2)]
    arts = {11: [(nome, _zip(_attestazione(SHA_BASE, SHA_1, 11)))]}
    gh = FakeGitHub(runs, arts)
    # attestazione di questo run (locale) per 1...2: catena completa
    code, out = _verify(monkeypatch, tmp_path, gh, SHA_2, locale=_attestazione(SHA_1, SHA_2, 12))
    assert code == 0 and out["ok"] is True
    # senza la review di 1...2 (provider a secco, lotto troncato, segreto mancante): rosso
    code, out = _verify(monkeypatch, tmp_path, FakeGitHub(runs, arts), SHA_2)
    assert code == 1 and out["ok"] is False
    # head che non e' un commit della PR: rosso
    code, out = _verify(monkeypatch, tmp_path, FakeGitHub(runs, arts), "e" * 40)
    assert code == 1 and "non e' un commit della PR" in out["reasons"][0]


def test_verify_senza_token_o_con_api_in_errore_e_rosso(monkeypatch, tmp_path):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    out_path = tmp_path / "o.json"
    code = rc.main(["verify", "--repo", REPO, "--pr", "7", "--reviewer", "gpt56sol", "--workflow-file", "w.yml",
                    "--base-sha", SHA_BASE, "--head-sha", SHA_1, "--output", str(out_path)])
    assert code == 1 and json.loads(out_path.read_text())["ok"] is False

    class Rotto(FakeGitHub):
        def get(self, path):
            raise RuntimeError("HTTP 500")

    code, out = _verify(monkeypatch, tmp_path, Rotto([], {}), SHA_1)
    assert code == 1 and "non eseguibile" in out["reasons"][0]


def test_radice_e_un_commit_del_branch_base_non_della_pr(monkeypatch, tmp_path):
    vecchia_base = "b" * 40  # base al momento dell'apertura, ora antenato del base corrente
    nome = "review-coverage-gpt56sol-x"
    gh = FakeGitHub([_run(11, SHA_1)], {11: [(nome, _zip(_attestazione(vecchia_base, SHA_1, 11)))]},
                    base_ancestors={vecchia_base})
    code, out = _verify(monkeypatch, tmp_path, gh, SHA_1)
    assert code == 0, out
    # un range che parte da un commit della PR non e' una radice
    gh2 = FakeGitHub([_run(11, SHA_2)], {11: [(nome, _zip(_attestazione(SHA_1, SHA_2, 11)))]})
    code, out = _verify(monkeypatch, tmp_path, gh2, SHA_2)
    assert code == 1


# ---------------------------------------------------------------------------
# Rilievi Codex su #539
# ---------------------------------------------------------------------------

def test_riscontro_valido_solo_come_ultima_riga():
    ids = ["F001S01", "F001S02"]
    in_coda = rc.batch_outcome("## Bloccanti\nNessuno\nSEGMENTI ESAMINATI: F001S01, F001S02\n\n",
                               completed=True, expected=ids)
    assert in_coda["reviewed"]
    poi_rifiuto = rc.batch_outcome("SEGMENTI ESAMINATI: F001S01, F001S02\nNon posso completare la review.",
                                   completed=True, expected=ids)
    assert not poi_rifiuto["reviewed"] and poi_rifiuto["missing"] == ids
    in_grassetto = rc.batch_outcome("ok\n**SEGMENTI ESAMINATI:** F001S01, F001S02", completed=True, expected=ids)
    assert in_grassetto["reviewed"]


def test_run_associato_alla_pr_anche_con_head_sul_branch_base():
    # pull_request_target: se l'API riportasse l'head del branch base, il run
    # resta della PR grazie a `pull_requests`; l'attestazione deve comunque
    # nominare un commit della PR.
    att = _attestazione(SHA_BASE, SHA_1, run_id=21)
    run = {"id": 21, "head_sha": "9" * 40, "event": "pull_request_target",
           "path": ".github/workflows/pr-review.yml", "pull_requests": [{"number": 7}]}
    altra_pr = dict(run, id=22, pull_requests=[{"number": 8}])
    gh = FakeGitHub([run, altra_pr], {21: [("review-coverage-gpt56sol-x", _zip(att))],
                                      22: [("review-coverage-gpt56sol-x", _zip(_attestazione(SHA_BASE, SHA_1, 22)))]})
    atts, _ = rc.official_attestations(gh, pr=7, reviewer="gpt56sol", workflow_file="pr-review.yml",
                                       pr_commits=set(gh.commits))
    assert [a["run_id"] for a in atts] == [21]
    fuori = _attestazione(SHA_BASE, "8" * 40, run_id=23)  # head che non e' della PR
    gh2 = FakeGitHub([dict(run, id=23)], {23: [("review-coverage-gpt56sol-x", _zip(fuori))]})
    atts2, scarti2 = rc.official_attestations(gh2, pr=7, reviewer="gpt56sol", workflow_file="pr-review.yml",
                                              pr_commits=set(gh2.commits))
    assert atts2 == [] and "non e' un commit della PR" in scarti2[0]


def test_range_attested_per_la_dedup_a_pagamento():
    nome = "review-coverage-gpt56sol-x"
    gh = FakeGitHub([_run(11, SHA_1)], {11: [(nome, _zip(_attestazione(SHA_BASE, SHA_1, 11)))]})
    assert rc.range_attested(gh, pr=7, reviewer="gpt56sol", workflow_file="pr-review.yml",
                             range_base=SHA_BASE, range_head=SHA_1)
    # artifact mancante (upload fallito, scaduto, cancellato): si rifa' la review
    vuoto = FakeGitHub([_run(11, SHA_1)], {})
    assert not rc.range_attested(vuoto, pr=7, reviewer="gpt56sol", workflow_file="pr-review.yml",
                                 range_base=SHA_BASE, range_head=SHA_1)

    class Rotto(FakeGitHub):
        def get(self, path):
            raise RuntimeError("HTTP 502")

    assert not rc.range_attested(Rotto([], {}), pr=7, reviewer="gpt56sol", workflow_file="pr-review.yml",
                                 range_base=SHA_BASE, range_head=SHA_1)


def test_verify_aspetta_il_run_del_push_precedente(monkeypatch, tmp_path):
    nome = "review-coverage-gpt56sol-x"
    predecessore = dict(_run(11, SHA_1), status="in_progress")
    gh = FakeGitHub([predecessore, dict(_run(12, SHA_2), status="in_progress")], {})

    def dorme(_s):  # al primo giro di attesa il predecessore finisce e carica l'artifact
        predecessore["status"] = "completed"
        gh.artifacts[11] = [(nome, _zip(_attestazione(SHA_BASE, SHA_1, 11)))]

    monkeypatch.setattr(rc.time, "sleep", dorme)
    monkeypatch.setenv("GITHUB_RUN_ID", "12")
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setattr(rc, "GitHub", lambda repo, token: gh)
    locale = tmp_path / "l.json"
    locale.write_text(json.dumps(_attestazione(SHA_1, SHA_2, 12)))
    argv = ["verify", "--repo", REPO, "--pr", "7", "--reviewer", "gpt56sol", "--workflow-file", "pr-review.yml",
            "--base-sha", SHA_BASE, "--head-sha", SHA_2, "--local-attestation", str(locale),
            "--wait-seconds", "60", "--poll-seconds", "1", "--output", str(tmp_path / "o.json")]
    assert rc.main(argv) == 0
    out = json.loads((tmp_path / "o.json").read_text())
    assert out["ok"] and out["waited_for_runs"] == [11]
    # senza attesa (comportamento di a022dfe) lo stesso caso era rosso
    gh.artifacts.clear()
    predecessore["status"] = "in_progress"
    assert rc.main(argv[:-6] + ["--output", str(tmp_path / "o2.json")]) == 1


def test_verify_non_aspetta_se_nessun_run_e_in_corso(monkeypatch, tmp_path):
    gh = FakeGitHub([dict(_run(11, SHA_1), status="completed")], {})
    chiamate = []
    monkeypatch.setattr(rc.time, "sleep", lambda s: chiamate.append(s))
    code, out = _verify(monkeypatch, tmp_path, gh, SHA_2, locale=_attestazione(SHA_1, SHA_2, 12))
    assert code == 1 and chiamate == []


def test_aggregate_una_sola_coppia_di_sezioni_canoniche():
    pulito = "## Bloccanti\nNessun bloccante evidente\n## Verdetto finale\nOK\nSEGMENTI ESAMINATI: F001S01"
    con_bug = "## Bloccanti\n- core/x.py: liability LAY ignorata\n## Verdetto finale\nNon approvo\nSEGMENTI ESAMINATI: F002S01"
    rotto = "solo testo libero"
    tutti_puliti = rc.aggregate_reviews([{"indice": 1, "totale": 2, "text": pulito},
                                         {"indice": 2, "totale": 2, "text": pulito}])
    assert tutti_puliti["bloccanti"] == "Nessun bloccante evidente"
    misto = rc.aggregate_reviews([{"indice": 1, "totale": 3, "text": pulito},
                                  {"indice": 2, "totale": 3, "text": con_bug},
                                  {"indice": 3, "totale": 3, "text": rotto}])
    assert "[lotto 2/3] core/x.py: liability LAY ignorata" in misto["bloccanti"]
    assert "[lotto 3/3] sezione Bloccanti assente" in misto["bloccanti"]
    assert "Nessun bloccante" not in misto["bloccanti"]
    # i dettagli non contengono piu' titoli di secondo livello
    assert all(not re.search(r"(?m)^##\s", d) for d in misto["dettagli"])


def test_tetto_di_segmenti_per_lotto():
    files = [{"filename": f"f{i}.py", "patch": "@@ -1 +1 @@\n+x\n", "changes": 1} for i in range(100)]
    plan = rc.plan_review(files, max_segment_chars=7000, max_batch_chars=24000, max_calls=8)
    assert all(len(b) <= rc.MAX_SEGMENTS_PER_BATCH for b in plan.batches)
    assert len(plan.batches) == 3
    assert "NON conta nel limite di parole" in rc.ack_instruction(["F001S01"])


@pytest.mark.parametrize("verdetto", ["", "## Verdetto finale\n", "## Verdetto finale\nOK\n## Verdetto finale\nKO\n"])
def test_verdetto_assente_vuoto_o_duplicato_non_e_pulito(verdetto):
    # Rilievo GPT-6.1 Sol su #539: Bloccanti pulita ma Verdetto finale non
    # univoco veniva normalizzato a "(verdetto assente)" e l'aggregato sembrava pulito.
    testo = "## Bloccanti\nNessun bloccante evidente\n" + verdetto + "SEGMENTI ESAMINATI: F001S01"
    unita = rc.aggregate_reviews([{"indice": 1, "totale": 2, "text": testo},
                                  {"indice": 2, "totale": 2,
                                   "text": "## Bloccanti\nNessun bloccante evidente\n## Verdetto finale\nOK"}])
    assert "Nessun bloccante" not in unita["bloccanti"]
    assert "[lotto 1/2] sezione Verdetto finale" in unita["bloccanti"]


def test_aggregate_espone_la_lista_dei_bloccanti():
    pulito = "## Bloccanti\nNessun bloccante evidente\n## Verdetto finale\nOK"
    con_bug = "## Bloccanti\n- core/x.py: liability LAY ignorata\n## Verdetto finale\nNo"
    assert rc.aggregate_reviews([{"indice": 1, "totale": 1, "text": pulito}])["lista_bloccanti"] == []
    misto = rc.aggregate_reviews([{"indice": 1, "totale": 2, "text": pulito},
                                  {"indice": 2, "totale": 2, "text": con_bug}])
    assert misto["lista_bloccanti"] == ["[lotto 2/2] core/x.py: liability LAY ignorata"]


def test_bloccanti_della_review_sul_head_rendono_rossa_la_verifica(monkeypatch, tmp_path):
    # Rilievo Codex su #539: con ogni segmento riscontrato ma un bloccante reale
    # nella review, la verifica era verde e Merge Readiness leggeva solo SUCCESS.
    nome = "review-coverage-gpt56sol-x"
    runs = [_run(11, SHA_1), _run(12, SHA_2)]

    def gh():
        return FakeGitHub(runs, {11: [(nome, _zip(_attestazione(SHA_BASE, SHA_1, 11)))]})
    code, out = _verify(monkeypatch, tmp_path, gh(), SHA_2,
                        locale=_attestazione(SHA_1, SHA_2, 12, bloccanti=["[lotto 1/1] stake raddoppiato"]))
    assert code == 1 and out["ok"] is False
    assert any("stake raddoppiato" in m for m in out["reasons"])
    # pulita sul head: verde
    code, out = _verify(monkeypatch, tmp_path, gh(), SHA_2, locale=_attestazione(SHA_1, SHA_2, 12))
    assert code == 0 and out["ok"] is True
    # esito della review non registrato nell'attestazione: rosso (fail-closed)
    senza = _attestazione(SHA_1, SHA_2, 12)
    del senza["blocking_findings"]
    code, out = _verify(monkeypatch, tmp_path, gh(), SHA_2, locale=senza)
    assert code == 1 and any("non registrato" in m for m in out["reasons"])


# ---------------------------------------------------------------------------
# Persistenza dei bloccanti (decisione owner D1-D4 su #539): un bloccante resta
# aperto attraverso rerun, push successivi, commit vuoti, merge di main,
# modifiche di titolo e force-push, finche' l'owner non registra il triage.
# ---------------------------------------------------------------------------
NOME_ART = "review-coverage-gpt56sol-x"
BLOCCANTE = "[lotto 1/2] money.py: stake raddoppiato"


def _commento_triage(bid, *, login="owner", assoc="OWNER", app=None, modificato=False,
                     motivo="falso positivo: lo stake e' calcolato una volta sola in money.py, vedi test_x",
                     reviewer="gpt56sol"):
    return {"user": {"login": login}, "author_association": assoc, "performed_via_github_app": app,
            "created_at": "2026-10-23T09:00:00Z",
            "updated_at": "2026-10-23T09:05:00Z" if modificato else "2026-10-23T09:00:00Z",
            "body": f"<!-- pfc-triage-bloccante:{reviewer}:{bid} -->\n{motivo}"}


def _catena_con_bloccante_vecchio(**kw):
    """BASE..1 con un bloccante, 1..2 pulito: il caso del commit vuoto / merge di main (D2)."""
    runs = [_run(11, SHA_1), _run(12, SHA_2)]
    vecchia = _attestazione(SHA_BASE, SHA_1, 11, bloccanti=[BLOCCANTE])
    return FakeGitHub(runs, {11: [(NOME_ART, _zip(vecchia))]}, **kw)


def test_d2_bloccante_di_un_range_precedente_resta_aperto(monkeypatch, tmp_path):
    # Prima: conta solo il range sul head, quindi un commit vuoto o un merge di
    # main (range senza segmenti, review "pulita") rendeva verde il check.
    code, out = _verify(monkeypatch, tmp_path, _catena_con_bloccante_vecchio(), SHA_2,
                        locale=_attestazione(SHA_1, SHA_2, 12))
    assert code == 1 and out["ok"] is False
    bid = rc.blocker_id("gpt56sol", BLOCCANTE, f"{SHA_BASE}...{SHA_1}")
    assert any(bid in m and "stake raddoppiato" in m for m in out["reasons"])
    assert out["open_blockers"][0]["id"] == bid


def test_d1_bloccante_di_un_tentativo_incompleto_sopravvive_al_rerun(monkeypatch, tmp_path):
    # Tentativo 1: un lotto trova il bloccante, un altro e' troncato (incompleta).
    # Tentativo 2 dello stesso run: review pulita e completa. Prima la verifica
    # scartava l'incompleta e il bloccante spariva.
    incompleta = _attestazione(SHA_BASE, SHA_1, 4242, completa=False, bloccanti=[BLOCCANTE])
    pulita = dict(_attestazione(SHA_BASE, SHA_1, 4242), run_attempt=2)
    gh = FakeGitHub([_run(4242, SHA_1)], {4242: [("review-coverage-gpt56sol-a1", _zip(incompleta)),
                                                 ("review-coverage-gpt56sol-a2", _zip(pulita))]})
    code, out = _verify(monkeypatch, tmp_path, gh, SHA_1)
    assert code == 1 and any("stake raddoppiato" in m for m in out["reasons"])


def test_bloccante_su_un_commit_tolto_dal_force_push_resta_aperto(monkeypatch, tmp_path):
    # Il run del commit riscritto e' ancora della PR (pull_requests), il suo
    # head non e' piu' fra i commit: il bloccante non deve sparire.
    riscritto = "f" * 40
    vecchio = dict(_run(10, riscritto), pull_requests=[{"number": 7}])
    runs = [vecchio, _run(11, SHA_1)]
    arts = {10: [(NOME_ART, _zip(_attestazione(SHA_BASE, riscritto, 10, bloccanti=[BLOCCANTE])))],
            11: [(NOME_ART, _zip(_attestazione(SHA_BASE, SHA_1, 11)))]}
    code, out = _verify(monkeypatch, tmp_path, FakeGitHub(runs, arts), SHA_1)
    assert code == 1 and any("stake raddoppiato" in m for m in out["reasons"])


def test_triage_dell_owner_chiude_il_bloccante(monkeypatch, tmp_path):
    bid = rc.blocker_id("gpt56sol", BLOCCANTE, f"{SHA_BASE}...{SHA_1}")
    gh = _catena_con_bloccante_vecchio(comments=[_commento_triage(bid)])
    code, out = _verify(monkeypatch, tmp_path, gh, SHA_2, locale=_attestazione(SHA_1, SHA_2, 12))
    assert code == 0 and out["ok"] is True
    assert out["triaged_blockers"] == [bid]


@pytest.mark.parametrize("variante", [
    {"app": {"slug": "claude"}},           # pubblicato da un'app: e' l'agente, non l'owner
    {"modificato": True},                  # commento modificato dopo la pubblicazione
    {"login": "altro", "assoc": "MEMBER"},  # non e' l'owner del repository
    {"assoc": "COLLABORATOR"},
    {"motivo": ""},                        # senza motivazione documentata
    {"reviewer": "grok46"},                # triage di un altro revisore
])
def test_triage_non_valido_non_chiude_niente(variante, monkeypatch, tmp_path):
    bid = rc.blocker_id("gpt56sol", BLOCCANTE, f"{SHA_BASE}...{SHA_1}")
    gh = _catena_con_bloccante_vecchio(comments=[_commento_triage(bid, **variante)])
    code, out = _verify(monkeypatch, tmp_path, gh, SHA_2, locale=_attestazione(SHA_1, SHA_2, 12))
    assert code == 1 and out["ok"] is False


def test_id_del_bloccante_stabile_fra_lotti_e_rerun_dello_stesso_range():
    # stesso range = stesso piano dei lotti: l'etichetta del lotto e' stabile
    rng = f"{SHA_BASE}...{SHA_1}"
    a = rc.blocker_id("gpt56sol", "[lotto 1/2] money.py: stake   raddoppiato", rng)
    b = rc.blocker_id("gpt56sol", "[lotto 1/2]  Money.py: stake raddoppiato", rng)
    assert a == b and re.fullmatch(r"[0-9a-f]{12}", a)
    assert rc.blocker_id("grok46", "money.py: stake raddoppiato", rng) != a


def test_id_dipende_dal_range_un_triage_non_copre_le_ripetizioni(monkeypatch, tmp_path):
    # Secondo audit su #539: l'id senza range faceva valere un triage per ogni
    # ripetizione dello stesso testo, compreso il bloccante sintetico
    # "lotto non leggibile", che nasconde il contenuto vero del lotto.
    sintetico = "[lotto 1/1] sezione Bloccanti assente, vuota o duplicata: lotto non leggibile"
    assert rc.blocker_id("gpt56sol", sintetico, f"{SHA_BASE}...{SHA_1}") != \
        rc.blocker_id("gpt56sol", sintetico, f"{SHA_1}...{SHA_2}")
    primo = rc.blocker_id("gpt56sol", sintetico, f"{SHA_BASE}...{SHA_1}")
    runs = [_run(11, SHA_1), _run(12, SHA_2)]
    gh = FakeGitHub(runs, {11: [(NOME_ART, _zip(_attestazione(SHA_BASE, SHA_1, 11, bloccanti=[sintetico])))]},
                    comments=[_commento_triage(primo)])
    code, out = _verify(monkeypatch, tmp_path, gh, SHA_2,
                        locale=_attestazione(SHA_1, SHA_2, 12, bloccanti=[sintetico]))
    assert code == 1 and primo in out["triaged_blockers"]
    assert any("lotto non leggibile" in m for m in out["reasons"])


def test_run_della_pr_in_corso_rende_rossa_anche_una_catena_completa(monkeypatch, tmp_path):
    # Secondo audit su #539: con un range dal base (force-push, reopen) la
    # catena e' completa con la sola attestazione locale; il run del push
    # precedente, ancora in corso, non era aspettato e i suoi bloccanti sparivano.
    monkeypatch.setenv("GITHUB_RUN_ID", "12")
    in_corso = dict(_run(10, SHA_1), status="in_progress")
    gh = FakeGitHub([in_corso, dict(_run(12, SHA_2), status="in_progress")], {})
    code, out = _verify(monkeypatch, tmp_path, gh, SHA_2, locale=_attestazione(SHA_BASE, SHA_2, 12))
    assert code == 1 and any("in corso" in m for m in out["reasons"])


def test_run_in_corso_si_aspetta_e_poi_si_leggono_i_suoi_bloccanti(monkeypatch, tmp_path):
    monkeypatch.setenv("GITHUB_RUN_ID", "12")
    in_corso = dict(_run(10, SHA_1), status="in_progress")
    gh = FakeGitHub([in_corso, dict(_run(12, SHA_2), status="in_progress")], {})

    def finisce(_secondi):
        in_corso["status"] = "completed"
        gh.artifacts[10] = [(NOME_ART, _zip(_attestazione(SHA_BASE, SHA_1, 10, bloccanti=[BLOCCANTE])))]
    monkeypatch.setattr(rc.time, "sleep", finisce)
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setattr(rc, "GitHub", lambda repo, token: gh)
    loc = tmp_path / "l.json"
    loc.write_text(json.dumps(_attestazione(SHA_BASE, SHA_2, 12)))
    out_path = tmp_path / "o.json"
    code = rc.main(["verify", "--repo", REPO, "--pr", "7", "--reviewer", "gpt56sol", "--workflow-file",
                    "pr-review.yml", "--base-sha", SHA_BASE, "--head-sha", SHA_2, "--output", str(out_path),
                    "--local-attestation", str(loc), "--wait-seconds", "600", "--poll-seconds", "1"])
    out = json.loads(out_path.read_text())
    assert code == 1 and any("stake raddoppiato" in m for m in out["reasons"])


def test_esito_non_registrato_e_artifact_scaduto_si_possono_triagiare(monkeypatch, tmp_path):
    # Liveness (secondo audit): senza un id la PR restava rossa per sempre.
    strana = dict(_attestazione(SHA_BASE, SHA_1, 11), blocking_findings="?")
    runs = [_run(11, SHA_1), _run(12, SHA_2), _run(13, SHA_2)]
    arts = {11: [(NOME_ART, _zip(strana))], 13: [("review-coverage-gpt56sol-vecchio", b"x")]}
    gh = FakeGitHub(runs, arts, expired={13})
    code, out = _verify(monkeypatch, tmp_path, gh, SHA_2, locale=_attestazione(SHA_1, SHA_2, 12))
    assert code == 1
    ids = [b["id"] for b in out["open_blockers"]]
    assert len(ids) == 2 and all(re.fullmatch(r"[0-9a-f]{12}", i) for i in ids)
    gh2 = FakeGitHub(runs, arts, expired={13}, comments=[_commento_triage(i) for i in ids])
    code, out = _verify(monkeypatch, tmp_path, gh2, SHA_2, locale=_attestazione(SHA_1, SHA_2, 12))
    assert code == 0, out["reasons"]


def test_artifact_scaduto_della_pr_e_rosso(monkeypatch, tmp_path):
    # Un artifact scaduto puo' contenere bloccanti: non si sa, quindi rosso.
    gh = _catena_con_bloccante_vecchio(expired={11})
    code, out = _verify(monkeypatch, tmp_path, gh, SHA_2, locale=_attestazione(SHA_1, SHA_2, 12))
    assert code == 1 and any("scadut" in m for m in out["reasons"])


def test_storico_dei_run_non_letto_per_intero_e_rosso(monkeypatch, tmp_path):
    class Piena(FakeGitHub):
        def get(self, path):
            if "/actions/workflows/" in path and "event=pull_request_target" in path:
                riempitivo = [{"id": 900000 + i, "head_sha": "e" * 40, "event": "pull_request_target",
                               "path": ".github/workflows/pr-review.yml"} for i in range(100)]
                return {"workflow_runs": (self.runs if "page=1&" in path or path.endswith("page=1") else [])
                        + riempitivo}
            return super().get(path)
    gh = Piena([_run(11, SHA_1)], {11: [(NOME_ART, _zip(_attestazione(SHA_BASE, SHA_1, 11)))]})
    code, out = _verify(monkeypatch, tmp_path, gh, SHA_1)
    assert code == 1 and any("storico" in m for m in out["reasons"])


def test_attestazione_di_versione_precedente_senza_campo_non_blocca_ma_si_annota(monkeypatch, tmp_path):
    vecchia = _attestazione(SHA_BASE, SHA_1, 11)
    del vecchia["blocking_findings"]
    gh = FakeGitHub([_run(11, SHA_1), _run(12, SHA_2)], {11: [(NOME_ART, _zip(vecchia))]})
    code, out = _verify(monkeypatch, tmp_path, gh, SHA_2, locale=_attestazione(SHA_1, SHA_2, 12))
    assert code == 0 and any("versione precedente" in n for n in out["notes"])


# ---------------------------------------------------------------------------
# Terzo audit indipendente su #539
# ---------------------------------------------------------------------------
def test_run_in_corso_legato_alla_pr_solo_dal_branch_si_aspetta(monkeypatch, tmp_path):
    # Head riscritto da un force-push e `pull_requests` vuoto: il run e' della
    # PR solo per il branch. Prima `runs_in_corso` non lo vedeva e il check del
    # nuovo head era verde prima che il run caricasse i suoi bloccanti.
    monkeypatch.setenv("GITHUB_RUN_ID", "12")
    vecchio = dict(_run(10, "f" * 40), status="in_progress", head_branch="feature", pull_requests=[])
    gh = FakeGitHub([vecchio, dict(_run(12, SHA_2), status="in_progress")], {})
    code, out = _verify(monkeypatch, tmp_path, gh, SHA_2, locale=_attestazione(SHA_BASE, SHA_2, 12))
    assert code == 1 and any("in corso" in m for m in out["reasons"])


def test_bloccanti_dell_attestazione_locale_incompleta_contano(monkeypatch, tmp_path):
    # L'artifact di questo run puo' non essere ancora elencato: i bloccanti
    # della sua attestazione, anche incompleta, si leggono dal file locale.
    runs = [_run(11, SHA_1), _run(12, SHA_1)]
    arts = {11: [(NOME_ART, _zip(_attestazione(SHA_BASE, SHA_1, 11)))]}
    loc = _attestazione(SHA_BASE, SHA_1, 12, completa=False, bloccanti=[BLOCCANTE])
    code, out = _verify(monkeypatch, tmp_path, FakeGitHub(runs, arts), SHA_1, locale=loc)
    assert code == 1 and any("stake raddoppiato" in m for m in out["reasons"])


def test_lotto_con_risposta_incompleta_non_crea_bloccanti_sintetici():
    # Liveness: un errore del provider o un troncamento rendono gia' incompleta
    # la copertura; il marcatore «lotto non leggibile» lo trasformava anche in
    # un bloccante persistente, da triagiare dopo ogni errore transitorio.
    # I bloccanti reali estratti dal testo parziale restano.
    errore = {"indice": 1, "totale": 3, "text": "HTTP 401 no credit", "completo": False}
    troncato = {"indice": 2, "totale": 3, "completo": False,
                "text": "## Bloccanti\n- money.py: stake raddoppiato\n## Verdetto fin"}
    malformato = {"indice": 3, "totale": 3, "completo": True, "text": "## Problemi\n- x\n## Verdetto finale\nNo"}
    unita = rc.aggregate_reviews([errore, troncato, malformato])
    assert unita["lista_bloccanti"] == [
        "[lotto 2/3] money.py: stake raddoppiato",
        "[lotto 3/3] sezione Bloccanti assente, vuota o duplicata: lotto non leggibile",
    ]


# Decisione owner sul terzo audit (#539): i bloccanti sono legati anche ai commit.
class _NuovaPR(FakeGitHub):
    """La PR 8 riapre gli stessi commit della PR 7, chiusa con un bloccante aperto."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.query_per_sha = []

    def get(self, path):
        if "head_sha=" in path:
            self.query_per_sha.append(path.split("head_sha=")[1].split("&")[0])
        if path.split("?")[0].endswith("/pulls/8"):
            return {"created_at": "2026-10-23T12:00:00Z", "head": {"ref": "feature-2"}}
        return super().get(path.replace("/pulls/8/commits", "/pulls/7/commits")
                           .replace("/issues/8/comments", "/issues/7/comments"))


def test_nuova_pr_sugli_stessi_commit_eredita_i_bloccanti(monkeypatch, tmp_path):
    vecchia = dict(_run(11, SHA_1), pull_requests=[{"number": 7}])
    arts = {11: [(NOME_ART, _zip(_attestazione(SHA_BASE, SHA_1, 11, bloccanti=[BLOCCANTE])))]}
    gh = _NuovaPR([vecchia, _run(12, SHA_1)], arts)
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setattr(rc, "GitHub", lambda repo, token: gh)
    loc = tmp_path / "l.json"
    loc.write_text(json.dumps(_attestazione(SHA_BASE, SHA_1, 12, pr=8)))
    out_path = tmp_path / "o.json"
    code = rc.main(["verify", "--repo", REPO, "--pr", "8", "--reviewer", "gpt56sol", "--workflow-file",
                    "pr-review.yml", "--base-sha", SHA_BASE, "--head-sha", SHA_1, "--output", str(out_path),
                    "--local-attestation", str(loc)])
    out = json.loads(out_path.read_text())
    assert code == 1 and any("stake raddoppiato" in m for m in out["reasons"]), out["reasons"]
    # i run si cercano per SHA, non solo per data: quelli della PR chiusa sono
    # nati prima della PR nuova
    assert set(gh.query_per_sha) >= {SHA_1}
    # il triage dell'owner, anche sulla PR nuova, chiude il bloccante (l'id dipende dal range, non dalla PR)
    bid = rc.blocker_id("gpt56sol", BLOCCANTE, f"{SHA_BASE}...{SHA_1}")
    gh2 = _NuovaPR([vecchia, _run(12, SHA_1)], arts, comments=[_commento_triage(bid)])
    monkeypatch.setattr(rc, "GitHub", lambda repo, token: gh2)
    code = rc.main(["verify", "--repo", REPO, "--pr", "8", "--reviewer", "gpt56sol", "--workflow-file",
                    "pr-review.yml", "--base-sha", SHA_BASE, "--head-sha", SHA_1, "--output", str(out_path),
                    "--local-attestation", str(loc)])
    assert code == 0, json.loads(out_path.read_text())["reasons"]


def test_attestazione_di_un_altra_pr_su_commit_estranei_non_conta(monkeypatch, tmp_path):
    estraneo = "e" * 40
    altra = dict(_run(11, estraneo), pull_requests=[{"number": 9}])
    a = dict(_attestazione(SHA_BASE, estraneo, 11, bloccanti=[BLOCCANTE]), pr=9)
    gh = FakeGitHub([altra, _run(12, SHA_1)], {11: [(NOME_ART, _zip(a))]})
    code, out = _verify(monkeypatch, tmp_path, gh, SHA_1, locale=_attestazione(SHA_BASE, SHA_1, 12))
    assert code == 0, out["reasons"]


# Quarto audit indipendente su #539
def test_bloccanti_sintetici_di_lotti_diversi_si_triagiano_uno_per_uno(monkeypatch, tmp_path):
    rng = f"{SHA_BASE}...{SHA_1}"
    l1 = "[lotto 1/3] sezione Bloccanti assente, vuota o duplicata: lotto non leggibile"
    l3 = "[lotto 3/3] sezione Bloccanti assente, vuota o duplicata: lotto non leggibile"
    assert rc.blocker_id("gpt56sol", l1, rng) != rc.blocker_id("gpt56sol", l3, rng)
    a = _attestazione(SHA_BASE, SHA_1, 11, bloccanti=[l1, l3])
    gh = FakeGitHub([_run(11, SHA_1)], {11: [(NOME_ART, _zip(a))]},
                    comments=[_commento_triage(rc.blocker_id("gpt56sol", l1, rng))])
    code, out = _verify(monkeypatch, tmp_path, gh, SHA_1)
    assert code == 1 and [b["text"] for b in out["open_blockers"]] == [l3]


def test_run_accodato_sullo_stesso_head_non_si_aspetta(monkeypatch, tmp_path):
    # Un `edited` durante la review accoda un run sullo stesso head (stessa
    # concurrency): parte solo dopo questo e vedra' tutto. Aspettarlo dava un
    # rosso a vuoto dopo 600 s.
    monkeypatch.setenv("GITHUB_RUN_ID", "12")
    accodato = dict(_run(13, SHA_1), status="queued")
    gh = FakeGitHub([_run(11, SHA_1), dict(_run(12, SHA_1), status="in_progress"), accodato],
                    {11: [(NOME_ART, _zip(_attestazione(SHA_BASE, SHA_1, 11)))]})
    code, out = _verify(monkeypatch, tmp_path, gh, SHA_1)
    assert code == 0, out["reasons"]
    # ma un run in esecuzione sullo stesso head si aspetta comunque
    gh2 = FakeGitHub([_run(11, SHA_1), dict(_run(12, SHA_1), status="in_progress"),
                      dict(_run(13, SHA_1), status="in_progress")],
                     {11: [(NOME_ART, _zip(_attestazione(SHA_BASE, SHA_1, 11)))]})
    code, out = _verify(monkeypatch, tmp_path, gh2, SHA_1)
    assert code == 1


def test_id_di_un_artifact_illeggibile_non_dipende_dal_messaggio_di_errore():
    gh = FakeGitHub([_run(11, SHA_1)], {11: [(NOME_ART, b"non e' uno zip")]})
    _t, _m, _n, illeggibili = rc.attestazioni_con_bloccanti(
        gh, pr=7, reviewer="gpt56sol", workflow_file="pr-review.yml", pr_commits={SHA_1}, head_ref="",
        created_since="")
    gh2 = FakeGitHub([_run(11, SHA_1)], {11: [(NOME_ART, b"altro contenuto rotto")]})
    _t, _m, _n, illeggibili2 = rc.attestazioni_con_bloccanti(
        gh2, pr=7, reviewer="gpt56sol", workflow_file="pr-review.yml", pr_commits={SHA_1}, head_ref="",
        created_since="")
    assert illeggibili[0]["id"] == illeggibili2[0]["id"]


# ---------------------------------------------------------------------------
# Ciclo straordinario 11/11 (deroga owner su #539): bloccanti delle PR storiche
# pertinenti. La PR A (5) trova un bloccante sul commit Z, un force-push la
# porta su SHA_1 (review pulita), poi viene chiusa senza merge. La PR B (7) e'
# aperta dopo.
# ---------------------------------------------------------------------------
Z = "c" * 40
NUOVA_BASE = "b" * 40


def _storia(ref_a="feature", ref_b="feature", commits_b=(SHA_2,), merged_a=False, comments=None, extra_runs=()):
    pr_a = {"number": 5, "state": "closed", "merged_at": "2026-10-12T00:00:00Z" if merged_a else None,
            "created_at": "2026-10-05T00:00:00Z", "closed_at": "2026-10-12T00:00:00Z", "head": {"ref": ref_a}}
    pr_b = {"number": 7, "state": "open", "merged_at": None, "created_at": "2026-10-13T00:00:00Z",
            "closed_at": None, "head": {"ref": ref_b}}
    run_z = _run(21, Z, created="2026-10-06T00:00:00Z", branch=ref_a)
    run_1 = _run(22, SHA_1, created="2026-10-08T00:00:00Z", branch=ref_a)
    run_b = _run(12, commits_b[-1], created="2026-10-14T00:00:00Z", branch=ref_b)
    att_z = dict(_attestazione(SHA_BASE, Z, 21, bloccanti=[BLOCCANTE]), pr=5)
    att_1 = dict(_attestazione(SHA_BASE, SHA_1, 22), pr=5)
    return FakeGitHub(
        [run_z, run_1, run_b, *extra_runs], {21: [(NOME_ART, _zip(att_z))], 22: [(NOME_ART, _zip(att_1))]},
        commits=commits_b, pulls={5: pr_a, 7: pr_b}, pr_commits={5: [SHA_1], 7: list(commits_b)},
        commit_pulls={Z: {5}}, comments=comments or {},
    )


def _verifica_b(monkeypatch, tmp_path, gh, head, base=SHA_BASE):
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setattr(rc, "GitHub", lambda repo, token: gh)
    loc = tmp_path / "l.json"
    loc.write_text(json.dumps(_attestazione(base, head, 12)))
    out = tmp_path / "o.json"
    code = rc.main(["verify", "--repo", REPO, "--pr", "7", "--reviewer", "gpt56sol", "--workflow-file",
                    "pr-review.yml", "--base-sha", base, "--head-sha", head, "--output", str(out),
                    "--local-attestation", str(loc)])
    return code, json.loads(out.read_text())


def _ha_il_bloccante(out):
    return any("stake raddoppiato" in m for m in out["reasons"])


@pytest.mark.parametrize("commits_b", [(SHA_1,), (SHA_2,)], ids=["stessi-commit", "commit-riscritti-rebase"])
def test_s1_force_push_chiusura_e_nuova_pr_sullo_stesso_branch(commits_b, monkeypatch, tmp_path):
    # Scenari 1 e 3a: la PR A e' trovata per branch; i suoi run si leggono
    # nella finestra in cui era aperta, compreso quello sul commit Z.
    code, out = _verifica_b(monkeypatch, tmp_path, _storia(commits_b=commits_b), commits_b[-1])
    assert code == 1 and _ha_il_bloccante(out), out["reasons"]
    assert out["linked_prs"] == [5]


def test_s2_stessa_sequenza_con_branch_diverso_e_un_commit_in_comune(monkeypatch, tmp_path):
    # La PR A e' trovata dal commit in comune (`/commits/<sha>/pulls`), poi i
    # suoi run dal SUO branch: Z non e' fra i commit di B, il bloccante si'.
    gh = _storia(ref_b="altro", commits_b=(SHA_1, SHA_2))
    code, out = _verifica_b(monkeypatch, tmp_path, gh, SHA_2)
    assert code == 1 and _ha_il_bloccante(out) and out["linked_prs"] == [5]


def test_s4_cambio_di_base_non_cancella_il_bloccante(monkeypatch, tmp_path):
    # Stessa PR: review dell'intera PR contro la base nuova, pulita.
    runs = [_run(11, SHA_1), _run(12, SHA_1)]
    vecchia = _attestazione(SHA_BASE, SHA_1, 11, bloccanti=[BLOCCANTE])
    gh = FakeGitHub(runs, {11: [(NOME_ART, _zip(vecchia))]}, commits=(SHA_1,), base_ancestors={NUOVA_BASE})
    code, out = _verifica_b(monkeypatch, tmp_path, gh, SHA_1, base=NUOVA_BASE)
    assert code == 1 and _ha_il_bloccante(out)


def test_s7_triage_dell_owner_sulla_pr_storica_chiude_il_bloccante_ereditato(monkeypatch, tmp_path):
    bid = rc.blocker_id("gpt56sol", BLOCCANTE, f"{SHA_BASE}...{Z}")
    code, out = _verifica_b(monkeypatch, tmp_path, _storia(comments={5: [_commento_triage(bid)]}), SHA_2)
    assert code == 0, out["reasons"]
    # un triage scritto tramite app sulla PR storica non vale
    falso = _storia(comments={5: [_commento_triage(bid, app={"slug": "claude"})]})
    code, out = _verifica_b(monkeypatch, tmp_path, falso, SHA_2)
    assert code == 1


def test_s8_nuova_pr_indipendente_non_eredita_bloccanti_estranei(monkeypatch, tmp_path):
    # Branch diverso e nessun commit in comune con la PR A: nessun legame.
    code, out = _verifica_b(monkeypatch, tmp_path, _storia(ref_b="altro", commits_b=(SHA_2,)), SHA_2)
    assert code == 0, out["reasons"]
    assert out["linked_prs"] == []


def test_pr_storica_mergiata_non_si_eredita(monkeypatch, tmp_path):
    # Il branch designato si riusa fra PR successive: una PR gia' mergiata
    # (merge owner, gate superati) non trasmette i suoi bloccanti.
    code, out = _verifica_b(monkeypatch, tmp_path, _storia(merged_a=True), SHA_2)
    assert code == 0, out["reasons"]


def test_run_della_pr_storica_fuori_dalla_sua_finestra_non_conta(monkeypatch, tmp_path):
    # Un run sullo stesso branch di un'ALTRA epoca (prima che A esistesse) non e' di A.
    estraneo = _run(30, "d" * 40, created="2026-09-01T00:00:00Z", branch="feature")
    gh = _storia(extra_runs=[estraneo])
    gh.artifacts[30] = [(NOME_ART, _zip(dict(_attestazione(SHA_BASE, "d" * 40, 30,
                                                           bloccanti=["[lotto 1/1] altro problema"]), pr=4)))]
    code, out = _verifica_b(monkeypatch, tmp_path, gh, SHA_2)
    assert code == 1 and _ha_il_bloccante(out)
    assert not any("altro problema" in m for m in out["reasons"])


def test_troppe_pr_collegate_e_rosso(monkeypatch, tmp_path):
    gh = _storia()
    for n in range(40, 52):
        gh.pulls[n] = {"number": n, "state": "closed", "merged_at": None, "created_at": "2026-10-02T00:00:00Z",
                       "closed_at": "2026-10-03T00:00:00Z", "head": {"ref": "feature"}}
    code, out = _verifica_b(monkeypatch, tmp_path, gh, SHA_2)
    assert code == 1 and any("PR collegate" in m for m in out["reasons"])


def test_branch_riusato_a_lungo_non_esaurisce_la_lettura(monkeypatch, tmp_path):
    # Liveness: il branch designato si riusa fra PR successive. Senza la
    # finestra della PR storica, i run di altre epoche (qui 1100) superano il
    # tetto di pagine e la PR nuova resterebbe rossa senza un id da triagiare.
    antichi = [_run(100000 + i, "e" * 40, created="2026-09-01T00:00:00Z", branch="feature") for i in range(1100)]
    gh = _storia(extra_runs=antichi)
    bid = rc.blocker_id("gpt56sol", BLOCCANTE, f"{SHA_BASE}...{Z}")
    gh.comments = {5: [_commento_triage(bid)]}
    code, out = _verifica_b(monkeypatch, tmp_path, gh, SHA_2)
    assert code == 0, out["reasons"]
