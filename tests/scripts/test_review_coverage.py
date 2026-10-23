"""PFC-002 (M159): copertura attestata delle review AI — test del modulo.

RED -> GREEN sul difetto reale: il `build_patch_payload` di main tagliava ogni
patch oltre MAX_PATCH_PER_FILE_CHARS e saltava i file oltre il budget totale, e
il job restava verde. Il primo test esegue la funzione VERA presa da main (git)
e mostra la perdita; il secondo mostra che la segmentazione non perde nulla.
"""
from __future__ import annotations

import ast
import io
import json
import subprocess
import sys
import textwrap
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


def test_file_senza_patch_testo_blocca_binario_e_rename_no():
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
    solo_binari = rc.plan_review(files[:2], max_segment_chars=7000, max_batch_chars=24000, max_calls=8)
    att2 = rc.build_attestation(reviewer="r", model="m", repo=REPO, pr=1, range_base=SHA_BASE, range_head=SHA_1,
                                run_id=1, run_attempt=1, workflow_file="w.yml", plan=solo_binari, outcomes=[])
    assert att2["complete"] is True and att2["binary_files"] == ["img/logo.png"]


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


def _attestazione(base=SHA_BASE, head=SHA_1, run_id=11, completa=True, reviewer="gpt56sol", pr=7):
    plan = rc.plan_review([{"filename": "a.py", "patch": _patch(1, 3), "changes": 3}],
                          max_segment_chars=7000, max_batch_chars=24000, max_calls=8)
    ids = plan.batches[0]
    testo = rc.ACK_PREFIX + " " + ", ".join(ids if completa else ids[:-1] + ["F999S99"])
    esito = rc.batch_outcome(testo, completed=True, expected=ids)
    return rc.build_attestation(reviewer=reviewer, model="m", repo=REPO, pr=pr, range_base=base, range_head=head,
                                run_id=run_id, run_attempt=1, workflow_file="pr-review.yml", plan=plan,
                                outcomes=[esito])


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
    def __init__(self, runs, artifacts, commits=(SHA_1, SHA_2, SHA_3), base_ancestors=()):
        super().__init__(REPO, "token")
        self.runs = runs
        self.artifacts = artifacts  # run_id -> list[(name, bytes)]
        self.commits = list(commits)
        self.base_ancestors = set(base_ancestors)

    def get(self, path):
        if "/pulls/7/commits" in path:
            return [{"sha": s} for s in self.commits] if "page=1" in path else []
        if "/actions/workflows/" in path:
            return {"workflow_runs": self.runs} if "page=1" in path else {"workflow_runs": []}
        if path.endswith("/artifacts?per_page=100"):
            run_id = int(path.split("/runs/")[1].split("/")[0])
            return {"artifacts": [{"name": n, "archive_download_url": f"zip:{run_id}:{i}", "expired": False}
                                  for i, (n, _b) in enumerate(self.artifacts.get(run_id, []))]}
        if "/compare/" in path:
            sha = path.split("/compare/")[1].split("...")[0]
            return {"status": "ahead" if sha in self.base_ancestors else "diverged"}
        raise AssertionError(path)

    def download(self, url):
        _z, run_id, idx = url.split(":")
        return self.artifacts[int(run_id)][int(idx)][1]


def _run(run_id, head, event="pull_request_target", path=".github/workflows/pr-review.yml"):
    return {"id": run_id, "head_sha": head, "event": event, "path": path}


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
