"""PFC-002 (M159): lo script VERO dei workflow Sol e Grok, eseguito end-to-end.

Lo script python incorporato nel workflow gira qui per intero, con la rete
sostituita: l'API GitHub e il provider del modello rispondono da un fake in
memoria. Si verifica cio' che conta per il gate:

- un diff piu' grande del budget viene diviso in lotti, e ogni segmento
  arriva al modello (nessun troncamento);
- l'attestazione e' completa SOLO se ogni lotto ha risposta completa e
  riscontra tutti i suoi segmenti; solo allora il commento porta il done_marker;
- lotto troncato, segmento non riscontrato, errore del provider, tetto di
  chiamate superato: attestazione incompleta, nessun done_marker, e nel caso
  del tetto nessuna chiamata a pagamento.
"""
from __future__ import annotations

import io
import json
import urllib.error
import re
import textwrap
import urllib.request
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = {
    "pr-review-openrouter-gpt56-sol.yml": ("OPENROUTER_API_KEY", "openrouter.ai"),
    "pr-review-xai-grok46.yml": ("XAI_API_KEY", "api.x.ai"),
}
BASE = "a" * 40
HEAD = "c" * 40
FORCE_BEFORE = "b" * 40  # head precedente riscritto da un force-push


def _script_e_env(nome: str):
    wf = yaml.safe_load((ROOT / ".github" / "workflows" / nome).read_text(encoding="utf-8"))
    job = wf["jobs"]["review"]
    passo = next(s for s in job["steps"] if "python3 <<'PY'" in str(s.get("run", "")))
    codice = passo["run"].split("python3 <<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
    return textwrap.dedent(codice), {k: str(v) for k, v in job["env"].items()}


class _Risposta(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _patch(file_idx: int, righe: int) -> str:
    out = [f"@@ -1,{righe} +1,{righe} @@\n"]
    out += [f"+    valore_{file_idx}_{i} = calcola({i}) + {'z' * 70}\n" for i in range(righe)]
    return "".join(out)


class Fake:
    """GitHub + provider in memoria. `modo` decide come risponde il modello."""

    def __init__(self, files, modo="ok"):
        self.files = files
        self.modo = modo
        self.prompt_ricevuti: list[str] = []
        self.commenti: list[str] = []
        self.commenti_esistenti: list[dict] = []
        self.compare_chiesti: list[str] = []
        self.pubblicazione_rotta = False
        self.barriera = None

    def __call__(self, req, timeout=None):
        url = req.full_url
        if "api.github.com" in url:
            return self._github(req, url)
        return self._modello(req)

    def _github(self, req, url):
        metodo = req.get_method()
        if "/compare/" in url:
            base = url.split("/compare/")[1].split("...")[0]
            self.compare_chiesti.append(base)
            status = "diverged" if base == FORCE_BEFORE else "ahead"
            return _Risposta(json.dumps({"files": self.files, "commits": [{"sha": HEAD}], "status": status}).encode())
        if "/issues/" in url and url.split("?")[0].endswith("/comments") and metodo == "GET":
            return _Risposta(json.dumps(self.commenti_esistenti).encode())
        if "/pulls/7/commits" in url:
            return _Risposta(json.dumps([{"sha": HEAD}] if "page=1" in url else []).encode())
        if "/actions/workflows/" in url:
            return _Risposta(json.dumps({"workflow_runs": []}).encode())
        if metodo in ("POST", "PATCH") and "/comments" in url:
            if self.pubblicazione_rotta:
                raise urllib.error.HTTPError(url, 403, "forbidden", {}, io.BytesIO(b"read-only token"))
            self.commenti.append(json.loads(req.data.decode())["body"])
            return _Risposta(b"{}")
        if "/labels" in url:
            return _Risposta(b"{}")
        raise AssertionError(f"chiamata GitHub inattesa: {metodo} {url}")

    def _modello(self, req):
        prompt = json.loads(req.data.decode())["messages"][1]["content"]
        self.prompt_ricevuti.append(prompt)
        if self.barriera is not None:
            self.barriera.wait()  # rotta (timeout) se i lotti non girano tutti insieme
        richiesti = re.findall(r"### SEGMENTO (F\d{3}S\d{2,})", prompt)
        if self.modo == "errore":
            raise urllib.error.HTTPError(req.full_url, 401, "unauthorized", {}, io.BytesIO(b"no credit"))
        citati = list(richiesti)
        finish = "stop"
        if self.modo == "salta_uno" and len(self.prompt_ricevuti) == 1:
            citati = citati[:-1]
        if self.modo == "troncato" and len(self.prompt_ricevuti) == 1:
            finish = "length"
        bloccanti = "Nessun bloccante evidente"
        if self.modo == "bloccante_nel_secondo" and len(self.prompt_ricevuti) == 2:
            bloccanti = "- modulo_x.py: stake raddoppiato"
        corpo_extra = "x" * 33000 if self.modo == "lungo" else ""
        testo = (f"## Bloccanti\n{bloccanti}\n## Verdetto finale\nOK {corpo_extra}\n"
                 "SEGMENTI ESAMINATI: " + ", ".join(citati))
        corpo = {"choices": [{"message": {"content": testo}, "finish_reason": finish}],
                 "usage": {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120}}
        return _Risposta(json.dumps(corpo).encode())


def _esegui(nome, files, tmp_path, monkeypatch, modo="ok", max_calls=None, done_marker_gia_presente=False,
            evento="opened", before="", prepara=None, base_from=""):
    codice, env = _script_e_env(nome)
    chiave, _host = WORKFLOWS[nome]
    base_dir = tmp_path / "_base" / "scripts" / "ci"
    base_dir.mkdir(parents=True)
    (base_dir / "review_coverage.py").write_text(
        (ROOT / "scripts" / "ci" / "review_coverage.py").read_text(encoding="utf-8"), encoding="utf-8")
    gh_env = tmp_path / "github_env"
    gh_env.write_text("")
    valori = {k: v for k, v in env.items() if "${{" not in v}
    valori.update({
        chiave: "chiave-finta", "GITHUB_TOKEN": "token-finto", "REPO": "owner/repo", "PR_NUMBER": "7",
        "EVENT_ACTION": evento, "BEFORE_SHA": before, "AFTER_SHA": HEAD, "HEAD_SHA": HEAD, "BASE_SHA": BASE,
        "BASE_FROM": base_from, "GITHUB_RUN_ID": "4242", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_ENV": str(gh_env),
        "GITHUB_WORKSPACE": str(tmp_path), "ATTESTATION_FILE": str(tmp_path / "rc" / "attestation.json"),
    })
    if max_calls is not None:
        valori["MAX_REVIEW_CALLS"] = str(max_calls)
    for k, v in valori.items():
        monkeypatch.setenv(k, v)
    fake = Fake(files, modo)
    if done_marker_gia_presente:
        rid = env["REVIEW_ID"]
        fake.commenti_esistenti = [{"user": {"login": "github-actions[bot]"},
                                    "body": f"<!-- {rid}-pr-review-done:pickfair-nogui:{BASE[:12]}...{HEAD[:12]} -->"}]
    if prepara is not None:
        prepara(fake)
    monkeypatch.setattr(urllib.request, "urlopen", fake)
    monkeypatch.setattr("time.sleep", lambda *_a: None)
    monkeypatch.syspath_prepend(str(base_dir))
    try:
        exec(compile(codice, nome, "exec"), {"__name__": "__main__"})  # skipcq: PY-W0122 - codice versionato del repo
    except SystemExit as exc:
        assert exc.code in (0, None)
    att_path = tmp_path / "rc" / "attestation.json"
    att = json.loads(att_path.read_text()) if att_path.exists() else None
    return fake, att, env


def _diff_grande(n_file: int, righe: int):
    return [{"filename": f"modulo_{i}.py", "status": "modified", "changes": righe, "additions": righe,
             "deletions": 0, "patch": _patch(i, righe)} for i in range(n_file)]


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_diff_oltre_il_budget_diviso_in_lotti_e_coperto_per_intero(nome, tmp_path, monkeypatch):
    files = _diff_grande(4, 220)  # ~4 x 22 000 caratteri: ben oltre il budget di un prompt
    fake, att, env = _esegui(nome, files, tmp_path, monkeypatch)
    assert att is not None and att["complete"] is True, att and att["reasons"]
    assert len(fake.prompt_ricevuti) == len(att["batches"]) > 1
    tutto = "".join(fake.prompt_ricevuti)
    for i in range(4):
        for r in range(220):
            assert f"valore_{i}_{r} " in tutto, f"riga {i}/{r} mai arrivata al modello"
    assert "TRONCATO" not in tutto
    budget = int(env["MAX_TOTAL_PATCH_CHARS"])
    for p in fake.prompt_ricevuti:
        diff = p.split("(segmenti di questo lotto):", 1)[1]
        assert len(diff) <= budget + 2
    assert len(fake.commenti) == 1 and "-pr-review-done:" in fake.commenti[0]
    assert "COMPLETA" in fake.commenti[0]
    artifact = (tmp_path / "github_env").read_text()
    assert artifact.startswith("COVERAGE_ARTIFACT=review-coverage-")


@pytest.mark.parametrize("modo", ["salta_uno", "troncato", "errore"])
@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_lotto_incompleto_rende_incompleta_la_review(nome, modo, tmp_path, monkeypatch):
    fake, att, _env = _esegui(nome, _diff_grande(4, 220), tmp_path, monkeypatch, modo=modo)
    assert att is not None and att["complete"] is False
    assert att["unreviewed_segments"]
    assert len(fake.commenti) == 1
    assert "-pr-review-done:" not in fake.commenti[0], "una review incompleta non deve deduplicare i re-run"
    assert "INCOMPLETA" in fake.commenti[0]


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_oltre_il_tetto_di_lotti_nessuna_chiamata_a_pagamento(nome, tmp_path, monkeypatch):
    fake, att, _env = _esegui(nome, _diff_grande(6, 220), tmp_path, monkeypatch, max_calls=1)
    assert fake.prompt_ricevuti == []
    assert att["complete"] is False and any("tetto" in m for m in att["reasons"])
    assert "-pr-review-done:" not in fake.commenti[0]


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_diff_piccolo_un_solo_lotto(nome, tmp_path, monkeypatch):
    fake, att, _env = _esegui(nome, _diff_grande(1, 10), tmp_path, monkeypatch)
    assert len(fake.prompt_ricevuti) == 1 and att["complete"] is True


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_done_marker_senza_artifact_ufficiale_rifa_la_review(nome, tmp_path, monkeypatch):
    # Rilievo Codex su #539: il done_marker resta nel commento anche se
    # l'artifact manca (upload fallito, scaduto, cancellato). Saltare la review
    # lascerebbe il check rosso per sempre: senza artifact ufficiale si rifa'.
    fake, att, _env = _esegui(nome, _diff_grande(1, 10), tmp_path, monkeypatch, done_marker_gia_presente=True)
    assert len(fake.prompt_ricevuti) == 1
    assert att is not None and att["complete"] is True


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_force_push_rivede_l_intera_pr_dalla_base(nome, tmp_path, monkeypatch):
    # Rilievo Codex su #539: dopo un force-push BEFORE non e' antenato di HEAD; un
    # range BEFORE...HEAD partirebbe da un commit che non e' piu' della PR e la
    # catena delle attestazioni si spezzerebbe per sempre.
    fake, att, _env = _esegui(nome, _diff_grande(1, 10), tmp_path, monkeypatch,
                              evento="synchronize", before=FORCE_BEFORE)
    assert fake.compare_chiesti[:2] == [FORCE_BEFORE, BASE]
    assert att["range_base"] == BASE and att["range_head"] == HEAD and att["complete"] is True
    # push ordinario: il range resta quello della push
    fake2, att2, _ = _esegui(nome, _diff_grande(1, 10), tmp_path / "b", monkeypatch,
                             evento="synchronize", before="d" * 40)
    assert att2["range_base"] == "d" * 40


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_review_non_pubblicata_non_copre_niente(nome, tmp_path, monkeypatch):
    # Rilievo Codex su #539: con il commento non pubblicato (403, 5xx) nessuno
    # puo' leggere i bloccanti; l'attestazione deve restare incompleta.
    def rompi(fake):
        fake.pubblicazione_rotta = True
    fake, att, _env = _esegui(nome, _diff_grande(1, 10), tmp_path, monkeypatch, prepara=rompi)
    assert len(fake.prompt_ricevuti) == 1
    assert att["complete"] is False
    assert any("non pubblicato" in m for m in att["reasons"])


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_tutti_i_lotti_girano_insieme(nome, tmp_path, monkeypatch):
    # Rilievo Codex su #539: con meno worker che lotti le chiamate andrebbero in
    # piu' ondate e il caso peggiore uscirebbe dal timeout del job. La barriera
    # si rompe (timeout) se anche un solo lotto non parte insieme agli altri.
    import threading
    from scripts.ci import review_coverage as rc_mod  # noqa: F401  (solo per il calcolo dei lotti)
    _codice, env = _script_e_env(nome)
    tetto = int(env["MAX_REVIEW_CALLS"])

    def conta(files):
        return len(rc_mod.plan_review(
            [{"filename": f["filename"], "patch": f["patch"], "changes": f["changes"]} for f in files],
            max_segment_chars=int(env["MAX_PATCH_PER_FILE_CHARS"]),
            max_batch_chars=int(env["MAX_TOTAL_PATCH_CHARS"]), max_calls=tetto).batches)

    # il diff piu' grande che sta ancora nel tetto di lotti, cioe' il caso a piu' ondate
    n = 1
    while conta(_diff_grande(n + 1, 220)) <= tetto:
        n += 1
    files = _diff_grande(n, 220)
    lotti = conta(files)
    assert 4 < lotti <= tetto, lotti

    def barriera(fake):
        fake.barriera = threading.Barrier(lotti, timeout=10)
    fake, att, _ = _esegui(nome, files, tmp_path, monkeypatch, prepara=barriera)
    assert len(fake.prompt_ricevuti) == lotti
    assert att["complete"] is True, att["reasons"]


def _valida(commento: str, rid: str):
    import importlib.util
    spec = importlib.util.spec_from_file_location("pol", ROOT / "scripts" / "pr_autonomy_policy.py")
    pol = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pol)
    nome = "sol" if rid == "gpt56sol" else "grok"
    return pol.validate_review({"reviewer": nome, "author": "github-actions[bot]", "body": commento}, HEAD)


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_review_a_piu_lotti_leggibile_dal_gate(nome, tmp_path, monkeypatch):
    # Rilievo Codex su #539: con piu' lotti il commento aveva una coppia
    # Bloccanti/Verdetto per lotto, e pr_autonomy_policy._section tratta le
    # sezioni duplicate come schema inatteso (UNKNOWN).
    fake, att, env = _esegui(nome, _diff_grande(4, 220), tmp_path, monkeypatch)
    assert len(fake.prompt_ricevuti) > 1 and att["complete"] is True
    assert _valida(fake.commenti[0], env["REVIEW_ID"])["status"] == "VALID_CLEAN"


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_un_bloccante_in_un_lotto_resta_visibile_al_gate(nome, tmp_path, monkeypatch):
    fake, att, env = _esegui(nome, _diff_grande(4, 220), tmp_path, monkeypatch, modo="bloccante_nel_secondo")
    esito = _valida(fake.commenti[0], env["REVIEW_ID"])
    assert esito["status"] == "VALID_WITH_BLOCKERS"
    assert "stake raddoppiato" in fake.commenti[0].split("## Bloccanti", 1)[1].split("## Verdetto finale")[0]
    # Rilievo Codex su #539: i bloccanti entrano nell'attestazione, e la verifica
    # della copertura (il check obbligatorio) diventa rossa.
    assert att["complete"] is True
    assert any("stake raddoppiato" in b for b in att["blocking_findings"])


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_review_pulita_attesta_zero_bloccanti(nome, tmp_path, monkeypatch):
    _fake, att, _env = _esegui(nome, _diff_grande(1, 10), tmp_path, monkeypatch)
    assert att["complete"] is True and att["blocking_findings"] == []


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_range_senza_patch_testuale_pubblica_le_sezioni_canoniche(nome, tmp_path, monkeypatch):
    # Rilievo Codex su #539: un range di soli rename pubblicava il done_marker
    # senza `## Bloccanti` / `## Verdetto finale`, e il gate lo leggeva UNKNOWN.
    files = [{"filename": "nuovo.py", "previous_filename": "vecchio.py", "status": "renamed",
              "changes": 0, "additions": 0, "deletions": 0}]
    fake, att, env = _esegui(nome, files, tmp_path, monkeypatch)
    assert fake.prompt_ricevuti == []
    assert att["complete"] is True and att["blocking_findings"] == []
    assert "-pr-review-done:" in fake.commenti[0]
    assert _valida(fake.commenti[0], env["REVIEW_ID"])["status"] == "VALID_CLEAN"


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_oltre_il_tetto_le_sezioni_dicono_bloccante(nome, tmp_path, monkeypatch):
    fake, att, env = _esegui(nome, _diff_grande(6, 220), tmp_path, monkeypatch, max_calls=1)
    sezione = fake.commenti[0].split("## Bloccanti", 1)[1].split("## Verdetto finale")[0]
    assert "Nessun bloccante" not in sezione and "copertura" in sezione


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_edited_senza_cambio_di_base_non_chiama_il_modello(nome, tmp_path, monkeypatch):
    # Rilievo Codex su #539: `edited` serve per il cambio di base; un titolo o
    # una descrizione modificati non devono ripagare la review. Nessuna
    # attestazione nuova: la verifica usa quelle ufficiali gia' caricate.
    fake, att, _env = _esegui(nome, _diff_grande(1, 10), tmp_path, monkeypatch, evento="edited")
    assert fake.prompt_ricevuti == [] and fake.commenti == [] and att is None


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_cambio_di_base_rivede_l_intera_pr(nome, tmp_path, monkeypatch):
    fake, att, _env = _esegui(nome, _diff_grande(1, 10), tmp_path, monkeypatch, evento="edited",
                              base_from="release")
    assert len(fake.prompt_ricevuti) == 1
    assert att["range_base"] == BASE and att["range_head"] == HEAD and att["complete"] is True


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_review_oltre_il_limite_del_commento_non_copre(nome, tmp_path, monkeypatch):
    # Rilievo Codex su #539: il commento veniva tagliato a 64000 caratteri in
    # silenzio, perdendo i bloccanti dei lotti in coda.
    fake, att, env = _esegui(nome, _diff_grande(2, 220), tmp_path, monkeypatch, modo="lungo")
    assert att["complete"] is False and any("oltre il limite" in m for m in att["reasons"])
    assert "-pr-review-done:" not in fake.commenti[0]


# ---------------------------------------------------------------------------
# Audit indipendente su #539 (D1/D2): i due falsi verdi riprodotti end-to-end,
# con lo script vero dei workflow e la verifica vera. Ora restano rossi.
# ---------------------------------------------------------------------------
def _moduli_verifica():
    import importlib.util
    import sys
    sys.path.insert(0, str(ROOT / "scripts" / "ci"))
    spec = importlib.util.spec_from_file_location("trc_e2e", ROOT / "tests" / "scripts" / "test_review_coverage.py")
    trc = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(trc)
    return trc


def _verifica(trc, monkeypatch, tmp_path, gh, head):
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setattr(trc.rc, "GitHub", lambda repo, token: gh)
    out = tmp_path / "verifica.json"
    code = trc.rc.main(["verify", "--repo", "owner/repo", "--pr", "7", "--reviewer", "gpt56sol",
                        "--workflow-file", "pr-review.yml", "--base-sha", BASE, "--head-sha", head,
                        "--output", str(out)])
    return code, json.loads(out.read_text())


def _zip_att(att):
    import zipfile
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("attestation.json", json.dumps(att))
    return buf.getvalue()


def _ufficiale(att, run_id, attempt=1):
    return {**att, "reviewer": "gpt56sol", "run_id": run_id, "run_attempt": attempt}


class _BloccantePoiTroncato(Fake):
    """Tentativo 1: lotto 1 con un bloccante, lotto 2 troncato. Tentativo 2: tutto pulito."""

    def _modello(self, req):
        prompt = json.loads(req.data.decode())["messages"][1]["content"]
        self.prompt_ricevuti.append(prompt)
        ids = re.findall(r"### SEGMENTO (F\d{3}S\d{2,})", prompt)
        if self.modo == "tentativo1" and len(self.prompt_ricevuti) == 1:
            testo, fine = ("## Bloccanti\n- money.py: stake raddoppiato\n## Verdetto finale\nNo\n"
                           "SEGMENTI ESAMINATI: " + ", ".join(ids)), "stop"
        elif self.modo == "tentativo1":
            testo, fine = "## Bloccanti\nNessun", "length"
        else:
            testo, fine = ("## Bloccanti\nNessun bloccante evidente\n## Verdetto finale\nOK\n"
                           "SEGMENTI ESAMINATI: " + ", ".join(ids)), "stop"
        return _Risposta(json.dumps({"choices": [{"message": {"content": testo}, "finish_reason": fine}],
                                     "usage": {"prompt_tokens": 1, "completion_tokens": 1}}).encode())


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_d1_rerun_dopo_lotto_troncato_non_cancella_il_bloccante(nome, tmp_path, monkeypatch):
    trc = _moduli_verifica()
    files = _diff_grande(4, 220)
    originale = globals()["Fake"]
    try:
        globals()["Fake"] = lambda f, m: _BloccantePoiTroncato(f, "tentativo1")
        _f1, att1, _ = _esegui(nome, files, tmp_path / "t1", monkeypatch)
        globals()["Fake"] = lambda f, m: _BloccantePoiTroncato(f, "tentativo2")
        monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
        _f2, att2, _ = _esegui(nome, files, tmp_path / "t2", monkeypatch)
    finally:
        globals()["Fake"] = originale
    assert att1["complete"] is False and any("stake raddoppiato" in b for b in att1["blocking_findings"])
    assert att2["complete"] is True and att2["blocking_findings"] == []
    run = {"id": 4242, "head_sha": HEAD, "event": "pull_request_target", "path": ".github/workflows/pr-review.yml",
            "status": "completed"}
    gh = trc.FakeGitHub([run], {4242: [("review-coverage-gpt56sol-a1", _zip_att(_ufficiale(att1, 4242, 1))),
                                       ("review-coverage-gpt56sol-a2", _zip_att(_ufficiale(att2, 4242, 2)))]},
                        commits=(HEAD,))
    code, out = _verifica(trc, monkeypatch, tmp_path, gh, HEAD)
    assert code == 1 and any("stake raddoppiato" in m for m in out["reasons"])


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_d2_commit_vuoto_dopo_un_bloccante_resta_rosso(nome, tmp_path, monkeypatch):
    trc = _moduli_verifica()
    intermedio = "d" * 40
    fake, att_vuota, _ = _esegui(nome, [], tmp_path, monkeypatch, evento="synchronize", before=intermedio)
    assert fake.prompt_ricevuti == [] and att_vuota["complete"] is True and att_vuota["blocking_findings"] == []
    att_x = trc._attestazione(BASE, intermedio, 11, bloccanti=["money.py: stake raddoppiato"])
    runs = [{"id": 11, "head_sha": intermedio, "event": "pull_request_target", "path": ".github/workflows/pr-review.yml",
            "status": "completed"},
            {"id": 4242, "head_sha": HEAD, "event": "pull_request_target", "path": ".github/workflows/pr-review.yml",
            "status": "completed"}]
    gh = trc.FakeGitHub(runs, {11: [("review-coverage-gpt56sol-x", _zip_att(att_x))],
                               4242: [("review-coverage-gpt56sol-y", _zip_att(_ufficiale(att_vuota, 4242)))]},
                        commits=(intermedio, HEAD))
    code, out = _verifica(trc, monkeypatch, tmp_path, gh, HEAD)
    assert code == 1 and any("stake raddoppiato" in m for m in out["reasons"])


@pytest.mark.parametrize("nome", sorted(WORKFLOWS))
def test_errore_del_provider_poi_rerun_pulito_non_chiede_triage(nome, tmp_path, monkeypatch):
    # Terzo audit su #539: l'errore transitorio (crediti, 5xx) lasciava un
    # bloccante sintetico persistente anche dopo un rerun pulito.
    trc = _moduli_verifica()
    files = _diff_grande(1, 20)
    _f, att1, _ = _esegui(nome, files, tmp_path / "t1", monkeypatch, modo="errore")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    _f, att2, _ = _esegui(nome, files, tmp_path / "t2", monkeypatch, modo="ok")
    assert att1["complete"] is False and att1["blocking_findings"] == []
    assert att2["complete"] is True and att2["blocking_findings"] == []
    run = {"id": 4242, "head_sha": HEAD, "event": "pull_request_target", "path": ".github/workflows/pr-review.yml",
           "status": "completed"}
    gh = trc.FakeGitHub([run], {4242: [("review-coverage-gpt56sol-a1", _zip_att(_ufficiale(att1, 4242, 1))),
                                       ("review-coverage-gpt56sol-a2", _zip_att(_ufficiale(att2, 4242, 2)))]},
                        commits=(HEAD,))
    code, out = _verifica(trc, monkeypatch, tmp_path, gh, HEAD)
    assert code == 0, out["reasons"]
