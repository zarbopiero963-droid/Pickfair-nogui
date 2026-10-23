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

    def __call__(self, req, timeout=None):
        url = req.full_url
        if "api.github.com" in url:
            return self._github(req, url)
        return self._modello(req)

    def _github(self, req, url):
        metodo = req.get_method()
        if "/compare/" in url:
            return _Risposta(json.dumps({"files": self.files, "commits": [{"sha": HEAD}]}).encode())
        if "/issues/" in url and url.split("?")[0].endswith("/comments") and metodo == "GET":
            return _Risposta(json.dumps(self.commenti_esistenti).encode())
        if "/pulls/7/commits" in url:
            return _Risposta(json.dumps([{"sha": HEAD}] if "page=1" in url else []).encode())
        if "/actions/workflows/" in url:
            return _Risposta(json.dumps({"workflow_runs": []}).encode())
        if metodo in ("POST", "PATCH") and "/comments" in url:
            self.commenti.append(json.loads(req.data.decode())["body"])
            return _Risposta(b"{}")
        if "/labels" in url:
            return _Risposta(b"{}")
        raise AssertionError(f"chiamata GitHub inattesa: {metodo} {url}")

    def _modello(self, req):
        prompt = json.loads(req.data.decode())["messages"][1]["content"]
        self.prompt_ricevuti.append(prompt)
        richiesti = re.findall(r"### SEGMENTO (F\d{3}S\d{2,})", prompt)
        if self.modo == "errore":
            raise urllib.error.HTTPError(req.full_url, 401, "unauthorized", {}, io.BytesIO(b"no credit"))
        citati = list(richiesti)
        finish = "stop"
        if self.modo == "salta_uno" and len(self.prompt_ricevuti) == 1:
            citati = citati[:-1]
        if self.modo == "troncato" and len(self.prompt_ricevuti) == 1:
            finish = "length"
        testo = "## Bloccanti\nNessun bloccante evidente\n## Verdetto finale\nOK\nSEGMENTI ESAMINATI: " + ", ".join(citati)
        corpo = {"choices": [{"message": {"content": testo}, "finish_reason": finish}],
                 "usage": {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120}}
        return _Risposta(json.dumps(corpo).encode())


def _esegui(nome, files, tmp_path, monkeypatch, modo="ok", max_calls=None, done_marker_gia_presente=False):
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
        "EVENT_ACTION": "opened", "BEFORE_SHA": "", "AFTER_SHA": HEAD, "HEAD_SHA": HEAD, "BASE_SHA": BASE,
        "GITHUB_RUN_ID": "4242", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_ENV": str(gh_env),
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
