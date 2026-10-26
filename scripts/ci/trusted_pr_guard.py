#!/usr/bin/env python3
"""Controlli PR della lane Buildkite `trusted` (issue #562): overlap guard e merge readiness.

Gira su un checkout di `main` (mai il codice della PR) e legge la PR SOLO dalla GitHub REST API. Contratto, uguale
per i due wrapper `scripts/ci/pr_overlap_guard.sh` e `scripts/ci/pr_merge_readiness.sh`:

- nessun argomento da riga di comando (l'agent trusted gira con `no-command-eval`);
- `PF_PR_NUMBER` (obbligatorio): intero ASCII senza zeri iniziali;
- `PF_PR_HEAD_SHA` (obbligatorio): 40 esadecimali minuscoli, l'head per cui il verdetto e' chiesto. Se la PR ha un
  head diverso (push nel frattempo) il verdetto e' FAIL: mai un esito attaccato a uno SHA che non e' stato valutato;
- `PF_PR_BASE_REF` (facoltativo, default `main`): branch base atteso;
- token: file `PF_GITHUB_TOKEN_FILE`, altrimenti `$CREDENTIALS_DIRECTORY/github-token` (systemd `LoadCredential`).
  Mai da argv, mai stampato; file leggibile da gruppo/altri => rifiuto. Token classico con scope => rifiuto (un
  token classico con scope `repo`/`public_repo` scrive): serve un token fine-grained in sola lettura;
- `PF_RESULT_DIR` (facoltativo, default `.trusted-guard`): dove scrivere `<guard>.json` con PR, head, base, verdetto
  e motivi, per chi pubblica lo status sullo SHA della PR;
- solo per la readiness, `PF_READINESS_WAIT_SECONDS` (facoltativo, default 300, da 31 a 6600): attesa dei check
  pendenti; scaduta l'attesa con check non finiti => FAIL. Il minimo garantisce almeno due letture stabili (con 0 la
  readiness giudicherebbe una sola istantanea, prima che i check tardivi si registrino).

Provenienza verificata prima di ogni verdetto: repository fisso (e `BUILDKITE_REPO`, se presente, deve coincidere),
contesto Buildkite non di PR e su `main`, PR aperta, base = repository e branch attesi, base SHA antenato di `main`,
head nello stesso repository (niente fork), head = `PF_PR_HEAD_SHA`; a fine valutazione head, stato, repository e
branch base riletti uguali. La readiness e' FAIL anche con la label `manual-review-required` o con un file a merge
owner nel diff (`scripts/pr_autonomy_policy.is_owner_manual_path`): PASS vuol dire mergiabile senza decisioni owner.

Exit 0 solo con verdetto PASS; ogni altro esito (dato mancante, incoerente, errore di rete o API, check rosso o
pendente, review non conclusiva) => exit 1. Nessuna dipendenza oltre la libreria standard e `api.github.com`
(la readiness delega a `scripts/pr_merge_readiness.py`, che usa la CLI `gh`).
"""
from __future__ import annotations

import importlib.util
import json
import os
import re
import stat
import subprocess  # nosec B404 - solo per lanciare scripts/pr_merge_readiness.py con argv fisso
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

TRUSTED_REPO = "zarbopiero963-droid/Pickfair-nogui"
API_ROOT = "https://api.github.com"
DEFAULT_BASE_REF = "main"
# Stessi file critici del workflow `.github/workflows/pr-overlap-guard.yml` (riserva GitHub Actions).
CRITICAL_FILES = frozenset({
    "core/trading_engine.py",
    "order_manager.py",
    "core/reconciliation_engine.py",
    "core/state_recovery.py",
    "database.py",
})
READINESS_POLL_SECONDS = 15
READINESS_WAIT_MIN = 2 * READINESS_POLL_SECONDS + 1  # almeno due letture: con 0 si giudica una sola istantanea
READINESS_WAIT_DEFAULT = 300           # sta dentro il timeout di 10 minuti della pipeline trusted (vedi sotto)
READINESS_WAIT_MAX = 6600              # come la readiness GitHub Actions
# Margine oltre l'attesa per la decisione finale della readiness. Il job deve stare nel timeout della pipeline:
# timeout pipeline >= attesa + READINESS_MARGIN_SECONDS + API_CALLS_BUDGET + checkout (~60 s):
# 300 + 120 + 120 + 60 = 600 s = 10 minuti.
READINESS_MARGIN_SECONDS = 120
API_CALLS_BUDGET = 4 * 30              # token, PR, compare, rilettura finale: HTTP_TIMEOUT ciascuna
MANUAL_LABEL = "manual-review-required"
HTTP_TIMEOUT = 30
MAX_PAGES = 50                         # 50 x 100 = 5000 elementi: oltre, il dato non e' verificabile
GITHUB_FILES_CAP = 3000                # l'API dei file di una PR non ne elenca piu' di 3000

_PR_NUMBER_RE = re.compile(r"[1-9][0-9]{0,6}", re.ASCII)
_SHA_RE = re.compile(r"[0-9a-f]{40}", re.ASCII)
_REF_RE = re.compile(r"[A-Za-z0-9._/-]{1,100}", re.ASCII)
_LINK_NEXT_RE = re.compile(r'<([^>]+)>;\s*rel="next"')
_REPO_URL_RE = re.compile(
    r"(?:https://github\.com/|git@github\.com:|ssh://git@github\.com/)([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?)(?:\.git)?/?",
    re.ASCII)


class GuardError(Exception):
    """Dato mancante, incoerente o non verificabile: verdetto FAIL."""


# --------------------------------------------------------------------------------------------- ambiente
def parse_environment(env: Mapping[str, str]) -> dict[str, Any]:
    """Valida l'ambiente controllato. Ogni valore assente o fuori formato => GuardError."""
    raw_pr = env.get("PF_PR_NUMBER")
    if raw_pr is None or not _PR_NUMBER_RE.fullmatch(raw_pr):
        raise GuardError("PF_PR_NUMBER assente o non valido (intero ASCII senza zeri iniziali)")
    head = env.get("PF_PR_HEAD_SHA")
    if head is None or not _SHA_RE.fullmatch(head):
        raise GuardError("PF_PR_HEAD_SHA assente o non valido (40 esadecimali minuscoli)")
    base_ref = env.get("PF_PR_BASE_REF", DEFAULT_BASE_REF)
    if not _REF_RE.fullmatch(base_ref) or ".." in base_ref:
        raise GuardError("PF_PR_BASE_REF non valido")
    bk_repo = env.get("BUILDKITE_REPO")
    if bk_repo is not None:
        m = _REPO_URL_RE.fullmatch(bk_repo.strip())
        if not m or m.group(1).lower() != TRUSTED_REPO.lower():
            raise GuardError("BUILDKITE_REPO non e' il repository atteso")
    bk_pr = env.get("BUILDKITE_PULL_REQUEST")
    if bk_pr is not None and bk_pr not in ("", "false"):
        raise GuardError("contesto Buildkite di PR: la lane trusted gira solo su main")
    bk_branch = env.get("BUILDKITE_BRANCH")
    if bk_branch is not None and bk_branch != DEFAULT_BASE_REF:
        raise GuardError("BUILDKITE_BRANCH diverso da main")
    return {"pr": int(raw_pr), "head_sha": head, "base_ref": base_ref}


def readiness_wait_seconds(env: Mapping[str, str]) -> int:
    raw = env.get("PF_READINESS_WAIT_SECONDS")
    if raw is None:
        return READINESS_WAIT_DEFAULT
    if not re.fullmatch(r"[0-9]{1,5}", raw, re.ASCII) or not READINESS_WAIT_MIN <= int(raw) <= READINESS_WAIT_MAX:
        raise GuardError(f"PF_READINESS_WAIT_SECONDS non valido ({READINESS_WAIT_MIN}..{READINESS_WAIT_MAX})")
    return int(raw)


def load_token(env: Mapping[str, str]) -> str:
    """Token GitHub dal file indicato. Mai da argv o da una variabile d'ambiente del job."""
    path = env.get("PF_GITHUB_TOKEN_FILE")
    if not path:
        cred_dir = env.get("CREDENTIALS_DIRECTORY")
        if not cred_dir:
            raise GuardError("token GitHub non fornito (PF_GITHUB_TOKEN_FILE o CREDENTIALS_DIRECTORY/github-token)")
        path = os.path.join(cred_dir, "github-token")
    try:
        st = os.stat(path)
    except OSError as exc:
        raise GuardError(f"file del token non leggibile: {type(exc).__name__}") from None
    if not stat.S_ISREG(st.st_mode):
        raise GuardError("il token non e' un file regolare")
    if st.st_mode & 0o077:
        raise GuardError("file del token accessibile a gruppo o altri: richiesti permessi 0600/0400")
    try:
        with open(path, encoding="ascii") as fh:
            token = fh.read().strip()
    except (OSError, UnicodeDecodeError) as exc:
        raise GuardError(f"file del token non leggibile: {type(exc).__name__}") from None
    if not re.fullmatch(r"[A-Za-z0-9_]{20,255}", token, re.ASCII):
        raise GuardError("token GitHub vuoto o in formato non valido")
    return token


# --------------------------------------------------------------------------------------------- GitHub API
Transport = Callable[[str, Mapping[str, str]], "tuple[int, Mapping[str, str], bytes]"]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """urllib ripete l'header Authorization anche su un redirect verso un altro host: nessun redirect seguito."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401 - firma di urllib
        return None


def urllib_transport(url: str, headers: Mapping[str, str]) -> "tuple[int, Mapping[str, str], bytes]":
    opener = urllib.request.build_opener(_NoRedirect)
    req = urllib.request.Request(url, headers=dict(headers), method="GET")
    try:
        with opener.open(req, timeout=HTTP_TIMEOUT) as resp:  # nosec B310 - URL verificato su API_ROOT
            return resp.status, dict(resp.headers.items()), resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers.items()) if exc.headers else {}, b""


class GitHub:
    """Client minimo in sola lettura, ristretto a https://api.github.com/."""

    def __init__(self, token: str, transport: Transport = urllib_transport) -> None:
        self._token = token
        self._transport = transport

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}", "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "pickfair-trusted-guard"}

    def _get(self, url: str) -> "tuple[Any, Mapping[str, str]]":
        if not url.startswith(API_ROOT + "/"):
            raise GuardError("URL fuori da api.github.com rifiutato")
        try:
            status, headers, body = self._transport(url, self._headers())
        except GuardError:
            raise
        except Exception as exc:  # rete, TLS, timeout: esito non verificabile
            raise GuardError(f"GitHub API non raggiungibile: {type(exc).__name__}") from None
        if status != 200:
            raise GuardError(f"GitHub API HTTP {status} su {url[len(API_ROOT):].split('?')[0]}")
        try:
            return json.loads(body.decode("utf-8")), headers
        except (UnicodeDecodeError, ValueError):
            raise GuardError("risposta GitHub non JSON") from None

    def get(self, path: str) -> Any:
        return self._get(API_ROOT + path)[0]

    def get_with_headers(self, path: str) -> "tuple[Any, Mapping[str, str]]":
        return self._get(API_ROOT + path)

    def get_all(self, path: str) -> list:
        """Tutte le pagine di una lista. Una pagina non lista o troppe pagine => GuardError, mai un elenco parziale."""
        url: Optional[str] = API_ROOT + path
        items: list = []
        for _ in range(MAX_PAGES):
            page, headers = self._get(url)  # type: ignore[arg-type]
            if not isinstance(page, list):
                raise GuardError("pagina GitHub non e' una lista")
            items.extend(page)
            link = next((v for k, v in headers.items() if k.lower() == "link"), "")
            m = _LINK_NEXT_RE.search(link or "")
            if not m:
                return items
            url = m.group(1)
        raise GuardError("troppe pagine: elenco non verificabile")


def check_token_privileges(gh: GitHub) -> None:
    """Token classico con scope => rifiuto (permessi minimi: fine-grained in sola lettura, o classico senza scope)."""
    _, headers = gh.get_with_headers(f"/repos/{TRUSTED_REPO}")
    scopes = next((v for k, v in headers.items() if k.lower() == "x-oauth-scopes"), None)
    if scopes is not None and scopes.strip():
        raise GuardError("token GitHub con scope di scrittura o eccessivi: richiesto un token in sola lettura")


# --------------------------------------------------------------------------------------------- provenienza
def verify_pr(gh: GitHub, ctx: Mapping[str, Any]) -> dict[str, Any]:
    """Legge la PR e ne verifica repository, stato, base, head e provenienza. Ritorna il riepilogo verificato."""
    pr = gh.get(f"/repos/{TRUSTED_REPO}/pulls/{ctx['pr']}")
    if not isinstance(pr, dict):
        raise GuardError("dati PR non strutturati")
    if pr.get("number") != ctx["pr"]:
        raise GuardError("la PR restituita non e' quella richiesta")
    if pr.get("state") != "open":
        raise GuardError(f"PR non aperta (state={pr.get('state')!r})")
    base = pr.get("base") if isinstance(pr.get("base"), dict) else {}
    head = pr.get("head") if isinstance(pr.get("head"), dict) else {}
    base_repo = (base.get("repo") or {}).get("full_name") if isinstance(base.get("repo"), dict) else None
    head_repo = (head.get("repo") or {}).get("full_name") if isinstance(head.get("repo"), dict) else None
    if not isinstance(base_repo, str) or base_repo.lower() != TRUSTED_REPO.lower():
        raise GuardError("repository base della PR diverso da quello atteso")
    if base.get("ref") != ctx["base_ref"]:
        raise GuardError(f"branch base della PR diverso da {ctx['base_ref']!r}")
    if not isinstance(head_repo, str) or head_repo.lower() != TRUSTED_REPO.lower():
        raise GuardError("head della PR fuori dal repository (fork o repository cancellato): provenienza non verificata")
    head_sha, base_sha = head.get("sha"), base.get("sha")
    if not isinstance(head_sha, str) or not _SHA_RE.fullmatch(head_sha):
        raise GuardError("head SHA della PR illeggibile")
    if not isinstance(base_sha, str) or not _SHA_RE.fullmatch(base_sha):
        raise GuardError("base SHA della PR illeggibile")
    if head_sha != ctx["head_sha"]:
        raise GuardError(f"head della PR cambiato: atteso {ctx['head_sha']}, attuale {head_sha}")
    cmp = gh.get(f"/repos/{TRUSTED_REPO}/compare/{base_sha}...{ctx['base_ref']}")
    if not isinstance(cmp, dict) or cmp.get("status") not in ("ahead", "identical"):
        raise GuardError("base SHA della PR non e' nella storia del branch base")
    labels = label_names(pr)
    return {"pr": ctx["pr"], "head_sha": head_sha, "base_sha": base_sha, "base_ref": ctx["base_ref"],
            "draft": pr.get("draft") is True, "changed_files": pr.get("changed_files"), "url": pr.get("url"),
            "labels": labels}


def label_names(pr: Mapping[str, Any]) -> list[str]:
    labels = pr.get("labels")
    if not isinstance(labels, list) or not all(isinstance(lb, dict) and isinstance(lb.get("name"), str)
                                               for lb in labels):
        raise GuardError("label della PR illeggibili")
    return sorted(lb["name"] for lb in labels)


def verify_head_unchanged(gh: GitHub, ctx: Mapping[str, Any], *, owner_labels: bool = False) -> None:
    """Rilettura finale: head, stato, repository e branch base devono essere quelli verificati all'inizio. Una PR
    ridiretta su un altro branch durante l'attesa non resta certificata (rilievo Codex P2 su #563). Per la readiness
    (`owner_labels`) anche la label `manual-review-required` si rilegge qui: aggiunta durante l'attesa => FAIL
    (rilievo Codex P1 su 80e0867)."""
    pr = gh.get(f"/repos/{TRUSTED_REPO}/pulls/{ctx['pr']}")
    if not isinstance(pr, dict) or pr.get("state") != "open":
        raise GuardError("head o stato della PR cambiati durante la valutazione")
    head = pr.get("head") if isinstance(pr.get("head"), dict) else {}
    base = pr.get("base") if isinstance(pr.get("base"), dict) else {}
    base_repo = (base.get("repo") or {}).get("full_name") if isinstance(base.get("repo"), dict) else None
    if head.get("sha") != ctx["head_sha"]:
        raise GuardError("head o stato della PR cambiati durante la valutazione")
    if base.get("ref") != ctx["base_ref"] or not isinstance(base_repo, str) \
            or base_repo.lower() != TRUSTED_REPO.lower():
        raise GuardError("repository o branch base della PR cambiati durante la valutazione")
    if owner_labels and MANUAL_LABEL in label_names(pr):
        raise GuardError(f"label {MANUAL_LABEL} presente a fine valutazione: merge riservato all'owner")


# --------------------------------------------------------------------------------------------- overlap guard
def pr_files(gh: GitHub, number: int, expected: Any) -> set:
    files = gh.get_all(f"/repos/{TRUSTED_REPO}/pulls/{number}/files?per_page=100")
    names = set()
    for f in files:
        name = f.get("filename") if isinstance(f, dict) else None
        if not isinstance(name, str) or not name:
            raise GuardError(f"elenco file della PR #{number} illeggibile")
        names.add(name)
        # Rename: GitHub mette il percorso di origine in `previous_filename`. Contano entrambi, cosi' un file
        # protetto rinominato verso un percorso libero resta visibile (rilievo Codex P1 su faa61d1).
        if f.get("status") == "renamed" or "previous_filename" in f:
            source = f.get("previous_filename")
            if not isinstance(source, str) or not source:
                raise GuardError(f"rename senza percorso di origine leggibile nella PR #{number}")
            names.add(source)
    if not isinstance(expected, int) or isinstance(expected, bool) or expected < 0:
        raise GuardError(f"numero di file della PR #{number} illeggibile")
    if expected >= GITHUB_FILES_CAP or len(files) != expected:
        raise GuardError(f"elenco file della PR #{number} incompleto ({len(files)} su {expected})")
    return names


def run_overlap(gh: GitHub, ctx: Mapping[str, Any], verified: Mapping[str, Any]) -> list[str]:
    """Regola «una sola PR aperta» e sovrapposizione su file critici. Ritorna i motivi di FAIL (vuota = PASS).
    A differenza dello step inline del workflow, una PR i cui file non si leggono e' FAIL, mai saltata."""
    reasons: list[str] = []
    open_prs = gh.get_all(f"/repos/{TRUSTED_REPO}/pulls?state=open&per_page=100")
    numbers = []
    for p in open_prs:
        n = p.get("number") if isinstance(p, dict) else None
        if not isinstance(n, int) or isinstance(n, bool) or n < 1:
            raise GuardError("elenco delle PR aperte illeggibile")
        numbers.append(n)
    numbers.sort()
    if ctx["pr"] not in numbers:
        raise GuardError("la PR non risulta fra le PR aperte")
    extra = numbers[1:]
    if extra:
        reasons.append(f"una sola PR aperta alla volta (CLAUDE.md): aperte {', '.join('#' + str(n) for n in numbers)}, "
                       f"di troppo {', '.join('#' + str(n) for n in extra)}")
    current = pr_files(gh, ctx["pr"], verified.get("changed_files"))
    for p in open_prs:
        if p["number"] == ctx["pr"]:
            continue
        detail = gh.get(f"/repos/{TRUSTED_REPO}/pulls/{p['number']}")
        other = pr_files(gh, p["number"], detail.get("changed_files") if isinstance(detail, dict) else None)
        critical = sorted(current & other & CRITICAL_FILES)
        if critical:
            reasons.append(f"sovrapposizione critica con #{p['number']}: {', '.join(critical)}")
    return reasons


# --------------------------------------------------------------------------------------------- merge readiness
def default_run_readiness(repo_root: Path, pr: int, wait: int, token: str, output: Path) -> int:
    """Lancia la readiness esistente (`scripts/pr_merge_readiness.py`, la stessa del workflow GitHub Actions) dal
    checkout di main, con un ambiente minimo: solo il token per `gh`, nessun file di configurazione persistente."""
    with tempfile.TemporaryDirectory(prefix="pf-gh-") as gh_config:
        child_env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": gh_config, "GH_CONFIG_DIR": gh_config, "GH_TOKEN": token, "GH_HOST": "github.com",
            "GH_PROMPT_DISABLED": "1", "GH_NO_UPDATE_NOTIFIER": "1", "NO_COLOR": "1", "LANG": "C.UTF-8",
        }
        for key in ("HTTPS_PROXY", "https_proxy", "SSL_CERT_FILE"):
            if key in os.environ:
                child_env[key] = os.environ[key]
        cmd = [sys.executable, "-I", str(repo_root / "scripts" / "pr_merge_readiness.py"),
               "--repo", TRUSTED_REPO, "--pr", str(pr), "--ignore-safe-autofix",
               "--wait-pending-seconds", str(wait), "--poll-seconds", str(READINESS_POLL_SECONDS),
               "--output", str(output)]
        proc = subprocess.run(cmd, cwd=str(repo_root), env=child_env, stdout=subprocess.PIPE,  # nosec B603
                              stderr=subprocess.STDOUT, timeout=wait + READINESS_MARGIN_SECONDS, check=False)
        sys.stdout.write(redact(proc.stdout.decode("utf-8", "replace"), token)[-20000:])
        return proc.returncode


def evaluate_readiness(decision: Any, rc: int, ctx: Mapping[str, Any]) -> list[str]:
    """PASS solo se la readiness esistente dice can_merge=true sull'head atteso E il gate Sol/Grok e' PASS.
    `ai_review_gate` e' informativo nel workflow Actions; qui no: review assente, non conclusiva o UNKNOWN => FAIL."""
    if not isinstance(decision, dict):
        return ["decisione della readiness assente o illeggibile"]
    reasons: list[str] = []
    if rc != 0:
        reasons.append(f"readiness uscita con codice {rc}")
    if decision.get("headRefOid") != ctx["head_sha"]:
        reasons.append("la readiness ha valutato un head diverso da quello atteso")
    if decision.get("can_merge") is not True:
        reasons.append("can_merge non e' true")
    if decision.get("is_open") is not True or decision.get("is_draft") is not False:
        reasons.append("PR non aperta o in bozza")
    for key in ("blockers", "pending"):
        value = decision.get(key)
        if not isinstance(value, list) or value:
            reasons.append(f"{key} non vuoti o illeggibili")
    gate = decision.get("ai_review_gate_status")
    if gate != "PASS":
        reasons.append(f"gate review Sol/Grok = {gate!r}, non PASS")
    for r in decision.get("reasons") or []:
        if isinstance(r, str):
            reasons.append(f"readiness: {r}")
    return reasons


def load_autonomy_policy(repo_root: Path) -> Any:
    """`scripts/pr_autonomy_policy.py` del checkout di main (modulo puro, solo libreria standard), caricato per
    percorso: nessuna directory aggiunta a sys.path, quindi nessun modulo che possa ombreggiare la libreria standard."""
    path = repo_root / "scripts" / "pr_autonomy_policy.py"
    spec = importlib.util.spec_from_file_location("pf_trusted_autonomy_policy", path)
    if spec is None or spec.loader is None:
        raise GuardError("classificatore dei file a merge owner non caricabile")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not callable(getattr(module, "is_owner_manual_path", None)):
        raise GuardError("classificatore dei file a merge owner non disponibile")
    return module


def owner_decision_reasons(gh: GitHub, ctx: Mapping[str, Any], verified: Mapping[str, Any],
                           repo_root: Path) -> list[str]:
    """Una PR riservata all'owner non e' mai PASS (review indipendente P1 su #563): label `manual-review-required` o
    file a merge owner nel diff COMPLETO (tutte le pagine, elenco incompleto => FAIL)."""
    reasons: list[str] = []
    if MANUAL_LABEL in verified.get("labels", []):
        reasons.append(f"label {MANUAL_LABEL}: merge riservato all'owner")
    policy = load_autonomy_policy(repo_root)
    owner_files = sorted(p for p in pr_files(gh, ctx["pr"], verified.get("changed_files"))
                         if policy.is_owner_manual_path(p))
    if owner_files:
        reasons.append(f"file a merge owner nel diff: {', '.join(owner_files)}")
    return reasons


def run_readiness(ctx: Mapping[str, Any], wait: int, token: str, repo_root: Path,
                  runner: Callable[..., int]) -> list[str]:
    with tempfile.TemporaryDirectory(prefix="pf-readiness-") as tmp:
        output = Path(tmp) / "decision.json"
        rc = runner(repo_root, ctx["pr"], wait, token, output)
        try:
            decision = json.loads(output.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            decision = None
    return evaluate_readiness(decision, rc, ctx)


# --------------------------------------------------------------------------------------------- entry point
def redact(text: str, token: Optional[str]) -> str:
    return text.replace(token, "***") if token else text


def result_path(env: Mapping[str, str], guard: str) -> Path:
    return Path(env.get("PF_RESULT_DIR") or ".trusted-guard") / f"{guard}.json"


def discard_previous_result(env: Mapping[str, str], guard: str) -> None:
    """Un risultato di un giro precedente non deve mai essere pubblicato al posto di questo (job ucciso o scrittura
    fallita): si cancella PRIMA della valutazione. Impossibile cancellarlo => GuardError."""
    path = result_path(env, guard)
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except OSError as exc:
        raise GuardError(f"risultato precedente non cancellabile: {type(exc).__name__}") from None


def write_result(env: Mapping[str, str], guard: str, result: Mapping[str, Any]) -> bool:
    """True solo se il risultato e' scritto: senza il file chi pubblica lo status non ha l'head valutato, quindi
    una scrittura fallita non e' mai un'uscita PASS (rilievo Codex P2 su #563)."""
    path = result_path(env, guard)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return True
    except OSError as exc:
        print(f"[{guard}] risultato non scritto: {type(exc).__name__}", file=sys.stderr)
        return False


def main(guard: str, argv: list[str], env: Mapping[str, str], *, transport: Transport = urllib_transport,
         readiness_runner: Callable[..., int] = default_run_readiness,
         repo_root: Optional[Path] = None) -> int:
    token: Optional[str] = None
    result: dict[str, Any] = {"guard": guard, "repo": TRUSTED_REPO, "verdict": "FAIL", "reasons": []}
    try:
        if guard not in ("overlap", "readiness"):
            raise GuardError("controllo sconosciuto")
        if argv:
            raise GuardError("nessun argomento ammesso: i dati arrivano solo dall'ambiente controllato")
        discard_previous_result(env, guard)
        ctx = parse_environment(env)
        result.update(pr=ctx["pr"], head_sha=ctx["head_sha"], base_ref=ctx["base_ref"])
        wait = readiness_wait_seconds(env) if guard == "readiness" else 0
        token = load_token(env)
        gh = GitHub(token, transport)
        check_token_privileges(gh)
        verified = verify_pr(gh, ctx)
        result["base_sha"] = verified["base_sha"]
        if guard == "overlap":
            reasons = run_overlap(gh, ctx, verified)
        else:
            root = repo_root or Path(__file__).resolve().parents[2]
            reasons = owner_decision_reasons(gh, ctx, verified, root)
            reasons += run_readiness(ctx, wait, token, root, readiness_runner)
        verify_head_unchanged(gh, ctx, owner_labels=(guard == "readiness"))
        result["reasons"] = [redact(r, token) for r in reasons]
        result["verdict"] = "PASS" if not reasons else "FAIL"
    except GuardError as exc:
        result["reasons"] = [redact(str(exc), token)]
    except Exception as exc:  # qualunque errore inatteso e' FAIL, mai PASS
        result["reasons"] = [f"errore inatteso: {type(exc).__name__}"]
    if not write_result(env, guard, result) and result["verdict"] == "PASS":
        result["verdict"] = "FAIL"
        result["reasons"] = ["risultato non scritto: verdetto non pubblicabile"]
    print(f"[{guard}] PR #{result.get('pr', '?')} head {result.get('head_sha', '?')}: {result['verdict']}")
    for r in result["reasons"]:
        print(f"  - {r}")
    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("uso interno: trusted_pr_guard.py overlap|readiness (lanciato dai wrapper scripts/ci/*.sh)",
              file=sys.stderr)
        raise SystemExit(1)
    raise SystemExit(main(sys.argv[1], sys.argv[2:], os.environ))
