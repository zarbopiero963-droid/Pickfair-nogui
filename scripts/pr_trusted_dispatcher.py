#!/usr/bin/env python3
"""Dispatcher trusted della lane Buildkite (issue #562, PF-TRUSTED-DISPATCHER-IMPLEMENTATION-01).

Contratto di Grok (#562 c.6438092317, §5) con l'intake sostituito dal **polling autenticato** dell'API GitHub
(decisione owner PF-562 GITHUB POLLING ARCHITECTURE, opzione B): nessun endpoint pubblico, nessun web server.

1. **Intake.** Ogni `poll_interval_seconds` (consigliato 60) il dispatcher legge con un token di SOLA LETTURA a
   durata limitata (prodotto da `github.token_command`, l'helper della GitHub App) l'elenco COMPLETO delle PR aperte
   del repository `zarbopiero963-droid/Pickfair-nogui`, con paginazione verificata pagina per pagina. Una risposta
   incompleta, malformata, un errore API, il rate limit o credenziali assenti/scadute annullano il giro: nessuna
   transizione, nessuno status. Solo GET su due elenchi (`pulls`, `issues/comments`) e sul permesso attuale dei
   login in allowlist dei rerun (`collaborators/{login}/permission`): mai il codice delle PR.
2. **Transizioni.** Per ogni PR valutabile (base `main`, head nello stesso repository, non in bozza) si confronta
   lo stato osservato con quello salvato: nuova PR (`opened`), nuovo head (`synchronize`, anche il ritorno a uno SHA
   precedente), riapertura (`reopened`, anche sullo stesso SHA), uscita dalla bozza (`ready_for_review`). Ogni
   transizione ha un identificatore stabile `p<PR>-t<seq>-<sha12>` (contatore persistente per PR) che diventa
   `PF_DELIVERY_ID`. Un head nuovo annulla e sostituisce le build del vecchio; una PR sparita da DUE elenchi completi
   consecutivi e' chiusa e le sue build si annullano.
3. **Rerun.** Commento esatto `/pf-trusted-rerun <sha40>` su una PR aperta, di un login in `rerun.allowed_logins`,
   mai modificato, sullo SHA head corrente: nuova transizione `p<PR>-r<comment_id>-<sha12>`, `PF_EVENT=manual`.
   Ogni comando (accettato o rifiutato, con motivo) resta nell'archivio `reruns`; il commento e' il record pubblico.
   Un rerun rivaluta soltanto: stesse pipeline, stesso esito di `decide_outcome`, nessun check aggirato.
4. **Build.** API Buildkite solo sulle pipeline `pr-overlap-guard` e `pr-merge-readiness`, `branch=main`,
   `commit=HEAD`, env ESATTAMENTE `PF_PR_NUMBER`, `PF_PR_HEAD_SHA`, `PF_PR_BASE_REF=main`, `PF_EVENT`,
   `PF_DELIVERY_ID`. Le pipeline trusted eseguono solo script di `main`.
5. **Esito e pubblicazione.** Come prima: `success` solo con build `passed` e risultato PASS del guard sullo stesso
   head; pubblicazione solo tramite il comando configurato (helper root `pf-publish-status`, credenziale separata).
6. **Segreti.** Token Buildkite da file 0600/0400; token GitHub solo in memoria, mai passato alle build, mai nei
   log; la chiave della GitHub App resta all'helper del token, fuori da questo processo.
7. **Fail closed** su ogni errore di API, rete, validazione, credenziali o configurazione.

Configurazione: un file JSON con TUTTE le chiavi obbligatorie e nessun default per i parametri infrastrutturali.
Chiave mancante, sconosciuta o fuori formato => `NOT CONFIGURED`. Dettagli in `docs/trusted_dispatcher.md`.

Solo libreria standard. Nessun ordine Betfair, nessun accesso al codice delle PR.
"""
from __future__ import annotations

import argparse
import errno
import fcntl
import http.client
import json
import os
import re
import signal
import sqlite3
import stat
import struct
import subprocess  # nosec B404 - solo i comandi configurati (token, pubblicazione), argv senza shell
import sys
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional

TRUSTED_REPO = "zarbopiero963-droid/Pickfair-nogui"
#: id numerico di TRUSTED_REPO: GitHub lo usa nei link `next` della paginazione (`/repositories/<id>/...`). Fisso:
#: un link con un altro id e' rifiutato (#562 F1, rollback T252 di #576)
TRUSTED_REPO_ID = 1189713160
BASE_REF = "main"
#: Unica fonte delle pipeline avviabili: la configurazione non puo' aggiungerne altre.
GUARDS: dict[str, dict[str, str]] = {
    "overlap": {"pipeline": "pr-overlap-guard", "context": "overlap-check"},
    "readiness": {"pipeline": "pr-merge-readiness", "context": "pr-merge-readiness"},
}
#: Suite completa sulla lane NON fidata (#562 PR-A): build sul commit ESATTO della PR, nessun token, nessun artifact
#: del guard. Si attiva solo con `full_suite.enabled` (chiave obbligatoria, nessun default).
SUITE = "full-suite"
SUITE_TARGET = {"pipeline": "pf-dedicated-ci", "context": "full-suite"}
#: tutte le pipeline che il dispatcher conosce; quelle avviate dipendono dalla configurazione (`Config.targets`)
PIPELINES: dict[str, dict[str, str]] = {**GUARDS, SUITE: SUITE_TARGET}
#: stati ammessi in una build `passed` per uno step NON atteso (scenari con `if:` non eseguiti: Buildkite li segna
#: `broken`): mai `failed`, `canceled`, `timed_out` o soft-fail
SUITE_EXTRA_STEP_STATES = ("passed", "broken", "skipped")
#: step che una build `full-suite` deve contenere, tutti `passed`, per valere `success`: le chiavi di
#: `scripts/pr_full_suite_pipeline.py` (un test le confronta). Una build che ne perde anche uno non e' mai un PASS.
FULL_SUITE_STEP_KEYS = frozenset({
    "w1-repo-guardrails-ext", "w1-repo-guardrails", "w1-quarantine", "w4-noise-free", "w4-ci-dynamic-detect",
    "w4-readiness-gate", "w4-rollback-gate", "w4-live-gate", "w4-incident-snapshot", "w3-observability-minimum",
    "w1-unit", "w1-core-guardrails", "w1-core-invariants", "w1-recovery-guardrails", "w3-smoke-core-fast",
    "w2-money-management", "w2-dutching", "w2-pnl-engine", "w2-order-manager", "w2-risk-middleware",
    "w2-rate-limiter", "w2-reconciliation-engine", "w2-session-manager", "w2-execution-guard",
    "w2-trading-engine", "w2-copy-engine", "w2-simulation-broker", "w2-runtime-controller",
    "w2-telegram-listener", "w2-pnl-invariants", "w2-trading-engine-hard", "w2-stateful-integrity",
    "w3-integration", "w3-e2e", "w3-smoke", "w3-failure", "w3-net", "w3-chaos-critical", "w3-chaos-runtime",
    "w3-chaos-stateful", "w3-core-chaos", "w3-observability-runtime", "w3-observability-tests",
    "w4-live-sim-parity", "w4-build-linux-tests", "w1-pr-check", "w2-backstop", "w4-merge-simulation",
    "w1-mutation",
    # M2 (#562 PR-B)
    "w5-lockfile-fresh", "w5-telegram-live-sim", "w5-build-linux-appimage",
})
GITHUB_API = "https://api.github.com"
BUILDKITE_API = "https://api.buildkite.com/v2"
BUILDKITE_WEB = "https://buildkite.com"
MAX_ARTIFACT_BYTES = 64 * 1024
MAX_API_BYTES = 4 * 1024 * 1024
MAX_PAGES = 50                          # 50 x 100 elementi: oltre, l'elenco non e' verificabile
MIN_RETENTION_HOURS = 24
DESCRIPTION_MAX = 140
TOKEN_REFRESH_MARGIN = 300              # un token che scade entro 5 minuti si rinnova prima di usarlo
CLOSE_AFTER_MISSES = 2                  # una PR e' chiusa solo se manca da due elenchi completi consecutivi
#: rerun piu' vecchio di cosi' (orario di creazione del COMMENTO su GitHub, non dell'archivio locale): rifiutato e
#: registrato. Seconda difesa dopo la verifica su Buildkite: limita cio' che un archivio vecchio puo' far rileggere.
RERUN_MAX_AGE_SECONDS = 6 * 3600
#: metadato Buildkite con l'identita' stabile dell'intento (= `PF_DELIVERY_ID`, il tid): sopravvive a crash, riavvii e
#: ripristino di uno snapshot dell'archivio, quindi e' la prova remota che l'intento ha gia' una build (#562 P0)
INTENT_META_KEY = "pf_intent"
#: metadato Buildkite con il numero della PR (#562 P0, Sol e Codex su #583): il `seq` delle transizioni sta
#: nell'archivio locale e uno snapshot lo riporta indietro; dalle build della PR si ricava il `seq` piu' alto gia' usato
PR_META_KEY = "pf_pr"
TRANSITION_EVENTS = frozenset({"opened", "reopened", "synchronize", "ready_for_review"})   # `Store.apply_poll`
_TRANSITION_TID_RE = re.compile(r"p([0-9]+)-t([0-9]+)-([0-9a-f]{12})", re.ASCII)
_PR_META_RE = re.compile(r"[1-9][0-9]{0,9}", re.ASCII)
INTENT_PAGE = 100                       # una pagina piena di build per lo stesso intento: assenza non verificabile
PR_PAGES_MAX = 10                       # pagine di build di una PR lette per trovarne l'ultima transizione
COMMENTS_OVERLAP_SECONDS = 120          # sovrapposizione fra giri per i commenti: i doppioni li toglie l'archivio

_PR_RE = re.compile(r"[1-9][0-9]{0,6}", re.ASCII)
# branch di head della PR, usato solo dalla full-suite (campo `branch` della build): fuori formato => ref vuoto, e la
# full-suite di quella PR finisce `error` senza fermare i guard trusted
_REF_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,199}", re.ASCII)
_SHA_RE = re.compile(r"[0-9a-f]{40}", re.ASCII)
MAX_COMMENT_ID = 10 ** 15
_TID_RE = re.compile(r"[A-Za-z0-9-]{8,64}", re.ASCII)         # stesso formato del gate pre-command (PF_DELIVERY_ID)
_TIMESTAMP_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", re.ASCII)
_SLUG_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,99}", re.ASCII)
_LOGIN_RE = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?", re.ASCII)
_HOST_RE = re.compile(r"[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+", re.ASCII)
_ARTIFACT_RE = re.compile(r"[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*", re.ASCII)
_TOKEN_RE = re.compile(r"[!-~]{20,8192}", re.ASCII)
_RERUN_RE = re.compile(r"/pf-trusted-rerun ([0-9a-f]{40})", re.ASCII)
_LINK_NEXT_RE = re.compile(r'<([^>]+)>;\s*rel="next"')
_GITHUB_PATHS = (f"/repos/{TRUSTED_REPO}/pulls", f"/repos/{TRUSTED_REPO}/issues/comments")
# permesso attuale di un login sul repository fidato (solo per i login in allowlist dei rerun)
# check run e status di UN commit (solo per l'attesa della readiness, #562 opzione A): sola lettura, SHA di 40 hex
_COMMIT_CHECKS_PATH_RE = re.compile(rf"/repos/{re.escape(TRUSTED_REPO)}/commits/[0-9a-f]{{40}}/(check-runs|statuses)",
                                    re.ASCII)
_PERMISSION_PATH_RE = re.compile(rf"/repos/{re.escape(TRUSTED_REPO)}/collaborators/({_LOGIN_RE.pattern})/permission",
                                 re.ASCII)

# Stati Buildkite di una build (API REST v2). Sconosciuto => error, mai attesa infinita.
BUILD_RUNNING = frozenset({"creating", "scheduled", "running", "failing", "canceling", "waiting", "waiting_failed"})
BUILD_TERMINAL = frozenset({"passed", "failed", "canceled", "skipped", "not_run", "blocked"})

# Stati del job nell'archivio.
# `canceling`: esito gia' pubblicato, resta da confermare l'annullamento di una o piu' build (mai orfane).
ACTIVE_STATES = ("queued", "creating", "dispatched", "publishing", "canceling")
FINAL_STATES = ("done", "superseded", "publish_failed")
# un esito non consegnato entro la finestra (`publish_failed`) si riconsegna a questo intervallo finche' resta l'esito
# piu' recente della PR per quel guard, sull'head corrente di una PR aperta (Sol su #569): mai un vecchio `success`
# lasciato valido perche' il publisher era fermo
REDELIVERY_SECONDS = 300

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_NOT_CONFIGURED = 3


class ConfigError(Exception):
    """Configurazione assente, incompleta o fuori formato: il servizio resta NOT CONFIGURED."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


class ApiError(Exception):
    """Risposta HTTP definita ma non quella attesa (4xx/5xx, JSON malformato, elenco non verificabile)."""

    def __init__(self, message: str, status: Optional[int] = None) -> None:
        super().__init__(message)
        self.status = status


class TransportError(Exception):
    """Esito della richiesta ignoto (rete, timeout): per una creazione di build e' AMBIGUO."""


class CredentialError(Exception):
    """Token GitHub di lettura assente, illeggibile, rifiutato o scaduto: il giro di polling non avviene."""


class RateLimited(Exception):
    """Rate limit GitHub: nessun giro fino a `until` (epoch)."""

    def __init__(self, until: float) -> None:
        super().__init__(f"rate limit GitHub fino a {int(until)}")
        self.until = until


# --------------------------------------------------------------------------------------------- configurazione
CONFIG_SCHEMA_KEYS = {
    "github": {"token_command", "token_command_timeout_seconds"},
    "rerun": {"allowed_logins"},
    "buildkite": {"organization", "token_file", "result_artifacts", "artifact_download_hosts"},
    "publisher": {"command", "timeout_seconds"},
    "state_db": None,
    "retention_hours": None,
    "poll_interval_seconds": None,
    "build_timeout_seconds": set(GUARDS),
    "http_timeout_seconds": None,
    "readiness_wait": {"enabled", "max_seconds", "settle_polls", "min_delay_seconds"},
    "full_suite": {"enabled", "build_timeout_seconds"},
}


@dataclass(frozen=True)
class Config:
    github_token_command: tuple
    github_token_command_timeout_seconds: int
    rerun_allowed_logins: frozenset
    organization: str
    buildkite_token_file: str
    result_artifacts: dict
    artifact_download_hosts: tuple
    publisher_command: tuple
    publisher_timeout_seconds: int
    state_db: str
    retention_hours: int
    poll_interval_seconds: int
    build_timeout_seconds: dict             # per ogni target attivo, full-suite compresa se abilitata
    http_timeout_seconds: int
    # readiness non bloccante (#562 opzione A): la build parte solo quando gli altri check dell'head sono chiusi
    readiness_wait_enabled: bool = False
    readiness_wait_max_seconds: int = 7200
    readiness_wait_settle_polls: int = 2
    readiness_wait_min_delay_seconds: int = 120
    full_suite_enabled: bool = False

    @property
    def targets(self) -> tuple:
        """Pipeline avviate a ogni transizione: i due guard trusted e, se abilitata, la full-suite."""
        return (*GUARDS, SUITE) if self.full_suite_enabled else tuple(GUARDS)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _abs_path(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("/") and "\x00" not in value and ".." not in value.split("/")


def _argv(value: Any) -> bool:
    return (isinstance(value, list) and bool(value) and all(isinstance(c, str) and c and "\x00" not in c for c in value)
            and _abs_path(value[0]))


def validate_config(raw: Any) -> Config:
    """Tutte le chiavi sono obbligatorie e nessuna ha un default: un parametro infrastrutturale mancante non viene
    mai indovinato. Chiavi sconosciute => errore (un refuso non deve diventare un default silenzioso)."""
    problems: list[str] = []
    if not isinstance(raw, dict):
        raise ConfigError(["la configurazione deve essere un oggetto JSON"])
    for key, sub in CONFIG_SCHEMA_KEYS.items():
        if key not in raw:
            problems.append(f"{key}: MISSING")
        elif sub is not None:
            if not isinstance(raw[key], dict):
                problems.append(f"{key}: deve essere un oggetto")
                continue
            for k in sorted(sub - set(raw[key])):
                problems.append(f"{key}.{k}: MISSING")
            for k in sorted(set(raw[key]) - sub):
                problems.append(f"{key}.{k}: chiave sconosciuta")
    for k in sorted(set(raw) - set(CONFIG_SCHEMA_KEYS)):
        problems.append(f"{k}: chiave sconosciuta")
    if problems:
        raise ConfigError(problems)

    gh, bk = raw["github"], raw["buildkite"]
    if not _argv(gh["token_command"]):
        problems.append("github.token_command: argv non vuoto con eseguibile a percorso assoluto")
    token_timeout = gh["token_command_timeout_seconds"]
    if not _is_int(token_timeout) or not 5 <= token_timeout <= 120:
        problems.append("github.token_command_timeout_seconds: intero 5..120")
    logins = raw["rerun"]["allowed_logins"]
    # elenco vuoto ammesso e ESPLICITO: rerun disabilitati
    if not isinstance(logins, list) or not all(isinstance(x, str) and _LOGIN_RE.fullmatch(x) for x in logins):
        problems.append("rerun.allowed_logins: elenco di login GitHub (vuoto = rerun disabilitati)")
    if not _abs_path(raw["state_db"]):
        problems.append("state_db: percorso assoluto senza '..'")
    if not _abs_path(bk["token_file"]):
        problems.append("buildkite.token_file: percorso assoluto senza '..'")
    org = bk["organization"]
    if not isinstance(org, str) or not _SLUG_RE.fullmatch(org):
        problems.append("buildkite.organization: slug Buildkite [a-z0-9-]")
    artifacts = bk["result_artifacts"]
    if not isinstance(artifacts, dict) or set(artifacts) != set(GUARDS):
        problems.append(f"buildkite.result_artifacts: oggetto con esattamente {sorted(GUARDS)}")
    else:
        for guard, value in artifacts.items():
            # il guard scrive `<guard>.json` (scripts/ci/trusted_pr_guard.py): un altro nome non e' il suo risultato
            if (not isinstance(value, str) or not _ARTIFACT_RE.fullmatch(value) or ".." in value.split("/")
                    or value.rsplit("/", 1)[-1] != f"{guard}.json"):
                problems.append(f"buildkite.result_artifacts.{guard}: percorso relativo che finisce con {guard}.json")
    hosts = bk["artifact_download_hosts"]
    if (not isinstance(hosts, list) or not hosts
            or not all(isinstance(h, str) and _HOST_RE.fullmatch(h) for h in hosts)):
        problems.append("buildkite.artifact_download_hosts: elenco non vuoto di host esatti (minuscoli)")
    if not _argv(raw["publisher"]["command"]):
        problems.append("publisher.command: argv non vuoto con eseguibile a percorso assoluto")
    pub_timeout = raw["publisher"]["timeout_seconds"]
    if not _is_int(pub_timeout) or not 5 <= pub_timeout <= 300:
        problems.append("publisher.timeout_seconds: intero 5..300")
    retention = raw["retention_hours"]
    if not _is_int(retention) or not MIN_RETENTION_HOURS <= retention <= 24 * 90:
        problems.append(f"retention_hours: intero {MIN_RETENTION_HOURS}..{24 * 90}")
    poll = raw["poll_interval_seconds"]
    if not _is_int(poll) or not 10 <= poll <= 600:
        problems.append("poll_interval_seconds: intero 10..600 (consigliato 60)")
    timeouts = raw["build_timeout_seconds"]
    for guard in GUARDS:
        value = timeouts[guard]
        if not _is_int(value) or not 60 <= value <= 6 * 3600:
            problems.append(f"build_timeout_seconds.{guard}: intero 60..21600")
    http_timeout = raw["http_timeout_seconds"]
    if not _is_int(http_timeout) or not 5 <= http_timeout <= 120:
        problems.append("http_timeout_seconds: intero 5..120")
    wait = raw["readiness_wait"]
    if not isinstance(wait["enabled"], bool):
        problems.append("readiness_wait.enabled: booleano true/false")
    if not _is_int(wait["max_seconds"]) or not 600 <= wait["max_seconds"] <= 6 * 3600:
        problems.append("readiness_wait.max_seconds: intero 600..21600")
    if not _is_int(wait["settle_polls"]) or not 1 <= wait["settle_polls"] <= 10:
        problems.append("readiness_wait.settle_polls: intero 1..10")
    if not _is_int(wait["min_delay_seconds"]) or not 0 <= wait["min_delay_seconds"] <= 3600:
        problems.append("readiness_wait.min_delay_seconds: intero 0..3600")
    suite = raw["full_suite"]
    # booleano ESPLICITO: un valore ambiguo (stringa, 0/1) non accende ne' spegne la suite in silenzio
    if not isinstance(suite["enabled"], bool):
        problems.append("full_suite.enabled: booleano true/false")
    suite_timeout = suite["build_timeout_seconds"]
    if not _is_int(suite_timeout) or not 600 <= suite_timeout <= 6 * 3600:
        problems.append("full_suite.build_timeout_seconds: intero 600..21600")
    # la readiness attende anche lo status `full-suite`: un'attesa piu' corta della suite finirebbe in `error`
    if not problems and wait["enabled"] and suite["enabled"] and wait["max_seconds"] < suite_timeout:
        problems.append("readiness_wait.max_seconds: almeno full_suite.build_timeout_seconds con entrambe attive")
    if not problems and suite["enabled"] and not wait["enabled"]:
        # senza attesa la readiness partirebbe insieme alla suite e non potrebbe mai avere la suite conclusa (#562 P0)
        problems.append("readiness_wait.enabled: obbligatoria con full_suite.enabled")
    if problems:
        raise ConfigError(problems)
    timeouts = dict(timeouts)
    # sempre, anche con la suite spenta: un job della suite gia' nell'archivio (rollback `enabled: false`) arriva
    # comunque al proprio esito o alla scadenza (Codex su #584). `targets` decide se se ne creano di nuovi
    timeouts[SUITE] = suite_timeout
    return Config(
        github_token_command=tuple(gh["token_command"]), github_token_command_timeout_seconds=token_timeout,
        rerun_allowed_logins=frozenset(logins), organization=org, buildkite_token_file=bk["token_file"],
        result_artifacts=dict(artifacts), artifact_download_hosts=tuple(hosts),
        publisher_command=tuple(raw["publisher"]["command"]), publisher_timeout_seconds=pub_timeout,
        state_db=raw["state_db"], retention_hours=retention, poll_interval_seconds=poll,
        build_timeout_seconds=timeouts, http_timeout_seconds=http_timeout,
        readiness_wait_enabled=wait["enabled"], readiness_wait_max_seconds=wait["max_seconds"],
        readiness_wait_settle_polls=wait["settle_polls"], readiness_wait_min_delay_seconds=wait["min_delay_seconds"],
        full_suite_enabled=suite["enabled"])


def load_config(path: Optional[str]) -> Config:
    if not path:
        raise ConfigError(["--config: MISSING (nessun file di configurazione predefinito)"])
    try:
        with open(path, encoding="utf-8") as fh:
            raw = json.load(fh)
    except FileNotFoundError:
        raise ConfigError([f"file di configurazione assente: {path}"]) from None
    except (OSError, ValueError) as exc:
        raise ConfigError([f"file di configurazione illeggibile: {type(exc).__name__}"]) from None
    return validate_config(raw)


_ACL_XATTR = "system.posix_acl_access"
# voci di una ACL POSIX (formato xattr di Linux, versione 2): proprietario, utente nominativo, gruppo proprietario,
# gruppo nominativo, mask, altri
_ACL_USER_OBJ, _ACL_USER, _ACL_GROUP_OBJ, _ACL_GROUP, _ACL_MASK, _ACL_OTHER = 0x01, 0x02, 0x04, 0x08, 0x10, 0x20


def _credential_acl_ok(acl: bytes, euid: int, egid: int) -> bool:
    """ACL di una credenziale: utenti nominativi solo root o il servizio, gruppi nominativi solo root o il gruppo del
    servizio, nessun permesso ad «altri». ACL malformata, di versione o con voci sconosciute => False."""
    if len(acl) < 4 or (len(acl) - 4) % 8 or struct.unpack_from("<I", acl)[0] != 2:
        return False
    for off in range(4, len(acl), 8):
        tag, perm, ident = struct.unpack_from("<HHI", acl, off)
        if tag not in (_ACL_USER_OBJ, _ACL_USER, _ACL_GROUP_OBJ, _ACL_GROUP, _ACL_MASK, _ACL_OTHER):
            return False
        if (tag == _ACL_USER and ident not in (0, euid)) or (tag == _ACL_GROUP and ident not in (0, egid)):
            return False
        if tag == _ACL_OTHER and perm:
            return False
    return True


def _systemd_credential(path: str, fd: int, st: os.stat_result) -> bool:
    """Credenziale di `LoadCredential` (systemd >= 255 la espone 0440, #562): ammessa SOLO se il file sta proprio
    nella directory `$CREDENTIALS_DIRECTORY` del servizio, la directory non e' scrivibile da gruppo o altri, il
    proprietario del file e della directory e' root o l'utente del processo, il gruppo e' root o quello del processo,
    i permessi sono esattamente 0440 e l'eventuale ACL non concede la lettura a nessun altro. Su systemd 255 il file e'
    root:root e il servizio lo legge tramite una voce ACL (D1, Grok su #562). Qualsiasi altra combinazione resta
    rifiutata."""
    creds = os.environ.get("CREDENTIALS_DIRECTORY", "")
    if not creds or not os.path.isabs(creds) or stat.S_IMODE(st.st_mode) != 0o440:
        return False
    euid, egid = os.geteuid(), os.getegid()
    try:
        cdir = os.path.realpath(creds)
        dst = os.stat(cdir)
    except OSError:
        return False
    # anche la directory deve essere di root o dell'utente del servizio: il suo proprietario potrebbe sostituirne i file
    # (review su #571). La sua mask ACL e' nei bit di gruppo, quindi `0o022` limita anche le voci nominative.
    if not (os.path.realpath(os.path.dirname(os.path.abspath(path))) == cdir and stat.S_ISDIR(dst.st_mode)
            and dst.st_uid in (0, euid) and not dst.st_mode & 0o022
            and st.st_uid in (0, euid) and st.st_gid in (0, egid)):
        return False
    # i bit di gruppo di un file con ACL sono la mask: chi legge davvero lo dice la ACL, letta sul descrittore aperto
    try:
        acl = os.getxattr(fd, _ACL_XATTR)
    except OSError as exc:
        return exc.errno in (errno.ENODATA, errno.ENOTSUP)      # nessuna ACL: valgono proprietario e gruppo
    except AttributeError:                                      # piattaforma senza xattr: niente systemd
        return False
    return _credential_acl_ok(acl, euid, egid)


def read_secret_file(path: str, pattern: re.Pattern, label: str) -> str:
    """File regolare, non link simbolico, 0600/0400 (nessun permesso a gruppo o altri), oppure credenziale di
    systemd 0440 nella directory del servizio (`_systemd_credential`); contenuto nel formato atteso.
    Il valore non compare mai nei messaggi d'errore."""
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise ConfigError([f"{label}: file non apribile ({type(exc).__name__})"]) from None
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise ConfigError([f"{label}: non e' un file regolare"])
        if st.st_mode & 0o077 and not _systemd_credential(path, fd, st):
            raise ConfigError([f"{label}: file accessibile a gruppo o altri (servono 0600/0400, oppure 0440 nella "
                               "directory $CREDENTIALS_DIRECTORY di systemd)"])
        if st.st_size > 16 * 1024:
            raise ConfigError([f"{label}: file troppo grande"])
        data = os.read(fd, 16 * 1024 + 1)
    finally:
        os.close(fd)
    try:
        value = data.decode("ascii")
    except UnicodeDecodeError:
        raise ConfigError([f"{label}: contenuto non ASCII"]) from None
    if value.endswith("\n"):
        value = value[:-1]
    if not pattern.fullmatch(value):
        raise ConfigError([f"{label}: contenuto fuori formato"])
    return value


def iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(value: Any) -> float:
    if not isinstance(value, str) or not _TIMESTAMP_RE.fullmatch(value):
        raise ValueError("timestamp fuori formato")
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()


# --------------------------------------------------------------------------------------------- credenziali GitHub
TokenRunner = Callable[[tuple, int], "tuple[int, bytes]"]


def default_token_runner(argv: tuple, timeout: int) -> "tuple[int, bytes]":
    proc = subprocess.run(list(argv), env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C.UTF-8"},  # nosec B603
                          stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                          timeout=timeout, check=False)
    return proc.returncode, proc.stdout[:64 * 1024]


class TokenProvider:
    """Token di SOLA LETTURA a durata limitata, prodotto dal comando configurato (l'helper della GitHub App, che
    tiene la chiave fuori da questo processo). Uscita attesa: un oggetto JSON con le chiavi `token`
    (stringa) ed `expires_at` (ISO UTC `...Z`).
    Assente, illeggibile, fuori formato o in scadenza => CredentialError, mai un token di ripiego."""

    def __init__(self, command: tuple, timeout: int, clock: Callable[[], float],
                 runner: TokenRunner = default_token_runner) -> None:
        self._command, self._timeout, self._clock, self._runner = command, timeout, clock, runner
        self._bearer: Optional[str] = None
        self._expires = 0.0

    def get(self) -> str:
        now = self._clock()
        if self._bearer is not None and now < self._expires - TOKEN_REFRESH_MARGIN:
            return self._bearer
        self._bearer = None
        try:
            rc, out = self._runner(self._command, self._timeout)
        except (OSError, subprocess.SubprocessError) as exc:
            raise CredentialError(f"comando del token non eseguito ({type(exc).__name__})") from None
        if rc != 0:
            raise CredentialError(f"comando del token uscito con {rc}")
        try:
            data = json.loads(out.decode("utf-8"))
            token, expires = data["token"], parse_iso(data["expires_at"])
        except (UnicodeDecodeError, ValueError, RecursionError, KeyError, TypeError):
            raise CredentialError("uscita del comando del token fuori formato") from None
        if not isinstance(token, str) or not _TOKEN_RE.fullmatch(token):
            raise CredentialError("token fuori formato")
        if expires <= now + TOKEN_REFRESH_MARGIN:
            raise CredentialError("token scaduto o in scadenza")
        self._bearer, self._expires = token, expires
        return token

    def invalidate(self) -> None:
        self._bearer = None


# --------------------------------------------------------------------------------------------- GitHub (polling)
@dataclass(frozen=True)
class Observed:
    pr: int
    head_sha: str
    eligible: bool          # base main, head nello stesso repository, non in bozza
    head_ref: str = ""      # branch di head, solo per la full-suite; "" se assente o fuori formato


def _get(obj: Any, *keys: str) -> Any:
    for key in keys:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj


def parse_pull(item: Any) -> Observed:
    """Una voce dell'elenco delle PR aperte, con tipi esatti. Malformata => ApiError (il giro intero si annulla)."""
    if not isinstance(item, dict):
        raise ApiError("voce dell'elenco PR non e' un oggetto")
    number = item.get("number")
    if not _is_int(number) or not _PR_RE.fullmatch(str(number)):
        raise ApiError("numero di PR fuori formato")
    if item.get("state") != "open" or not isinstance(item.get("draft"), bool):
        raise ApiError(f"PR #{number}: stato o bozza fuori formato")
    head_sha = _get(item, "head", "sha")
    if not isinstance(head_sha, str) or not _SHA_RE.fullmatch(head_sha):
        raise ApiError(f"PR #{number}: head SHA fuori formato")
    if _get(item, "base", "repo", "full_name") != TRUSTED_REPO or not isinstance(_get(item, "base", "ref"), str):
        raise ApiError(f"PR #{number}: base fuori formato")
    head_repo = _get(item, "head", "repo")
    if head_repo is not None and not isinstance(_get(head_repo, "full_name"), str):
        raise ApiError(f"PR #{number}: repository head fuori formato")
    eligible = (not item["draft"] and item["base"]["ref"] == BASE_REF
                and head_repo is not None and head_repo["full_name"] == TRUSTED_REPO)
    return Observed(pr=number, head_sha=head_sha, eligible=eligible, head_ref=clean_ref(_get(item, "head", "ref")))


def clean_ref(ref: Any) -> str:
    """Branch di head ammesso come `branch` di una build della full-suite, altrimenti "" (fail-closed solo per la
    full-suite di quella PR: i guard trusted non ne dipendono)."""
    if (not isinstance(ref, str) or not _REF_RE.fullmatch(ref) or ".." in ref or "//" in ref
            or ref.endswith(("/", ".lock", "."))):
        return ""
    return ref


def parse_pulls(items: list) -> list:
    seen: dict[int, Observed] = {}
    for item in items:
        obs = parse_pull(item)
        if obs.pr in seen and seen[obs.pr] != obs:
            raise ApiError(f"PR #{obs.pr} due volte con dati diversi: elenco incoerente")
        seen[obs.pr] = obs
    return sorted(seen.values(), key=lambda o: o.pr)


class GitHub:
    """Solo GET su `pulls`, `issues/comments` e `collaborators/{login}/permission` del repository fidato, paginazione
    completa e verificata. Nessun endpoint di contenuti o di archivi: il codice delle PR non passa mai da qui."""

    def __init__(self, cfg: Config, tokens: TokenProvider, transport: "Transport", clock: Callable[[], float]) -> None:
        self._cfg, self._tokens, self._transport, self._clock = cfg, tokens, transport, clock

    @staticmethod
    def check_url(url: str) -> None:
        try:
            parts = urllib.parse.urlsplit(url)
        except ValueError:
            raise ApiError("URL GitHub malformata") from None
        if parts.scheme != "https" or parts.netloc != "api.github.com" \
                or (parts.path not in _GITHUB_PATHS and not _PERMISSION_PATH_RE.fullmatch(parts.path)
                    and not _COMMIT_CHECKS_PATH_RE.fullmatch(parts.path)):
            raise ApiError(f"URL GitHub non ammessa: {parts.path}")

    def _rate_limit_until(self, headers: Mapping[str, str]) -> float:
        now = self._clock()
        retry = headers.get("retry-after", "")
        if retry.isdigit():
            return now + min(int(retry), 3600)
        reset = headers.get("x-ratelimit-reset", "")
        if reset.isdigit():
            return min(float(reset), now + 3600)
        return now + 60

    def _request(self, url: str) -> tuple[dict, Any]:
        """Una GET autenticata: (header in minuscolo, JSON). Rate limit, 401, HTTP diverso da 200, token classico,
        risposta troppo grande o non JSON => eccezione, mai un valore di ripiego."""
        self.check_url(url)
        headers = {"Authorization": f"Bearer {self._tokens.get()}", "Accept": "application/vnd.github+json",
                   "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "pf-trusted-dispatcher"}
        status, resp_headers, body = self._transport("GET", url, headers, None, self._cfg.http_timeout_seconds)
        h = {k.lower(): v for k, v in dict(resp_headers).items()}
        if status in (403, 429) and (h.get("x-ratelimit-remaining") == "0" or "retry-after" in h):
            raise RateLimited(self._rate_limit_until(h))
        if status == 401:
            self._tokens.invalidate()
            raise CredentialError("token GitHub rifiutato (401)")
        if status != 200:
            raise ApiError(f"GitHub HTTP {status}", status)
        if h.get("x-oauth-scopes", "").strip():
            raise CredentialError("token classico con scope: serve un token di sola lettura a durata limitata")
        if len(body) > MAX_API_BYTES:
            raise ApiError("risposta GitHub troppo grande")
        try:
            return h, json.loads(body)
        except (ValueError, RecursionError):
            raise ApiError("risposta GitHub non JSON") from None

    @staticmethod
    def next_page_url(link: str, path: str, params: Mapping[str, str], page: int) -> str:
        """URL della pagina `page + 1`, ricostruito dal client in forma canonica `/repos/<repo>/...`. Il link `next`
        ricevuto vale solo come segnale e deve descrivere ESATTAMENTE quella pagina: `https://api.github.com`
        senza credenziali, porta o frammento; lo stesso path, nella forma `/repos/<repo>/...` o in quella che GitHub
        usa davvero, `/repositories/<TRUSTED_REPO_ID>/...`; gli stessi parametri, una volta sola, piu' `page` uguale
        a `page + 1`. Ogni altra cosa (host, repository, commit, endpoint, parametri diversi) => ApiError."""
        prefix = f"/repos/{TRUSTED_REPO}/"
        if not path.startswith(prefix):
            raise ApiError("paginazione fuori dal repository fidato")
        expected = {**params, "page": str(page + 1)}
        try:
            parts = urllib.parse.urlsplit(link)
            pairs = urllib.parse.parse_qsl(parts.query, keep_blank_values=True, strict_parsing=True)
        except ValueError:
            raise ApiError("link di paginazione GitHub malformato") from None
        aliases = (path, f"/repositories/{TRUSTED_REPO_ID}/{path[len(prefix):]}")
        if parts.scheme != "https" or parts.netloc != "api.github.com" or parts.fragment or parts.path not in aliases:
            raise ApiError(f"link di paginazione GitHub non ammesso: {parts.path}")
        if len(dict(pairs)) != len(pairs) or dict(pairs) != expected:
            raise ApiError("link di paginazione GitHub con parametri o pagina inattesi")
        return f"{GITHUB_API}{path}?{urllib.parse.urlencode(expected)}"

    def get_all(self, path: str, params: Mapping[str, str], key: Optional[str] = None) -> list:
        """Tutte le pagine. Con `key` ogni pagina e' un oggetto `{"total_count": n, key: [...]}` (check run): il
        totale deve restare lo stesso fra le pagine e coincidere con gli elementi letti. Le pagine successive si
        chiedono sempre all'URL canonico (`next_page_url`), mai a quello del link ricevuto."""
        url: Optional[str] = f"{GITHUB_API}{path}?{urllib.parse.urlencode(params)}"
        items: list = []
        total: Optional[int] = None
        for page_number in range(1, MAX_PAGES + 1):
            h, page = self._request(url)
            if key is not None:
                if not isinstance(page, dict) or not _is_int(page.get("total_count")) \
                        or (total is not None and page["total_count"] != total):
                    raise ApiError("pagina GitHub con totale fuori formato o cambiato durante la lettura")
                total, page = page["total_count"], page.get(key)
            if not isinstance(page, list):
                raise ApiError("pagina GitHub non e' un elenco")
            items.extend(page)
            m = _LINK_NEXT_RE.search(h.get("link", ""))
            if m is None:
                if total is not None and len(items) != total:
                    raise ApiError("elenco GitHub incompleto rispetto a total_count")
                return items
            # pagina successiva strettamente crescente: nessun ciclo possibile, MAX_PAGES limita la lettura
            url = self.next_page_url(m.group(1), path, params, page_number)
        raise ApiError("paginazione GitHub oltre il limite: elenco non verificabile")

    def open_pulls(self) -> list:
        return self.get_all(f"/repos/{TRUSTED_REPO}/pulls",
                            {"state": "open", "per_page": "100", "sort": "created", "direction": "asc"})

    def comments_since(self, since: str) -> list:
        return self.get_all(f"/repos/{TRUSTED_REPO}/issues/comments",
                            {"since": since, "per_page": "100", "sort": "created", "direction": "asc"})

    def repo_permission(self, login: str) -> str:
        """Permesso ATTUALE di `login` sul repository fidato. 404 = nessun accesso (`none`). Valore sconosciuto o
        risposta riferita a un altro utente => ApiError: il permesso non e' verificato."""
        if not _LOGIN_RE.fullmatch(login):
            raise ApiError("login non valido")
        try:
            _, data = self._request(f"{GITHUB_API}/repos/{TRUSTED_REPO}/collaborators/{login}/permission")
        except ApiError as exc:
            if exc.status == 404:
                return "none"
            raise
        perm, who = (data.get("permission"), _get(data, "user", "login")) if isinstance(data, dict) else (None, None)
        if perm not in REPO_PERMISSIONS or not isinstance(who, str) or who.lower() != login.lower():
            raise ApiError("permesso GitHub fuori formato")
        return perm

    def check_runs(self, sha: str) -> list:
        """Check run dell'ultimo tentativo di ogni job (`filter=latest`) sul commit, tutte le pagine, numero letto
        uguale a `total_count`. Elenco incompleto o malformato => ApiError."""
        if not _SHA_RE.fullmatch(sha):
            raise ApiError("SHA non valido")
        return self.get_all(f"/repos/{TRUSTED_REPO}/commits/{sha}/check-runs", {"filter": "latest", "per_page": "100"},
                            key="check_runs")

    def statuses(self, sha: str) -> list:
        """Status del commit, dal piu' recente (ordine dell'API), tutte le pagine."""
        if not _SHA_RE.fullmatch(sha):
            raise ApiError("SHA non valido")
        return self.get_all(f"/repos/{TRUSTED_REPO}/commits/{sha}/statuses", {"per_page": "100"})


# `author_association` non certifica i permessi attuali (MEMBER = membro dell'organizzazione, COLLABORATOR anche in
# sola lettura): conta solo il permesso effettivo letto da `GET /repos/{repo}/collaborators/{login}/permission`
REPO_PERMISSIONS = frozenset({"admin", "maintain", "write", "triage", "read", "none"})
RERUN_PERMISSIONS = frozenset({"admin", "maintain", "write"})


@dataclass(frozen=True)
class RerunRequest:
    comment_id: int
    pr: int
    head_sha: str
    login: str
    user_type: str = ""
    permission: str = ""      # permesso attuale letto dall'API, solo per i login in allowlist
    created_at: str = ""      # ISO al secondo, come lo restituisce GitHub


def parse_rerun_comment(item: Any) -> Optional[RerunRequest]:
    """None se il commento non e' un comando di rerun; ApiError se e' un comando ma i dati sono fuori formato."""
    if not isinstance(item, dict) or not isinstance(item.get("body"), str):
        raise ApiError("commento fuori formato")
    m = _RERUN_RE.fullmatch(item["body"].strip())
    if m is None:
        return None
    cid, login, user_type = item.get("id"), _get(item, "user", "login"), _get(item, "user", "type")
    url = item.get("issue_url")
    prefix = f"{GITHUB_API}/repos/{TRUSTED_REPO}/issues/"
    # id entro 10^15: entra in SQLite e l'identificatore del rerun resta sotto i 64 caratteri del gate pre-command
    if not _is_int(cid) or not 0 < cid < MAX_COMMENT_ID or not isinstance(login, str) or not isinstance(url, str) \
            or not isinstance(user_type, str) \
            or not url.startswith(prefix) or not _PR_RE.fullmatch(url[len(prefix):]):
        raise ApiError("comando di rerun con dati fuori formato")
    edited = item.get("created_at") != item.get("updated_at") or not _TIMESTAMP_RE.fullmatch(str(item.get("created_at")))
    # modificato dopo la pubblicazione: il testo eseguito non sarebbe quello registrato
    return RerunRequest(comment_id=cid, pr=int(url[len(prefix):]), head_sha="" if edited else m.group(1),
                        login=login, user_type=user_type, created_at=str(item.get("created_at")))


def _rerun_too_old(created_at: str, now: float) -> bool:
    """Orario di creazione del commento (GitHub) oltre `RERUN_MAX_AGE_SECONDS`; illeggibile => troppo vecchio."""
    try:
        return now - parse_iso(created_at) > RERUN_MAX_AGE_SECONDS
    except (TypeError, ValueError):
        return True


# --------------------------------------------------------------------------------------------- archivio
SCHEMA = """
CREATE TABLE IF NOT EXISTS pr_state (
  pr INTEGER PRIMARY KEY,
  head_sha TEXT NOT NULL,
  eligible INTEGER NOT NULL,
  open INTEGER NOT NULL,
  missing INTEGER NOT NULL DEFAULT 0,
  seq INTEGER NOT NULL DEFAULT 0,
  head_ref TEXT NOT NULL DEFAULT '',
  updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS transitions (
  tid TEXT PRIMARY KEY,
  pr INTEGER NOT NULL,
  head_sha TEXT NOT NULL,
  kind TEXT NOT NULL,
  created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS reruns (
  comment_id INTEGER PRIMARY KEY,
  pr INTEGER NOT NULL,
  head_sha TEXT NOT NULL,
  login TEXT NOT NULL,
  accepted INTEGER NOT NULL,
  reason TEXT NOT NULL,
  tid TEXT,
  created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  delivery_id TEXT NOT NULL,
  event TEXT NOT NULL,
  pr INTEGER NOT NULL,
  head_sha TEXT NOT NULL,
  guard TEXT NOT NULL,
  state TEXT NOT NULL,
  superseded INTEGER NOT NULL DEFAULT 0,
  build_number INTEGER,
  build_url TEXT,
  outcome TEXT,
  description TEXT,
  created_at REAL NOT NULL,
  dispatched_at REAL,
  publishing_since REAL,
  pending_published INTEGER NOT NULL DEFAULT 0,
  pending_cancels TEXT NOT NULL DEFAULT '[]',
  final_published INTEGER NOT NULL DEFAULT 0,
  gate_seen INTEGER NOT NULL DEFAULT -1,
  gate_stable INTEGER NOT NULL DEFAULT 0,
  released_at REAL,
  head_ref TEXT NOT NULL DEFAULT '',
  updated_at REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_active_current ON jobs(pr, head_sha, guard)
  WHERE superseded = 0 AND state IN ('queued', 'creating', 'dispatched', 'publishing');
"""

# Colonne aggiunte dopo la prima versione dello schema: (nome, definizione), applicate da `Store._migrate`.
MIGRATION_COLUMNS = (
    ("publishing_since", "REAL"),
    ("pending_published", "INTEGER NOT NULL DEFAULT 0"),
    ("pending_cancels", "TEXT NOT NULL DEFAULT '[]'"),
    ("final_published", "INTEGER NOT NULL DEFAULT 0"),
    # attesa della readiness (#562 opzione A): check osservati, giri stabili consecutivi, istante del via libera
    ("gate_seen", "INTEGER NOT NULL DEFAULT -1"),
    ("gate_stable", "INTEGER NOT NULL DEFAULT 0"),
    ("released_at", "REAL"),
    ("head_ref", "TEXT NOT NULL DEFAULT ''"),
)
# stesse aggiunte per `pr_state` (branch di head per la full-suite, #562 PR-A)
MIGRATION_PR_STATE_COLUMNS = (
    ("head_ref", "TEXT NOT NULL DEFAULT ''"),
)


class Store:
    """SQLite con transazioni `BEGIN IMMEDIATE`: stato delle PR, transizioni e job si scrivono insieme o niente."""

    def __init__(self, path: str, *, read_only: bool = False, targets: tuple = tuple(GUARDS)) -> None:
        if not targets or any(t not in PIPELINES for t in targets):
            raise ValueError("target non ammessi")
        self._targets = tuple(targets)
        self._lock = threading.Lock()
        self._lock_fd: Optional[int] = None
        if read_only:
            uri = "file:" + urllib.parse.quote(path) + "?mode=ro"
            self._db = sqlite3.connect(uri, uri=True, isolation_level=None, check_same_thread=False, timeout=30)
            self._db.row_factory = sqlite3.Row
            return
        # un solo processo scrittore: due istanze sullo stesso archivio creerebbero due build per la stessa richiesta
        self._lock_fd = os.open(path + ".lock", os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(self._lock_fd)
            self._lock_fd = None
            raise ConfigError([f"state_db: archivio gia' in uso da un'altra istanza ({path})"]) from None
        self._db = sqlite3.connect(path, isolation_level=None, check_same_thread=False, timeout=30)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript(SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        """Archivio creato da una versione precedente (`CREATE TABLE IF NOT EXISTS` non aggiunge colonne; rilievo
        Sol su #569): colonne mancanti aggiunte e vecchio indice rimosso, in modo idempotente."""
        def fn(db: sqlite3.Connection) -> None:
            cols = {row[1] for row in db.execute("PRAGMA table_info(jobs)")}
            for name, ddl in MIGRATION_COLUMNS:
                if name not in cols:
                    db.execute(f"ALTER TABLE jobs ADD COLUMN {name} {ddl}")
            pr_cols = {row[1] for row in db.execute("PRAGMA table_info(pr_state)")}
            for name, ddl in MIGRATION_PR_STATE_COLUMNS:
                if name not in pr_cols:
                    db.execute(f"ALTER TABLE pr_state ADD COLUMN {name} {ddl}")
            db.execute("DROP INDEX IF EXISTS jobs_active")   # indice senza `superseded = 0` (P2-1)
            # finestre ancorate: un job in pubblicazione/annullamento senza `publishing_since` userebbe
            # `updated_at`, che si aggiorna a ogni giro, e non scadrebbe mai
            db.execute("UPDATE jobs SET publishing_since = updated_at WHERE publishing_since IS NULL "
                       "AND state IN ('publishing', 'canceling')")
        self._tx(fn)

    def close(self) -> None:
        with self._lock:
            self._db.close()
            if self._lock_fd is not None:
                os.close(self._lock_fd)        # chiudere il descrittore rilascia il flock
                self._lock_fd = None

    def _tx(self, fn: Callable[[sqlite3.Connection], Any]) -> Any:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                result = fn(self._db)
                self._db.execute("COMMIT")
            except BaseException:
                # anche un COMMIT fallito (SQLITE_BUSY): mai lasciare la connessione dentro una transazione aperta
                try:
                    self._db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            return result

    @staticmethod
    def _supersede_pr(db: sqlite3.Connection, pr: int, now: float, keep_head: Optional[str]) -> None:
        """Job attivi della PR non piu' validi: tutti (`keep_head` None) o quelli di un head diverso."""
        placeholders = ",".join("?" * len(ACTIVE_STATES))
        db.execute(f"UPDATE jobs SET superseded = 1, updated_at = ? WHERE pr = ? "  # nosec B608
                   f"AND (? IS NULL OR head_sha != ?) AND state IN ({placeholders})",
                   (now, pr, keep_head, keep_head, *ACTIVE_STATES))

    def _new_jobs(self, db: sqlite3.Connection, tid: str, kind: str, pr: int, head_sha: str, now: float,
                  head_ref: str = "") -> list[int]:
        if not _TID_RE.fullmatch(tid):
            raise ValueError("identificatore di transizione fuori formato")
        db.execute("INSERT INTO transitions(tid, pr, head_sha, kind, created_at) VALUES (?, ?, ?, ?, ?)",
                   (tid, pr, head_sha, kind, now))
        created: list[int] = []
        placeholders = ",".join("?" * len(ACTIVE_STATES))
        for guard in self._targets:
            cur = db.execute(
                "INSERT OR IGNORE INTO jobs(delivery_id, event, pr, head_sha, guard, state, head_ref, created_at, "
                "updated_at) VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?)",
                (tid, kind, pr, head_sha, guard, head_ref, now, now))
            if cur.rowcount == 1:
                created.append(int(cur.lastrowid))
                # gli status GitHub sono per SHA e contesto: una valutazione nuova dello stesso SHA, anche di un'altra
                # PR, rende vecchie quelle attive precedenti, che annullano la build e non pubblicano piu' nulla
                # (Codex P1 su #569)
                db.execute(f"UPDATE jobs SET superseded = 1, updated_at = ? WHERE head_sha = ? AND guard = ? "  # nosec B608
                           f"AND id < ? AND state IN ({placeholders})",
                           (now, head_sha, guard, int(cur.lastrowid), *ACTIVE_STATES))
        return created

    def apply_poll(self, observed: list, now: float) -> list[str]:
        """Confronta un elenco COMPLETO delle PR aperte con lo stato salvato e registra le transizioni, tutto in una
        transazione. Ritorna gli identificatori delle transizioni create."""

        def fn(db: sqlite3.Connection) -> list[str]:
            created: list[str] = []
            seen = {o.pr for o in observed}
            for o in observed:
                row = db.execute("SELECT * FROM pr_state WHERE pr = ?", (o.pr,)).fetchone()
                if row is None:
                    kind: Optional[str] = "opened"
                elif not row["open"]:
                    kind = "reopened"                   # anche sullo stesso SHA: va rivalutata
                elif row["head_sha"] != o.head_sha:
                    kind = "synchronize"                # anche il ritorno a uno SHA gia' elaborato
                elif not row["eligible"] and o.eligible:
                    kind = "ready_for_review"
                else:
                    kind = None
                seq = row["seq"] if row is not None else 0
                if kind is not None or (row is not None and row["eligible"] and not o.eligible):
                    self._supersede_pr(db, o.pr, now, keep_head=o.head_sha if o.eligible else None)
                if kind is not None and o.eligible:
                    seq += 1
                    tid = f"p{o.pr}-t{seq}-{o.head_sha[:12]}"
                    self._new_jobs(db, tid, kind, o.pr, o.head_sha, now, o.head_ref)
                    created.append(tid)
                db.execute("INSERT INTO pr_state(pr, head_sha, eligible, open, missing, seq, head_ref, updated_at) "
                           "VALUES (?, ?, ?, 1, 0, ?, ?, ?) ON CONFLICT(pr) DO UPDATE SET "
                           "head_sha = excluded.head_sha, eligible = excluded.eligible, open = 1, missing = 0, "
                           "seq = excluded.seq, head_ref = excluded.head_ref, updated_at = excluded.updated_at",
                           (o.pr, o.head_sha, int(o.eligible), seq, o.head_ref, now))
            for row in db.execute("SELECT pr, missing FROM pr_state WHERE open = 1").fetchall():
                if row["pr"] in seen:
                    continue
                if row["missing"] + 1 >= CLOSE_AFTER_MISSES:
                    # assente da due elenchi completi consecutivi: chiusa (un singolo elenco puo' perdere una PR
                    # spostata fra due pagine mentre lo si legge)
                    db.execute("UPDATE pr_state SET open = 0, missing = 0, updated_at = ? WHERE pr = ?",
                               (now, row["pr"]))
                    self._supersede_pr(db, row["pr"], now, keep_head=None)
                else:
                    db.execute("UPDATE pr_state SET missing = missing + 1, updated_at = ? WHERE pr = ?",
                               (now, row["pr"]))
            return created

        return self._tx(fn)

    def record_rerun(self, req: RerunRequest, allowed: frozenset, now: float,
                     floor: Optional[str] = None) -> tuple[str, str]:
        """(esito, motivo). Esiti: `duplicate` (comando gia' registrato), `rejected`, `accepted`. Ogni comando
        nuovo resta nell'archivio, accettato o no."""

        def fn(db: sqlite3.Connection) -> tuple[str, str]:
            if db.execute("SELECT 1 FROM reruns WHERE comment_id = ?", (req.comment_id,)).fetchone():
                return "duplicate", "comando gia' registrato"
            row = db.execute("SELECT * FROM pr_state WHERE pr = ?", (req.pr,)).fetchone()
            if req.login not in allowed:
                reason = f"login {req.login} non autorizzato"
            elif req.user_type != "User":
                reason = f"l'autore non e' un utente ({req.user_type})"
            elif req.permission not in RERUN_PERMISSIONS:
                reason = f"permesso attuale {req.permission or 'non verificato'} senza scrittura sul repository"
            elif not req.head_sha:
                reason = "commento modificato dopo la pubblicazione"
            elif _rerun_too_old(req.created_at, now):
                reason = f"comando piu' vecchio di {RERUN_MAX_AGE_SECONDS // 3600} ore (orario del commento)"
            elif floor is None or req.created_at <= floor:
                # stesso secondo del primo avvio compreso: non si sa se e' anteriore, quindi non si esegue
                reason = "comando non successivo al primo avvio del dispatcher"
            elif row is None or not row["open"]:
                reason = "PR non aperta"
            elif row["missing"]:
                # assente dall'ultimo elenco delle PR aperte (chiusa o mergiata, non ancora confermata): dopo il
                # ripristino di uno snapshot e' il caso di #575 in T252, mai un rerun da eseguire
                reason = "PR assente dall'ultimo elenco delle PR aperte"
            elif not row["eligible"]:
                reason = "PR non valutabile (bozza, base diversa da main o fork)"
            elif row["head_sha"] != req.head_sha:
                reason = "lo SHA non e' l'head corrente della PR"
            else:
                reason = ""
            tid = f"p{req.pr}-r{req.comment_id}-{req.head_sha[:12]}" if not reason else None
            db.execute("INSERT INTO reruns(comment_id, pr, head_sha, login, accepted, reason, tid, created_at) "
                       "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (req.comment_id, req.pr, req.head_sha, req.login, int(not reason), reason, tid, now))
            if reason:
                return "rejected", reason
            self._supersede_pr(db, req.pr, now, keep_head=None)   # anche i job dello stesso head: si rivaluta
            self._new_jobs(db, tid, "manual", req.pr, req.head_sha, now, row["head_ref"])
            return "accepted", tid

        return self._tx(fn)

    def get_meta(self, key: str) -> Optional[str]:
        with self._lock:
            row = self._db.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row is not None else None

    def set_meta(self, key: str, value: str) -> None:
        self._tx(lambda db: db.execute("INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET "
                                       "value = excluded.value", (key, value)))

    def redeliverable_jobs(self, now: float) -> list[dict]:
        """Job `publish_failed` ancora validi: nessun job piu' nuovo sullo stesso SHA con lo stesso guard, PR aperta e
        valutabile con lo stesso head, ultimo tentativo almeno `REDELIVERY_SECONDS` fa."""
        with self._lock:
            rows = self._db.execute(
                "SELECT j.* FROM jobs j JOIN pr_state p ON p.pr = j.pr "
                "WHERE j.state = 'publish_failed' AND j.superseded = 0 AND p.open = 1 AND p.eligible = 1 "
                "AND p.head_sha = j.head_sha AND COALESCE(j.publishing_since, j.updated_at) <= ? "
                # gli status GitHub sono per SHA e contesto: un job piu' nuovo sullo stesso SHA, della stessa PR o di
                # un'altra, rende vecchio questo esito (Sol su #569); un head diverso e' escluso da `p.head_sha`
                "AND NOT EXISTS (SELECT 1 FROM jobs k WHERE k.head_sha = j.head_sha "
                "AND k.guard = j.guard AND k.id > j.id) "
                "ORDER BY j.id", (now - REDELIVERY_SECONDS,)).fetchall()
        return [dict(r) for r in rows]

    def active_jobs(self) -> list[dict]:
        placeholders = ",".join("?" * len(ACTIVE_STATES))
        with self._lock:
            rows = self._db.execute(f"SELECT * FROM jobs WHERE state IN ({placeholders}) ORDER BY id",  # nosec B608
                                    ACTIVE_STATES).fetchall()
        return [dict(r) for r in rows]

    def get_job(self, job_id: int) -> dict:
        with self._lock:
            row = self._db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return dict(row) if row is not None else {}

    def rekey_transition(self, old_tid: str, new_tid: str, event: str, pr: int, head_sha: str, seq: int,
                         now: float) -> None:
        """Una transizione locale e' la stessa valutazione di una gia' eseguita da Buildkite con un tid piu' recente
        (archivio ripristinato, #562 P0): tutti i job della transizione prendono quel tid ed evento, il `seq` della
        PR risale almeno a quello, e il tid entra nel registro delle transizioni. Tutto in una transazione."""

        def fn(db: sqlite3.Connection) -> None:
            # solo i job ancora senza build: uno gia' avviato resta legato alla propria build e al proprio intento
            db.execute("UPDATE jobs SET delivery_id = ?, event = ?, updated_at = ? WHERE delivery_id = ? AND pr = ? "
                       "AND head_sha = ? AND state = 'queued' AND build_number IS NULL",
                       (new_tid, event, now, old_tid, pr, head_sha))
            db.execute("UPDATE pr_state SET seq = MAX(seq, ?), updated_at = ? WHERE pr = ?", (seq, now, pr))
            db.execute("INSERT OR IGNORE INTO transitions(tid, pr, head_sha, kind, created_at) VALUES (?, ?, ?, ?, ?)",
                       (new_tid, pr, head_sha, event, now))

        self._tx(fn)

    def intent_job(self, delivery_id: str, pr: int, head_sha: str, guard: str) -> dict:
        """Job dello stesso intento (transizione o rerun) per un altro target: {} se assente."""
        with self._lock:
            row = self._db.execute("SELECT * FROM jobs WHERE delivery_id = ? AND pr = ? AND head_sha = ? AND guard = ? "
                                   "ORDER BY id DESC LIMIT 1", (delivery_id, pr, head_sha, guard)).fetchone()
        return dict(row) if row is not None else {}

    def recent_jobs(self, limit: int = 50) -> list[dict]:
        with self._lock:
            rows = self._db.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def has_rerun(self, comment_id: int) -> bool:
        with self._lock:
            return self._db.execute("SELECT 1 FROM reruns WHERE comment_id = ?", (comment_id,)).fetchone() is not None

    def recent_reruns(self, limit: int = 50) -> list[dict]:
        with self._lock:
            rows = self._db.execute("SELECT * FROM reruns ORDER BY comment_id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def update_job(self, job_id: int, now: float, *, expected_state: Optional[str] = None, **fields: Any) -> bool:
        """Con `expected_state` la modifica avviene solo se il job e' ancora in quello stato (True se applicata)."""
        allowed = {"state", "build_number", "build_url", "outcome", "description", "dispatched_at",
                   "publishing_since", "pending_published", "pending_cancels", "final_published", "gate_seen",
                   "gate_stable", "released_at"}
        if not fields or set(fields) - allowed:
            raise ValueError("campi non ammessi")
        cols = ", ".join(f"{k} = ?" for k in fields)
        where, params = "id = ?", [job_id]
        if expected_state is not None:
            where, params = "id = ? AND state = ?", [job_id, expected_state]
        cur = self._tx(lambda db: db.execute(f"UPDATE jobs SET {cols}, updated_at = ? WHERE {where}",  # nosec B608
                                             (*fields.values(), now, *params)))
        return cur.rowcount == 1

    def purge(self, now: float, retention_hours: int) -> None:
        """Pulizia dei job chiusi e delle transizioni vecchie. I comandi di rerun restano: sono l'audit."""
        cutoff = now - retention_hours * 3600
        placeholders = ",".join("?" * len(FINAL_STATES))

        def fn(db: sqlite3.Connection) -> None:
            db.execute(f"DELETE FROM jobs WHERE updated_at < ? AND state IN ({placeholders})",  # nosec B608
                       (cutoff, *FINAL_STATES))
            db.execute("DELETE FROM transitions WHERE created_at < ?", (cutoff,))

        self._tx(fn)


# --------------------------------------------------------------------------------------------- HTTP / Buildkite
Transport = Callable[[str, str, Mapping[str, str], Optional[bytes], int],
                     "tuple[int, Mapping[str, str], bytes]"]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401 - firma di urllib
        return None   # un redirect seguito ripeterebbe l'Authorization verso un altro host


_OPENER = urllib.request.build_opener(_NoRedirect)


def urllib_transport(method: str, url: str, headers: Mapping[str, str], body: Optional[bytes],
                     timeout: int) -> "tuple[int, Mapping[str, str], bytes]":
    if not url.startswith("https://"):
        raise TransportError("solo https")
    req = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    # `http.client.HTTPException` (es. IncompleteRead di una risposta chunked troncata, rilievo Codex P2 su #569)
    # e' un errore di trasporto: non deve sfuggire al polling prima del controllo del timeout
    try:
        with _OPENER.open(req, timeout=timeout) as resp:  # nosec B310 - schema https verificato sopra
            return resp.status, dict(resp.headers), resp.read(MAX_API_BYTES + 1)
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, dict(exc.headers or {}), exc.read(MAX_API_BYTES + 1) if exc.fp else b""
        except (http.client.HTTPException, OSError, ValueError) as inner:
            raise TransportError(type(inner).__name__) from None
    except (urllib.error.URLError, http.client.HTTPException, OSError, ValueError) as exc:
        raise TransportError(type(exc).__name__) from None


class Buildkite:
    """Client minimo: crea, legge, annulla build delle DUE pipeline ammesse e scarica l'artifact del risultato."""

    def __init__(self, cfg: Config, bearer: str, transport: Transport = urllib_transport) -> None:
        self._cfg = cfg
        self._bearer = bearer
        self._transport = transport

    def _pipeline_url(self, guard: str) -> str:
        pipeline = PIPELINES[guard]["pipeline"]  # KeyError su un target non ammesso: mai una pipeline libera
        return f"{BUILDKITE_API}/organizations/{self._cfg.organization}/pipelines/{pipeline}"

    def _request(self, method: str, url: str, payload: Any = None) -> "tuple[int, Any]":
        headers = {"Authorization": f"Bearer {self._bearer}", "Accept": "application/json"}
        body = None
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        status, _, data = self._transport(method, url, headers, body, self._cfg.http_timeout_seconds)
        if len(data) > MAX_API_BYTES:
            raise ApiError("risposta Buildkite troppo grande", status)
        try:
            return status, (json.loads(data) if data else None)
        except (ValueError, RecursionError):        # RecursionError: JSON annidato all'estremo, non ValueError
            raise ApiError("risposta Buildkite non JSON", status) from None

    def create_build(self, guard: str, env: Mapping[str, str], message: str, *, commit: str = "HEAD",
                     branch: str = BASE_REF) -> dict:
        """Solo un rifiuto 4xx esplicito e' un esito DEFINITO (nessuna build). Ogni altra risposta non
        interpretabile (5xx, 201 con corpo troncato o non JSON, 2xx inattesi) e' AMBIGUA: la build puo' esistere,
        quindi TransportError e riconciliazione (rilievi Codex P2 su #569)."""
        try:
            status, data = self._request("POST", f"{self._pipeline_url(guard)}/builds",
                                         {"commit": commit, "branch": branch, "message": message,
                                          "env": dict(env),
                                          "meta_data": {INTENT_META_KEY: env.get("PF_DELIVERY_ID", ""),
                                                        PR_META_KEY: env.get("PF_PR_NUMBER", "")}})
        except ApiError as exc:
            if exc.status is not None and 400 <= exc.status < 500:
                raise
            raise TransportError(f"risposta di creazione non interpretabile ({exc})") from None
        if 400 <= status < 500:
            # il motivo di Buildkite (es. pipeline o branch non ammessi) entra nell'errore, quindi nello status e nel
            # journal: senza, un rifiuto non e' diagnosticabile (certificazione #585)
            reason = rejection_reason(data, secrets=(self._bearer,))
            raise ApiError(f"creazione build rifiutata (HTTP {status}{': ' + reason if reason else ''})", status)
        if (status != 201 or not isinstance(data, dict) or not _is_int(data.get("number"))
                or data["number"] <= 0):
            # anche un 201 senza un numero di build utilizzabile: la build puo' esistere, si riconcilia
            raise TransportError(f"risposta di creazione non interpretabile (HTTP {status})")
        return data

    def get_build(self, guard: str, number: int) -> dict:
        status, data = self._request("GET", f"{self._pipeline_url(guard)}/builds/{int(number)}")
        if status != 200 or not isinstance(data, dict):
            raise ApiError(f"lettura build fallita (HTTP {status})", status)
        return data

    def list_builds(self, guard: str, created_from: str, *, commit: Optional[str] = None) -> list:
        """Build della pipeline dal momento indicato: su `main` per i guard trusted, sul commit della PR per la
        full-suite."""
        scope = {"commit": commit} if commit is not None else {"branch": BASE_REF}
        query = urllib.parse.urlencode({**scope, "created_from": created_from, "per_page": "100"})
        status, data = self._request("GET", f"{self._pipeline_url(guard)}/builds?{query}")
        if status != 200 or not isinstance(data, list):
            raise ApiError(f"elenco build fallito (HTTP {status})", status)
        if len(data) >= 100:
            # pagina piena: la build cercata puo' stare nella pagina dopo; l'assenza non e' dimostrata
            raise ApiError("elenco build pieno: assenza non verificabile")
        return data

    def list_builds_by_intent(self, guard: str, intent: str, *, commit: Optional[str] = None) -> list:
        """Build della pipeline con il metadato dell'intento (`pf_intent`), a qualunque data: la prova remota che un
        intento ha gia' una build, indipendente dall'archivio locale. Stesso ambito di `list_builds` (`main` per i
        guard trusted, commit della PR per la full-suite). Pagina piena => assenza non verificabile."""
        if not _TID_RE.fullmatch(intent):
            raise ApiError("identita' dell'intento fuori formato")
        scope = {"commit": commit} if commit is not None else {"branch": BASE_REF}
        return self._list_builds_by_meta(guard, INTENT_META_KEY, intent, scope=scope)

    def list_builds_by_pr(self, guard: str, pr: str, *, any_branch: bool = False) -> list:
        """Build della pipeline con il metadato `pf_pr`, dalla piu' recente (ordine dell'API), pagina dopo pagina fino
        alla prima pagina che contiene una transizione della PR o alla fine dell'elenco: col `seq` crescente la
        transizione piu' recente porta il massimo, e i rerun (anche cento di fila) non la nascondono (Sol su #583).
        Oltre `PR_PAGES_MAX` pagine senza transizioni => eccezione: stato remoto incerto, mai un massimo indovinato.
        `any_branch` (full-suite): le build stanno sui branch della PR, non su `main`; basta il metadato `pf_pr`."""
        if not _PR_META_RE.fullmatch(pr):
            raise ApiError("numero della PR fuori formato")
        found: list = []
        for page in range(1, PR_PAGES_MAX + 1):
            data = self._list_builds_by_meta(guard, PR_META_KEY, pr, full_page_ok=True, page=page,
                                             scope={} if any_branch else {"branch": BASE_REF})
            found.extend(data)
            if len(data) < INTENT_PAGE or any(
                    isinstance(b, dict) and isinstance(b.get("env"), dict)
                    and isinstance(b["env"].get("PF_DELIVERY_ID"), str)
                    and _TRANSITION_TID_RE.fullmatch(b["env"]["PF_DELIVERY_ID"]) for b in data):
                return found
        raise ApiError(f"oltre {PR_PAGES_MAX} pagine di build della PR senza transizioni: massimo non verificabile")

    def _list_builds_by_meta(self, guard: str, key: str, value: str, *, scope: Mapping[str, str],
                             full_page_ok: bool = False, page: int = 1) -> list:
        query = urllib.parse.urlencode({**scope, f"meta_data[{key}]": value,
                                        "per_page": str(INTENT_PAGE), "page": str(page)})
        status, data = self._request("GET", f"{self._pipeline_url(guard)}/builds?{query}")
        if status != 200 or not isinstance(data, list):
            raise ApiError(f"elenco build dell'intento fallito (HTTP {status})", status)
        if len(data) >= INTENT_PAGE and not full_page_ok:
            raise ApiError("elenco build dell'intento pieno: assenza non verificabile")
        return data

    def cancel_build(self, guard: str, number: int) -> None:
        status, _ = self._request("PUT", f"{self._pipeline_url(guard)}/builds/{int(number)}/cancel")
        if status not in (200, 422):           # 422: build gia' terminata, niente da annullare
            raise ApiError(f"annullamento fallito (HTTP {status})", status)

    def fetch_result(self, guard: str, number: int) -> bytes:
        """L'unico artifact `result_artifacts[guard]` della build, in stato `finished`. Il download segue UN
        redirect verso un host ammesso, SENZA Authorization."""
        status, data = self._request("GET", f"{self._pipeline_url(guard)}/builds/{int(number)}/artifacts?per_page=100")
        if status != 200 or not isinstance(data, list):
            raise ApiError(f"elenco artifact fallito (HTTP {status})", status)
        wanted = self._cfg.result_artifacts[guard]
        matches = [a for a in data if isinstance(a, dict) and a.get("path") == wanted]
        if len(matches) != 1:
            raise ApiError(f"artifact {wanted}: {len(matches)} trovati, atteso 1")
        art = matches[0]
        size = art.get("file_size")
        if art.get("state") != "finished" or not _is_int(size) or not 0 < size <= MAX_ARTIFACT_BYTES:
            raise ApiError("artifact non completo o di dimensione non ammessa")
        url = art.get("download_url")
        prefix = f"{BUILDKITE_API}/organizations/{self._cfg.organization}/pipelines/{GUARDS[guard]['pipeline']}/"
        if not isinstance(url, str) or not url.startswith(prefix):
            raise ApiError("download_url fuori dall'API Buildkite attesa")
        headers = {"Authorization": f"Bearer {self._bearer}"}
        status, resp_headers, _ = self._transport("GET", url, headers, None, self._cfg.http_timeout_seconds)
        location = {k.lower(): v for k, v in dict(resp_headers).items()}.get("location")
        if status not in (301, 302, 303, 307) or not isinstance(location, str):
            raise ApiError(f"download artifact senza redirect (HTTP {status})", status)
        try:
            parsed = urllib.parse.urlsplit(location)
            host = parsed.hostname
        except ValueError:
            raise ApiError("redirect dell'artifact malformato") from None
        if parsed.scheme != "https" or host not in self._cfg.artifact_download_hosts:
            raise ApiError("redirect dell'artifact verso un host non ammesso")
        status, _, body = self._transport("GET", location, {}, None, self._cfg.http_timeout_seconds)
        if status != 200 or len(body) != size:
            # anche un corpo troncato (Content-Length non rispettata) e' un download fallito
            raise ApiError(f"download artifact fallito (HTTP {status}, {len(body)}/{size} byte)", status)
        return body


# --------------------------------------------------------------------------------------------- esito
def build_env(job: Mapping[str, Any]) -> dict[str, str]:
    """ESATTAMENTE le cinque variabili del contratto §5.3; nessun segreto, nessun'altra variabile."""
    return {
        "PF_PR_NUMBER": str(job["pr"]),
        "PF_PR_HEAD_SHA": job["head_sha"],
        "PF_PR_BASE_REF": BASE_REF,
        "PF_EVENT": job["event"],
        "PF_DELIVERY_ID": job["delivery_id"],
    }


def build_matches_job(build: Any, guard: str, job: Mapping[str, Any], org: str) -> bool:
    """La build letta e' quella chiesta: env identico a quello inviato, URL della pipeline giusta e, per i guard
    trusted, branch main; per la full-suite branch di head della PR e commit ESATTO dell'head (mai `HEAD`)."""
    if not isinstance(build, dict):
        return False
    number, url = build.get("number"), build.get("web_url")
    prefix = f"{BUILDKITE_WEB}/{org}/{PIPELINES[guard]['pipeline']}/builds/"
    if guard == SUITE:
        where = (bool(job.get("head_ref")) and build.get("branch") == job["head_ref"]
                 and build.get("commit") == job["head_sha"])
    else:
        where = build.get("branch") == BASE_REF
    return (_is_int(number) and number > 0 and where
            and build.get("env") == build_env(job)
            and isinstance(url, str) and url == f"{prefix}{number}")


def decide_outcome(build: Mapping[str, Any], artifact: Optional[bytes], job: Mapping[str, Any]) -> tuple[str, str]:
    """(stato GitHub, descrizione). `success` SOLO con build `passed` E risultato del guard PASS sullo stesso
    repository, PR, guard e head SHA. Tutto il resto e' `failure` (verdetto FAIL coerente) o `error`."""
    state = build.get("state")
    if state not in ("passed", "failed"):
        return "error", f"build {state}: nessun verdetto"
    if artifact is None:
        return "error", "risultato del guard assente"
    try:
        result = json.loads(artifact.decode("utf-8"))
    except (UnicodeDecodeError, ValueError, RecursionError):
        return "error", "risultato del guard illeggibile"
    if not isinstance(result, dict):
        return "error", "risultato del guard illeggibile"
    pr = result.get("pr")
    if (result.get("guard") != job["guard"] or result.get("repo") != TRUSTED_REPO
            or not _is_int(pr) or pr != job["pr"]):
        return "error", "risultato di un'altra PR o di un altro controllo"
    if result.get("head_sha") != job["head_sha"]:
        return "error", "stale SHA: il guard ha valutato un head diverso"
    verdict = result.get("verdict")
    if state == "passed" and verdict == "PASS":
        return "success", "PASS"
    if state == "failed" and verdict == "FAIL":
        reasons = result.get("reasons")
        first = reasons[0] if isinstance(reasons, list) and reasons and isinstance(reasons[0], str) else "FAIL"
        return "failure", f"FAIL: {first}"
    return "error", f"build {state} incoerente con verdetto {verdict!r}"


#: i self-check della readiness non si attendono (dipendenza circolare): lo status della lane `pr-merge-readiness`
#: e il check run Actions «Merge readiness», come `is_self_check` di `scripts/pr_flow_automation.py`
READINESS_SELF_STATUS_CONTEXTS = frozenset({GUARDS["readiness"]["context"]})
STATUS_STATES = frozenset({"pending", "success", "failure", "error"})


def _is_readiness_self_check(name: str) -> bool:
    return "merge readiness" in name.lower()


def checks_gate(runs: Any, statuses: Any) -> tuple[list, int]:
    """(check ancora aperti, check osservati) sull'head, esclusi i self-check della readiness. Un check run conta
    aperto finche' non e' `completed`; uno status finche' il suo ultimo valore (l'API li restituisce dal piu' recente)
    e' `pending`. L'esito (success/failure) non conta qui: lo giudica la readiness. Dati fuori formato => ApiError."""
    if not isinstance(runs, list) or not isinstance(statuses, list):
        raise ApiError("check o status fuori formato")
    pending: list = []
    seen = 0
    for run in runs:
        if not isinstance(run, dict) or not isinstance(run.get("name"), str) or not isinstance(run.get("status"), str):
            raise ApiError("check run fuori formato")
        if _is_readiness_self_check(run["name"]):
            continue
        seen += 1
        if run["status"] != "completed":
            pending.append(run["name"])
    latest: dict = {}
    for st in statuses:
        if not isinstance(st, dict) or not isinstance(st.get("context"), str) or st.get("state") not in STATUS_STATES:
            raise ApiError("status fuori formato")
        latest.setdefault(st["context"], st["state"])
    for context, state in latest.items():
        if context in READINESS_SELF_STATUS_CONTEXTS:
            continue
        seen += 1
        if state == "pending":
            pending.append(context)
    return sorted(pending), seen


def decide_suite_outcome(build: Mapping[str, Any], job: Mapping[str, Any]) -> tuple[str, str]:
    """Esito della full-suite (#562 PR-A), dallo stato della build e dei suoi step: nessun artifact, perche' un file
    prodotto dal codice della PR non aggiungerebbe garanzie. `success` SOLO se la build e' `passed` sul commit
    dell'head E contiene tutti gli step `FULL_SUITE_STEP_KEYS` (definiti su `main`), ciascuno `passed` e non
    soft-fail. Ogni altro step presente deve essere `passed`, oppure non eseguito per una condizione (`broken`,
    `skipped`: per esempio gli scenari di collaudo con `if:` della pipeline), mai fallito o soft-fail. Build
    `failed` coerente => `failure`. Tutto il resto (step mancanti, stato incoerente, elenco job illeggibile) =>
    `error`."""
    state = build.get("state")
    if state not in ("passed", "failed"):
        return "error", f"build {state}: nessun verdetto"
    if build.get("commit") != job["head_sha"]:
        return "error", "stale SHA: la build ha eseguito un commit diverso dall'head"
    jobs = build.get("jobs")
    if not isinstance(jobs, list) or not all(isinstance(j, dict) for j in jobs):
        return "error", "elenco degli step della build illeggibile"
    steps: dict[Any, list] = {}
    for j in jobs:
        if j.get("type") == "script":
            steps.setdefault(j.get("step_key"), []).append(j)
    # un retry lascia il tentativo vecchio `retried`: conta l'ultimo tentativo di ogni step
    current = {k: [x for x in js if not x.get("retried")] or js for k, js in steps.items()}
    missing = sorted(FULL_SUITE_STEP_KEYS - set(steps))
    if state == "failed":
        failed = sorted(str(k) for k, js in current.items() if any(x.get("state") != "passed" for x in js))
        first = failed[0] if failed else "build failed"
        return "failure", f"FAIL: {first}" + (f" (+{len(failed) - 1})" if len(failed) > 1 else "")
    if missing:
        return "error", f"suite incompleta: {len(missing)} step attesi assenti (es. {missing[0]})"
    for key, js in current.items():
        allowed = ("passed",) if key in FULL_SUITE_STEP_KEYS else SUITE_EXTRA_STEP_STATES
        if any(x.get("state") not in allowed or x.get("soft_failed") for x in js):
            return "error", f"build passed incoerente: step {key} non passed"
    return "success", f"PASS ({len(steps)} step)"


# contratto del publisher a socket (Grok t215u): 1..140 caratteri ASCII stampabili, senza ` $ \
_DESCRIPTION_FORBIDDEN = frozenset("`$\\")
_DESCRIPTION_RE = re.compile(r"[\x20-\x23\x25-\x5b\x5d-\x5f\x61-\x7e]{1,%d}" % DESCRIPTION_MAX)


REJECTION_REASON_MAX = 120
#: sequenza senza spazi lunga quanto un token (token API di Buildkite o GitHub, base64, hex): mai in status o journal
_SECRET_LIKE_RE = re.compile(r"[A-Za-z0-9_\-+/=.]{24,}")


def rejection_reason(data: Any, secrets: tuple = ()) -> str:
    """Il campo `message` di una risposta 4xx di Buildkite, su una riga e troncato; "" se assente o non testo. I
    `secrets` noti (il token del dispatcher) e ogni stringa simile a un segreto diventano `[redatto]` (owner su #585)."""
    message = data.get("message") if isinstance(data, dict) else None
    if not isinstance(message, str):
        return ""
    for known in secrets:
        if isinstance(known, str) and known != "":
            message = message.replace(known, "[redatto]")
    return _SECRET_LIKE_RE.sub("[redatto]", " ".join(message.split()))[:REJECTION_REASON_MAX]


def clean_description(text: str) -> str:
    """Descrizione dello status nel formato del socket, senza cambiarne il senso in silenzio: lettere accentate alla
    lettera base, spazi e controlli a spazio singolo, ogni altro carattere vietato a '?' (visibile), testo troppo
    lungo chiuso da '...'. Il testo originale resta nei log del dispatcher."""
    out = []
    for c in unicodedata.normalize("NFKD", text):
        if unicodedata.combining(c):
            continue
        if c.isspace() or not c.isprintable():
            c = " "
        elif not ("\x20" <= c <= "\x7e") or c in _DESCRIPTION_FORBIDDEN:
            c = "?"
        out.append(c)
    cleaned = " ".join("".join(out).split()) or "(nessuna descrizione)"
    if len(cleaned) > DESCRIPTION_MAX:
        cleaned = cleaned[:DESCRIPTION_MAX - 3].rstrip() + "..."
    return cleaned


def read_only_transport(transport: Transport) -> Transport:
    """DRY-RUN (#562): solo GET. Qualsiasi scrittura (creazione o annullamento di build, POST/PUT/PATCH/DELETE) e'
    rifiutata PRIMA di uscire dal processo, anche se un percorso del codice provasse a farla."""
    def guarded(method: str, url: str, headers: Mapping[str, str], body: Optional[bytes], timeout: int):
        if method != "GET":
            raise TransportError(f"DRY-RUN: richiesta {method} bloccata")
        return transport(method, url, headers, body, timeout)
    return guarded


def dry_run_publisher(*_args: Any) -> int:
    """DRY-RUN: nessuno status viene pubblicato. Chiamarlo e' un errore del codice, non un esito."""
    raise RuntimeError("DRY-RUN: pubblicazione di status non ammessa")


def default_publisher(command: tuple, timeout: int) -> Callable[..., int]:
    def publish(pr: int, sha: str, state: str, context: str, url: str, description: str) -> int:
        if not _DESCRIPTION_RE.fullmatch(description):
            log("descrizione fuori dal contratto del publisher: status non inviato")
            return 2
        argv = [*command, str(pr), sha, state, context, url, description]
        try:
            proc = subprocess.run(argv, env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C.UTF-8"},  # nosec B603
                                  stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                  timeout=timeout, check=False)
        except (OSError, subprocess.SubprocessError) as exc:
            log(f"pubblicazione non eseguita: {type(exc).__name__}")
            return 1
        if proc.returncode != 0:
            # l'uscita dell'helper non si copia nei log: il dettaglio resta nei log dell'helper stesso
            log(f"pubblicazione rifiutata (exit {proc.returncode})")
        return proc.returncode
    return publish


def log(message: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{stamp} [pr-trusted-dispatcher] {message}", file=sys.stderr, flush=True)


# --------------------------------------------------------------------------------------------- dispatcher
class Dispatcher:
    def __init__(self, cfg: Config, store: Store, buildkite: Buildkite, github: GitHub,
                 publisher: Callable[..., int], clock: Callable[[], float] = time.time,
                 dry_run: bool = False) -> None:
        self.cfg = cfg
        self.dry_run = dry_run
        self.store = store
        self.bk = buildkite
        self.gh = github
        self.publish = publisher
        self.clock = clock
        self.rate_limited_until = 0.0

    # ---- intake (polling)
    def poll(self) -> list[str]:
        """Un giro di polling. Elenco delle PR incompleto, malformato, rate limit, errore API o credenziali =>
        nessuna transizione e nessuno status (fail closed). Ritorna gli identificatori delle transizioni create."""
        now = self.clock()
        if now < self.rate_limited_until:
            return []
        try:
            observed = parse_pulls(self.gh.open_pulls())
        except RateLimited as exc:
            self.rate_limited_until = exc.until
            log(f"giro di polling rinviato: {exc}")
            return []
        except (ApiError, TransportError, CredentialError) as exc:
            log(f"giro di polling annullato: {exc}")
            return []
        created = self.store.apply_poll(observed, now)
        for tid in created:
            log(f"transizione {tid}")
        if self.cfg.rerun_allowed_logins:
            created += self._poll_reruns(now)
        return created

    def _poll_reruns(self, now: float) -> list[str]:
        since = self.store.get_meta("comments_since")
        if since is None:
            # primo avvio: nessun comando storico rieseguito, si parte da adesso; l'istante resta come limite
            # inferiore persistente del cursore
            self.store.set_meta("comments_floor", iso(now))
            self.store.set_meta("comments_since", iso(now))
            return []
        try:
            requests = [r for r in (parse_rerun_comment(c) for c in self.gh.comments_since(since)) if r is not None]
            # permesso attuale letto per ogni comando nuovo di un utente in allowlist, PRIMA di registrare
            # qualsiasi esito: un permesso non verificabile annulla l'intera lettura e il cursore resta fermo
            requests = [replace(r, permission=self.gh.repo_permission(r.login))
                        if r.user_type == "User" and r.login in self.cfg.rerun_allowed_logins
                        and not self.store.has_rerun(r.comment_id) else r for r in requests]
        except RateLimited as exc:
            self.rate_limited_until = exc.until
            log(f"lettura dei comandi di rerun rinviata: {exc}")
            return []
        except (ApiError, TransportError, CredentialError) as exc:
            log(f"lettura dei comandi di rerun annullata (cursore invariato): {exc}")
            return []
        created: list[str] = []
        for req in requests:
            outcome, detail = self.store.record_rerun(req, self.cfg.rerun_allowed_logins, now,
                                                      self.store.get_meta("comments_floor") or since)
            if outcome != "duplicate":
                log(f"rerun commento {req.comment_id} di {req.login} su PR #{req.pr}: {outcome} ({detail})")
            if outcome == "accepted":
                created.append(detail)
        # la sovrapposizione non riporta mai il cursore prima del primo avvio (Codex P2 su #569); un archivio senza
        # limite salvato (versione precedente) usa il cursore attuale, che non e' mai anteriore all'avvio
        floor = self.store.get_meta("comments_floor") or since
        self.store.set_meta("comments_since", max(iso(now - COMMENTS_OVERLAP_SECONDS), floor))
        return created

    # ---- worker
    def tick(self) -> None:
        for job in self.store.active_jobs():
            try:
                self.advance(job)
            except Exception as exc:  # un job non deve fermare gli altri; lo stato resta quello salvato
                log(f"job {job['id']}: errore inatteso {type(exc).__name__}")
                self._internal_error_fallback(job)
        for job in self.store.redeliverable_jobs(self.clock()):
            try:
                self._redeliver(job)
            except Exception as exc:  # resta `publish_failed`: si riprova al prossimo intervallo
                log(f"job {job['id']}: riconsegna fallita {type(exc).__name__}")
        self.store.purge(self.clock(), self.cfg.retention_hours)

    def _redeliver(self, job: dict) -> None:
        """Nuovo tentativo di consegna di un esito `publish_failed` (vedi `redeliverable_jobs`). Ogni tentativo
        aggiorna `updated_at`, quindi un esito ancora da consegnare non viene ripulito."""
        current = self._final_still_current(job)
        if current is False:
            return None
        if current and self._send_final(job) == 0:
            log(f"job {job['id']}: esito {job['outcome']} riconsegnato")
            return self._set(job, state="done")
        return self._set(job, state="publish_failed", publishing_since=self.clock())

    def _internal_error_fallback(self, job: dict) -> None:
        """Un job che solleva a ogni giro non resta attivo per sempre: scaduto il timeout della build, la build si
        annulla (o si ricorda da annullare) e l'esito e' `error` (audit indipendente su #569)."""
        try:
            window = self.cfg.build_timeout_seconds[job["guard"]]
            if job["state"] in ("publishing", "canceling"):
                since = job.get("publishing_since") or job["created_at"]
                if self.clock() - since <= window:
                    return
                if job["state"] == "publishing":
                    return self._set(job, state="publish_failed", publishing_since=self.clock())
                log(f"job {job['id']}: annullamento mai confermato {job.get('pending_cancels')}: da verificare a mano")
                return self._set(job, state=self._after_cancels(job), pending_cancels="[]")
            started = job.get("dispatched_at") or self._started(job)
            if job["state"] not in ("queued", "creating", "dispatched"):
                return
            window = self.cfg.build_timeout_seconds[job["guard"]]
            if job["state"] == "queued" and self._waits_for_checks(job):
                window = max(window, self.cfg.readiness_wait_max_seconds)     # l'attesa dei check ha la sua finestra
            if self.clock() - started <= window:
                return
            if job.get("build_number") is not None:
                self._cancel_or_remember(job, [job["build_number"]])
            self._fail(job, "error", "errore interno del dispatcher")
        except Exception as exc:
            log(f"job {job['id']}: fallback non riuscito ({type(exc).__name__}): riprovo al prossimo giro")

    def advance(self, job: dict) -> None:
        if self.dry_run:
            # prima di ogni altro ramo, annullamenti pendenti compresi: in DRY-RUN nessun percorso reale (review su #571)
            return self._dry_run(job)
        # Obbligo di annullamento persistente e indipendente dallo stato (rilievi Codex su #569): le build
        # rimaste da annullare si ritentano a ogni giro, qualunque cosa stia facendo il job.
        if job["state"] != "canceling" and json.loads(job.get("pending_cancels") or "[]"):
            self._cancel_or_remember(job, json.loads(job["pending_cancels"]))
        if job["state"] == "canceling":                # esito gia' pubblicato: resta solo l'annullamento
            return self._retry_cancels(job)
        if job["superseded"]:
            return self._supersede(job)
        state = job["state"]
        if state == "queued":
            if self._waits_for_checks(job):
                return self._await_checks(job)
            return self._create(job)
        if state == "creating":
            return self._recover_creation(job)
        if state == "dispatched":
            return self._poll(job)
        if state == "publishing":
            return self._publish_final(job)
        return None

    def _waits_for_checks(self, job: dict) -> bool:
        return job["guard"] == "readiness" and self.cfg.readiness_wait_enabled and not job.get("released_at")

    @staticmethod
    def _started(job: dict) -> float:
        """Inizio della finestra della build: il via libera dell'attesa della readiness, altrimenti la creazione."""
        return job.get("released_at") or job["created_at"]

    def _await_checks(self, job: dict) -> None:
        """Readiness non bloccante (#562 opzione A): la build della readiness si crea solo quando gli altri check
        dell'head sono chiusi da `settle_polls` giri consecutivi, con lo stesso numero di check e dopo
        `min_delay_seconds`. L'attesa la tiene il dispatcher, non un agent Buildkite. Oltre `max_seconds` => `error`,
        mai PASS. Un head nuovo sostituisce il job (`apply_poll`), quindi l'attesa vecchia non avvia nulla."""
        now = self.clock()
        waited = now - job["created_at"]
        if waited > self.cfg.readiness_wait_max_seconds:
            return self._fail(job, "error", f"attesa dei check oltre {self.cfg.readiness_wait_max_seconds} s: "
                                            "readiness non avviata")
        if now < self.rate_limited_until:
            return None
        if not job.get("pending_published"):
            # `pending` subito: un vecchio esito sullo stesso SHA (rerun, riapertura) non resta visibile nell'attesa
            url = f"{BUILDKITE_WEB}/{self.cfg.organization}/{PIPELINES[job['guard']]['pipeline']}"
            if self.publish(job["pr"], job["head_sha"], "pending", PIPELINES[job["guard"]]["context"], url,
                            "in attesa degli altri check") == 0:
                self._set(job, pending_published=1)
        try:
            statuses = self.gh.statuses(job["head_sha"])
            pending, seen = checks_gate(self.gh.check_runs(job["head_sha"]), statuses)
        except RateLimited as exc:
            self.rate_limited_until = exc.until
            log(f"job {job['id']}: lettura dei check rinviata: {exc}")
            return None
        except (ApiError, TransportError, CredentialError) as exc:
            log(f"job {job['id']}: check dell'head non leggibili ({exc}): attesa invariata")
            return None
        if self.cfg.full_suite_enabled:
            suite, why = self._suite_verdict(job, statuses)
            if suite == "wait":
                pending = [*pending, why]
            elif suite != "success" and not pending:
                # suite conclusa e non superata: readiness negativa senza build (mai un PASS con la suite rossa)
                return self._fail(job, suite, f"readiness non avviata: {why}")
        if pending or seen == 0 or waited < self.cfg.readiness_wait_min_delay_seconds:
            return self._set(job, gate_seen=-1, gate_stable=0)
        stable = job["gate_stable"] + 1 if seen == job["gate_seen"] else 1
        if stable < self.cfg.readiness_wait_settle_polls:
            return self._set(job, gate_seen=seen, gate_stable=stable)
        self._set(job, gate_seen=seen, gate_stable=stable, released_at=now)
        log(f"job {job['id']}: {seen} check chiusi sull'head dopo {int(waited)} s: avvio della readiness")
        return self._create(job)

    def _suite_verdict(self, job: dict, statuses: Optional[list] = None) -> tuple[str, str]:
        """Full-suite dello STESSO intento della readiness (#562 P0, readiness prematura): (`wait`|`success`|`failure`|
        `error`, motivo). Conclusa solo con esito finale pubblicato dal dispatcher e, se `statuses` e' dato, lo stesso
        esito come ultimo status `full-suite` sull'head: assenza, `pending` o incoerenza => `wait`, mai successo."""
        suite = self.store.intent_job(job["delivery_id"], job["pr"], job["head_sha"], SUITE)
        if not suite:
            return "wait", "full-suite dell'intento non registrata"
        if suite["superseded"] or suite["state"] == "superseded":
            return "wait", "full-suite dell'intento sostituita"
        if not (suite["final_published"] and suite["state"] in ("done", "canceling")):
            return "wait", "full-suite in corso"
        outcome = suite.get("outcome")
        if outcome not in ("success", "failure", "error"):
            return "wait", f"full-suite con esito {outcome}"
        if statuses is not None:
            latest = next((st.get("state") for st in statuses if st.get("context") == SUITE_TARGET["context"]), None)
            if latest != outcome:
                return "wait", f"full-suite {outcome} non visibile sull'head (status {latest or 'assente'})"
        if outcome != "success":
            return ("failure" if outcome == "failure" else "error"), f"full-suite {outcome} sull'head"
        return "success", "full-suite success sull'head"

    def _dry_run(self, job: dict) -> None:
        """DRY-RUN: il job e' valutato fino al punto in cui scriverebbe fuori (build, annullamenti, status) e chiuso
        `done` con esito `dry_run`; nel log la build che verrebbe creata, con le cinque variabili esatte."""
        if job["state"] == "queued" and not job["superseded"]:
            env = " ".join(f"{k}={v}" for k, v in build_env(job).items())
            where = (f"sul commit {job['head_sha']} ({job.get('head_ref') or 'ref assente'})" if job["guard"] == SUITE
                     else "su main")
            log(f"DRY-RUN job {job['id']}: creerei {PIPELINES[job['guard']]['pipeline']} {where} con {env}; "
                f"pubblicherei pending su {job['head_sha']} ({PIPELINES[job['guard']]['context']})")
        # nessuna build reale dietro un job di collaudo: gli annullamenti pendenti si azzerano, altrimenti `_set` lo
        # terrebbe `canceling` e ritenterebbe un annullamento verso Buildkite
        self._set(job, state="done", outcome="dry_run", pending_cancels="[]",
                  description="dry-run: nessuna build, nessuno status")

    def _set(self, job: dict, **fields: Any) -> None:
        # Invariante in un solo punto: nessun job passa a uno stato finale con build ancora da annullare; resta
        # `canceling` (finestra dal momento della conversione) finche' l'annullamento non e' confermato.
        pending = fields.get("pending_cancels", job.get("pending_cancels")) or "[]"
        if fields.get("state") in FINAL_STATES and json.loads(pending):
            fields["state"] = "canceling"
            fields.setdefault("publishing_since", self.clock())
        self.store.update_job(job["id"], self.clock(), **fields)
        job.update(fields)

    def _supersede(self, job: dict) -> None:
        numbers = [job["build_number"]] if job["build_number"] is not None else []
        if job["state"] == "creating" and not numbers:
            # creazione ambigua: la build puo' esistere anche senza numero, e comparire in ritardo; si cerca e si
            # annulla, e un elenco vuoto non basta a ritirare il job prima della finestra (Codex P2 su #569)
            within = self.clock() - self._started(job) <= self.cfg.build_timeout_seconds[job["guard"]]
            try:
                numbers = [b["number"] for b in self._builds_for(job) if _is_int(b.get("number"))]
            except (ApiError, TransportError) as exc:
                if within:
                    log(f"job {job['id']}: build sostituita non verificabile ({exc}): riprovo")
                    return None
            if not numbers and within:
                return None
        unconfirmed = False
        for number in numbers:
            try:
                self.bk.cancel_build(job["guard"], number)
            except (ApiError, TransportError) as exc:
                unconfirmed = True
                log(f"job {job['id']}: annullamento build {number} non confermato: {exc}")
        # annullamento non confermato: il job resta attivo e si ritenta al giro dopo (rilievo Codex P2 su #569),
        # fino alla finestra del timeout della build; poi si ritira, dichiarato nei log
        started = job.get("dispatched_at") or self._started(job)        # adottata tardi: finestra dall'adozione
        if unconfirmed and self.clock() - started <= self.cfg.build_timeout_seconds[job["guard"]]:
            return None
        if unconfirmed:
            log(f"job {job['id']}: annullamento mai confermato entro la finestra: ritirato")
        if job["state"] == "creating" and not numbers and job.get("build_number") is None:
            log(f"job {job['id']}: creazione ambigua sostituita: nessuna build trovata da annullare "
                "(se arrivata dopo, resta da verificare a mano)")
        self._set(job, state="superseded", description="sostituito da un head piu' recente")
        return None

    def _builds_for(self, job: dict, _realigned: bool = False) -> list:
        """Build gia' esistenti per l'intento del job, ordinate per numero: quelle col metadato `pf_intent` (a
        qualunque data, anche prima di un ripristino dell'archivio) piu' quelle con l'env identico nella finestra
        (build create prima del metadato). Ogni elenco illeggibile o pieno => eccezione: assenza NON dimostrata."""
        created_from = datetime.fromtimestamp(self._started(job) - 300, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        expected = build_env(job)
        commit = job["head_sha"] if job["guard"] == SUITE else None
        found = {b["number"]: b for b in self.bk.list_builds(job["guard"], created_from, commit=commit)
                 if isinstance(b, dict) and b.get("env") == expected and _is_int(b.get("number"))}
        for b in self.bk.list_builds_by_intent(job["guard"], job["delivery_id"], commit=commit):
            if not isinstance(b, dict) or not _is_int(b.get("number")):
                continue
            env = b.get("env")
            if env == expected:
                found[b["number"]] = b
                continue
            # stesso `pf_intent`, env diversa: mai prova di assenza (Codex P2 su #583). Solo l'evento diverso, per una
            # transizione ricostruita da un archivio ripristinato (es. `ready_for_review` ricostruito `synchronize`),
            # e' la stessa valutazione: si riallinea l'evento e si rilegge. Ogni altra differenza e' un conflitto.
            own = _TRANSITION_TID_RE.fullmatch(job["delivery_id"])
            if (not _realigned and own is not None and isinstance(env, dict)
                    and env.get("PF_EVENT") in TRANSITION_EVENTS
                    and {k: v for k, v in env.items() if k != "PF_EVENT"}
                    == {k: v for k, v in expected.items() if k != "PF_EVENT"}):
                log(f"job {job['id']}: intento {job['delivery_id']} gia' eseguito come {env['PF_EVENT']}: evento "
                    "riallineato")
                self.store.rekey_transition(job["delivery_id"], job["delivery_id"], env["PF_EVENT"], job["pr"],
                                            job["head_sha"], int(own.group(2)), self.clock())
                job["event"] = env["PF_EVENT"]
                return self._builds_for(job, _realigned=True)
            raise ApiError(f"build {b['number']} dello stesso intento con env diversa: conflitto")
        return [found[n] for n in sorted(found)]

    def _fail(self, job: dict, state: str, description: str) -> None:
        log(f"job {job['id']}: {state}: {description!r}")   # il motivo resta nel journal anche se lo status si perde
        self._to_publishing(job, state, description)
        self._publish_final(job)

    def _to_publishing(self, job: dict, outcome: str, description: str) -> None:
        if outcome == "success" and job["guard"] == "readiness" and self.cfg.full_suite_enabled:
            suite, why = self._suite_verdict(job)
            if suite != "success":
                # ultima difesa: nessun PASS della readiness senza la full-suite dello stesso intento superata
                outcome, description = "error", f"readiness senza full-suite superata: {why}"
        cleaned = clean_description(description)
        if cleaned != description:
            # lo status porta la forma pulita; il testo completo resta qui (repr: nessun a capo nel log)
            log(f"job {job['id']}: descrizione adattata al publisher; originale {description!r}")
        self._set(job, state="publishing", outcome=outcome, description=cleaned, publishing_since=self.clock())

    def _create(self, job: dict) -> None:
        # L'archivio locale non prova che l'intento non abbia gia' una build (crash, ripristino di uno snapshot,
        # seconda istanza): prima si cercano le build remote dello stesso intento (#562 P0). Non verificabile =>
        # nessuna build, il job resta in attesa (RECOVERY_UNCERTAIN) e alla scadenza e' `error`, mai una creazione.
        fresh = self.store.get_job(job["id"])          # un job fratello puo' aver gia' riallineato il tid
        if fresh:
            job.update(delivery_id=fresh["delivery_id"], event=fresh["event"])
        try:
            self._adopt_later_transition(job)
            prior = self._builds_for(job)
        except (ApiError, TransportError) as exc:
            if self.clock() - self._started(job) > self.cfg.build_timeout_seconds[job["guard"]]:
                return self._fail(job, "error", f"RECOVERY_UNCERTAIN: build precedenti non verificabili ({exc})")
            log(f"job {job['id']}: RECOVERY_UNCERTAIN, build precedenti non verificabili ({exc}): nessuna creazione")
            if not job.get("pending_published"):
                # `pending` subito, ritentato a ogni giro se fallisce: un vecchio esito sullo stesso SHA non resta
                # visibile mentre la rivalutazione e' sospesa (Codex P1 su #583)
                url = f"{BUILDKITE_WEB}/{self.cfg.organization}/{PIPELINES[job['guard']]['pipeline']}"
                if self.publish(job["pr"], job["head_sha"], "pending", PIPELINES[job["guard"]]["context"], url,
                                "intento non verificabile su Buildkite: nessuna build") == 0:
                    self._set(job, pending_published=1)
            return None
        # da qui in poi un crash lascia l'esito ambiguo, mai un retry cieco; transizione condizionata allo stato
        # letto, cosi' una copia vecchia del job non crea una seconda build
        if not self.store.update_job(job["id"], self.clock(), expected_state="queued", state="creating"):
            return None
        job["state"] = "creating"
        if prior:
            return self._resume_prior(job, prior)
        message = f"PR #{job['pr']} {job['head_sha'][:12]} ({job['guard']}, delivery {job['delivery_id']})"
        where: dict[str, str] = {}
        if job["guard"] == SUITE:
            if not job.get("head_ref"):
                # senza branch valido non si crea nessuna build: mai una suite su `main` o su un ref indovinato
                return self._fail(job, "error", "branch di head della PR assente o fuori formato")
            where = {"commit": job["head_sha"], "branch": job["head_ref"]}
        try:
            build = self.bk.create_build(job["guard"], build_env(job), message, **where)
        except TransportError as exc:
            log(f"job {job['id']}: creazione build con esito ignoto ({exc}): riconciliazione")
            return
        except ApiError as exc:
            return self._fail(job, "error", str(exc))
        # due istanze possono aver creato insieme (nessuna vedeva l'altra): si tiene la build piu' vecchia dello
        # stesso intento e si annullano le altre; elenco illeggibile => si prosegue con la propria (gia' creata)
        # solo con una build creata leggibile: una risposta senza numero resta al percorso di `_adopt` (error)
        try:
            twins = [b for b in self._builds_for(job) if b["number"] != build.get("number")] \
                if isinstance(build, dict) and _is_int(build.get("number")) else []
        except (ApiError, TransportError) as exc:
            log(f"job {job['id']}: verifica dei doppioni non riuscita ({exc}): proseguo con la build {build.get('number')}")
            twins = []
        if twins:
            keep = min([build, *twins], key=lambda b: b["number"])
            for other in [build, *twins]:
                if other is not keep:
                    self._cancel_or_remember(job, [other["number"]])
            log(f"job {job['id']}: build doppie dello stesso intento, tengo la {keep['number']}")
            build = keep
        self._adopt(job, build)

    def _adopt_later_transition(self, job: dict) -> None:
        """Transizione locale con il `seq` non oltre il piu' alto gia' usato su Buildkite per la PR: l'archivio e' stato
        ripristinato e il contatore locale e' tornato indietro (Sol e Codex su #583). Se l'ultima valutazione remota
        della PR (build piu' recente) e' dello stesso head con la stessa env, e' questa stessa valutazione: il job e i
        fratelli in coda prendono quel tid. Altrimenti e' una transizione nuova, avvenuta dopo il ripristino: prende
        un tid oltre il massimo remoto. In entrambi i casi il `seq` locale risale oltre il massimo remoto, quindi un
        ritorno legittimo a uno SHA gia' visto resta una valutazione nuova. Elenco illeggibile => eccezione."""
        own = _TRANSITION_TID_RE.fullmatch(job["delivery_id"])
        if own is None:
            return None                                    # rerun: il tid (id del commento) e' gia' stabile
        expected = {k: v for k, v in build_env(job).items() if k not in ("PF_DELIVERY_ID", "PF_EVENT")}
        remote: list[tuple[int, int, str, dict]] = []      # (numero build, seq, tid, env)
        for b in self.bk.list_builds_by_pr(job["guard"], str(job["pr"]), any_branch=job["guard"] == SUITE):
            env = b.get("env") if isinstance(b, dict) else None
            if not isinstance(env, dict) or not _is_int(b.get("number")) or not isinstance(env.get("PF_DELIVERY_ID"),
                                                                                           str):
                continue
            m = _TRANSITION_TID_RE.fullmatch(env["PF_DELIVERY_ID"])
            if m is not None and int(m.group(1)) == job["pr"]:
                remote.append((b["number"], int(m.group(2)), env["PF_DELIVERY_ID"], env))
        if not remote:
            return None
        top_seq = max(seq for _, seq, _, _ in remote)
        if int(own.group(2)) > top_seq:
            return None                                    # contatore locale avanti: nessun ripristino in gioco
        _, _, last_tid, last_env = max(remote)              # ultima valutazione della PR (build piu' recente)
        same = ({k: v for k, v in last_env.items() if k not in ("PF_DELIVERY_ID", "PF_EVENT")} == expected
                and last_env.get("PF_EVENT") in TRANSITION_EVENTS)
        if same:
            tid, event = last_tid, last_env["PF_EVENT"]
            log(f"job {job['id']}: transizione {job['delivery_id']} gia' eseguita come {tid} (archivio ripristinato): "
                "riallineata")
        else:
            tid, event = f"p{job['pr']}-t{top_seq + 1}-{job['head_sha'][:12]}", job["event"]
            log(f"job {job['id']}: contatore locale dietro a quello remoto ({top_seq}): transizione nuova {tid}")
        self.store.rekey_transition(job["delivery_id"], tid, event, job["pr"], job["head_sha"],
                                    max(top_seq, int(_TRANSITION_TID_RE.fullmatch(tid).group(2))), self.clock())
        job.update(delivery_id=tid, event=event)
        return None

    def _resume_prior(self, job: dict, prior: list) -> None:
        """L'intento ha gia' una build (es. dopo il ripristino di uno snapshot): nessuna nuova. In corso => adottata
        e seguita fino all'esito; gia' conclusa => job chiuso SENZA ripubblicare (l'esito era stato pubblicato da
        chi l'aveva creata); piu' build dello stesso intento => si segue la piu' vecchia, le altre annullate."""
        keep, others = prior[0], prior[1:]
        if others:
            self._cancel_or_remember(job, [b["number"] for b in others])
        log(f"job {job['id']}: intento {job['delivery_id']} gia' eseguito dalla build {keep.get('number')}: "
            "nessuna nuova build")
        if keep.get("state") in BUILD_TERMINAL:
            if not build_matches_job(keep, job["guard"], job, self.cfg.organization):
                return self._fail(job, "error", "build precedente dell'intento non corrisponde alla richiesta")
            delivery = self._delivery(job, keep["number"])
            if delivery is None:
                return self._recovery_pending(job)
            publish, restore = delivery
            if publish:
                # build conclusa ma esito mai consegnato (crash fra la fine della build e la pubblicazione, Sol su
                # #583): si adotta, e l'esito si ricava dalla build come per ogni altra
                return self._adopt(job, keep)
            if restore is not None:
                # un `pending` di attesa o recupero copre l'esito gia' consegnato: lo si ripristina com'era (mai
                # l'esito di questa build, che e' piu' vecchio o uguale)
                if self.publish(job["pr"], job["head_sha"], restore["state"], PIPELINES[job["guard"]]["context"],
                                restore["target_url"], clean_description(str(restore.get("description") or
                                                                             "esito ripristinato"))) != 0:
                    if self.store.update_job(job["id"], self.clock(), expected_state="creating", state="queued"):
                        job["state"] = "queued"
                    return None
            # esito gia' visibile sull'head: non si ripubblica, un esito piu' recente dello stesso SHA resta valido;
            # `final_published` evita che l'annullamento dei doppioni lo rimandi in consegna
            outcome = "recovered"
            if job["guard"] == SUITE:
                # la readiness dello stesso intento consuma l'esito della suite (Codex su #584). Il verdetto si
                # RICALCOLA dagli step della build, come nel percorso normale: lo status GitHub prova solo che quel
                # verdetto e' stato consegnato da QUESTA build (ultimo status `full-suite`, stesso numero, stesso
                # stato). Uno status di terzi o di un'altra build non e' mai un verdetto (Sol su #584): resta
                # `recovered`, attesa e poi `error`
                try:
                    seen = restore if restore is not None else self._latest_built_status(job)
                    # status di un'altra build (piu' recente): la build ritrovata e' superata, si chiude `recovered`
                    # senza rileggerla, cosi' un errore di lettura non copre mai con un `pending` l'esito della suite
                    # successiva (Sol su #584)
                    mine = isinstance(seen, dict) and self._status_build(job, seen) == keep["number"]
                    computed = self._recovered_suite_verdict(job, keep["number"]) if mine else None
                except (ApiError, TransportError, CredentialError, RateLimited) as exc:
                    # errore di lettura transitorio: il job resta ritentabile, mai chiuso `recovered` per sempre (la
                    # readiness andrebbe in `error` con una suite verde, Sol su #584); oltre la finestra, `error`
                    if self.clock() - self._started(job) > self.cfg.build_timeout_seconds[job["guard"]]:
                        return self._fail(job, "error", f"RECOVERY_UNCERTAIN: suite ritrovata non verificabile ({exc})")
                    log(f"job {job['id']}: suite ritrovata non verificabile ({exc}): riprovo")
                    return self._recovery_pending(job)
                if mine and computed is not None and seen.get("state") == computed:
                    outcome = computed
            return self._set(job, state="done", outcome=outcome, final_published=1, build_number=keep["number"],
                             build_url=keep["web_url"],
                             description=f"intento gia' eseguito dalla build {keep['number']}: esito non ripubblicato")
        return self._adopt(job, keep)

    def _status_build(self, job: dict, status: Any) -> Optional[int]:
        """Numero della build del guard a cui punta il `target_url` di uno status, se e' una build della pipeline."""
        prefix = f"{BUILDKITE_WEB}/{self.cfg.organization}/{PIPELINES[job['guard']]['pipeline']}/builds/"
        url = status.get("target_url") if isinstance(status, dict) else None
        if isinstance(url, str) and url.startswith(prefix) and url[len(prefix):].isdigit():
            number = int(url[len(prefix):])
            return number if number > 0 else None
        return None

    def _recovery_pending(self, job: dict) -> None:
        """Verifica della build ritrovata sospesa (status o build illeggibili): si torna `queued` e si riprova entro la
        finestra, mai `creating`, che al giro dopo adotterebbe la build e ripubblicherebbe senza la verifica."""
        if job["state"] == "creating" and self.store.update_job(job["id"], self.clock(),
                                                                 expected_state="creating", state="queued"):
            job["state"] = "queued"
        if job["state"] == "queued" and not job.get("pending_published"):
            # `pending` senza build, ritentato finche' non arriva: un vecchio esito sullo stesso SHA non resta
            # visibile mentre la verifica di consegna e' sospesa (Codex P1 su #583). Senza numero di build non
            # vale mai come consegna in `_delivery`
            url = f"{BUILDKITE_WEB}/{self.cfg.organization}/{PIPELINES[job['guard']]['pipeline']}"
            if self.publish(job["pr"], job["head_sha"], "pending", PIPELINES[job["guard"]]["context"], url,
                            "esito della build ritrovata in verifica") == 0:
                self._set(job, pending_published=1)
        return None

    def _recovered_suite_verdict(self, job: dict, number: int) -> Optional[str]:
        """Verdetto della build della suite ritrovata, ricalcolato dalla build stessa (`decide_suite_outcome`); None
        se la build non corrisponde alla richiesta. Build illeggibile => eccezione: il chiamante riprova."""
        build = self.bk.get_build(SUITE, number)
        if not build_matches_job(build, SUITE, job, self.cfg.organization):
            return None
        return decide_suite_outcome(build, job)[0]

    def _latest_built_status(self, job: dict) -> Optional[dict]:
        """Ultimo status del contesto sull'head che punta a una build della pipeline (dal piu' recente); None se
        assente. Status illeggibili => eccezione: il chiamante riprova."""
        statuses = self.gh.statuses(job["head_sha"])
        if not isinstance(statuses, list):
            raise ApiError("status fuori formato")
        context = PIPELINES[job["guard"]]["context"]
        return next((st for st in statuses if isinstance(st, dict) and st.get("context") == context
                     and self._status_build(job, st) is not None), None)

    def _newer_on_github(self, job: dict) -> Optional[int]:
        """Build piu' recente di quella del job con uno status sullo stesso contesto e head, se esiste: l'esito del
        job e' superato (archivio ripristinato con un job ancora attivo, Codex su #583). Status illeggibili =>
        eccezione."""
        number = job.get("build_number")
        if not _is_int(number):
            return None
        statuses = self.gh.statuses(job["head_sha"])
        if not isinstance(statuses, list):
            raise ApiError("status fuori formato")
        context = PIPELINES[job["guard"]]["context"]
        seen = [self._status_build(job, st) for st in statuses if isinstance(st, dict) and st.get("context") == context]
        newer = [n for n in seen if n is not None and n > number]
        return max(newer) if newer else None

    def _final_still_current(self, job: dict) -> Optional[bool]:
        """Prima di pubblicare l'esito finale: True se nessuna build piu' recente ha gia' uno status (pubblicare),
        False se superato (job chiuso senza pubblicare), None se non verificabile per un `success` (si riprova: mai
        un verde alla cieca). Un esito negativo non verificabile si pubblica: non puo' dare un verde falso."""
        try:
            newer = self._newer_on_github(job)
        except (ApiError, TransportError, CredentialError, RateLimited) as exc:
            if job.get("outcome") == "success":
                log(f"job {job['id']}: status dell'head non leggibili ({exc}): success non pubblicato, riprovo")
                return None
            return True
        if newer is None:
            return True
        log(f"job {job['id']}: esito della build {job['build_number']} superato dalla build {newer}: non pubblicato")
        self._set(job, state="superseded",
                  description=f"esito della build {job['build_number']} superato dalla build {newer}")
        return False

    def _delivery(self, job: dict, number: int) -> Optional[tuple[bool, Optional[dict]]]:
        """Esito della build `number` dell'intento, ritrovata conclusa: gia' consegnato o no? (pubblicare, status da
        ripristinare). Si correla con gli status del contesto sull'head tramite il numero di build del `target_url`
        (crescente per pipeline):
        - uno status di una build piu' recente, in qualunque stato => superato, non si pubblica; se lo status con build
          piu' recente e' finale e coperto da un `pending` senza build (attesa, recupero), lo si ripristina com'era;
        - ultimo status con build = esito finale di questa build => consegnato (ripristinato se coperto);
        - altrimenti (nessuno status con build, `pending` di questa build, esito di una precedente) => si pubblica.
        Status illeggibili => None (Sol su #583)."""
        context = PIPELINES[job["guard"]]["context"]
        try:
            statuses = self.gh.statuses(job["head_sha"])
        except (ApiError, TransportError, CredentialError, RateLimited) as exc:
            if self.clock() - self._started(job) > self.cfg.build_timeout_seconds[job["guard"]]:
                self._fail(job, "error", f"RECOVERY_UNCERTAIN: status dell'head non leggibili ({exc})")
                return None
            log(f"job {job['id']}: status dell'head non leggibili ({exc}): esito della build ritrovata in verifica")
            return None
        if not isinstance(statuses, list):
            return None
        mine = [st for st in statuses if isinstance(st, dict) and st.get("context") == context]   # dal piu' recente
        built = [(n, st) for n, st in ((self._status_build(job, st), st) for st in mine) if n is not None]
        if not built:
            return True, None                          # nessuno status con build: l'esito non e' mai arrivato
        newest = max(n for n, _ in built)
        top_n, top = built[0]                          # lo status con build piu' recente in ordine di pubblicazione
        hidden_by_wait = any(st.get("state") == "pending" for st in mine[:mine.index(top)])   # attesa/recupero sopra
        final = top.get("state") in ("success", "failure", "error")
        if newest > number:
            # una build piu' recente ha gia' uno status: questo esito e' superato e non si ripubblica. Si ripristina
            # solo lo status finale che un `pending` senza build ha coperto (cio' che era visibile prima); con la build
            # piu' recente ancora in corso non si ripristina nulla (Sol su #583)
            # (Grok su #583: e solo se e' lo status della build piu' recente, mai un finale vecchio pubblicato dopo)
            return False, (top if final and hidden_by_wait and top_n == newest else None)
        if top_n == number and final:
            return False, (top if hidden_by_wait else None)
        return True, None                              # ultimo status: `pending` di questa build o di una vecchia

    def _adopt(self, job: dict, build: Any) -> None:
        if not build_matches_job(build, job["guard"], job, self.cfg.organization):
            if isinstance(build, dict) and _is_int(build.get("number")):
                self._set(job, build_number=build["number"])
                self._cancel_quietly(job)
            return self._fail(job, "error", "build creata non corrisponde alla richiesta")
        self._set(job, state="dispatched", build_number=build["number"], build_url=build["web_url"],
                  dispatched_at=self.clock())
        self._publish_pending(job)

    def _publish_pending(self, job: dict) -> None:
        """`pending` sull'head finche' la build gira, ritentato a ogni giro se fallisce (rilievo Codex P1 su #569):
        rivalutando un head gia' `success` (reopen), il vecchio `success` non deve restare l'esito visibile."""
        if self.publish(job["pr"], job["head_sha"], "pending", PIPELINES[job["guard"]]["context"], job["build_url"],
                        "build in corso") == 0:
            self._set(job, pending_published=1)
        else:
            log(f"job {job['id']}: status pending non pubblicato: ritento al prossimo giro")

    def _cancel_or_remember(self, job: dict, numbers: list) -> None:
        """Annulla le build indicate; quelle non confermate restano in `pending_cancels` e si ritentano dopo la
        pubblicazione dell'esito (stato `canceling`), mai dimenticate (audit indipendente su #569)."""
        pending = set(json.loads(job.get("pending_cancels") or "[]"))
        for number in numbers:
            if not _is_int(number):
                continue
            try:
                self.bk.cancel_build(job["guard"], number)
                pending.discard(number)
            except (ApiError, TransportError) as exc:
                pending.add(number)
                log(f"job {job['id']}: annullamento build {number} non confermato: {exc}")
        self._set(job, pending_cancels=json.dumps(sorted(pending)))

    def _cancel_quietly(self, job: dict) -> None:
        self._cancel_or_remember(job, [job["build_number"]])

    def _after_cancels(self, job: dict) -> str:
        """Stato finale quando non restano build da annullare: un esito mai consegnato torna `publish_failed` e
        quindi nella riconsegna (Codex P2 su #569); altrimenti `done`."""
        fresh = self.store.get_job(job["id"]) or job
        if fresh.get("outcome") and not fresh.get("final_published") and not fresh.get("superseded"):
            return "publish_failed"
        return "done"

    def _retry_cancels(self, job: dict) -> None:
        pending = json.loads(job.get("pending_cancels") or "[]")
        if pending:
            self._cancel_or_remember(job, pending)
        if not json.loads(job["pending_cancels"]):
            return self._set(job, state=self._after_cancels(job), publishing_since=self.clock())
        since = job.get("publishing_since") or job["updated_at"]
        if self.clock() - since > self.cfg.build_timeout_seconds[job["guard"]]:
            log(f"job {job['id']}: annullamento mai confermato per le build {job['pending_cancels']}: "
                "da verificare a mano")
            return self._set(job, state=self._after_cancels(job), pending_cancels="[]", description=clean_description(
                f"annullamento non confermato {job['pending_cancels']}; {job.get('description') or ''}"))
        return None

    def _recover_creation(self, job: dict) -> None:
        """Creazione con esito ignoto (timeout, 5xx, crash): si cerca la build per delivery e head. Una sola =>
        adottata; nessuna (elenco letto con successo) => error, mai una seconda creazione; piu' di una =>
        annullate tutte, error. Elenco illeggibile => si riprova fino al timeout della build, poi error."""
        if not job.get("pending_published"):
            # head rivalutato con creazione ambigua: `pending` subito, cosi' un vecchio `success` non resta l'esito
            # visibile per tutta la finestra di riconciliazione (audit su #569)
            url = f"{BUILDKITE_WEB}/{self.cfg.organization}/{PIPELINES[job['guard']]['pipeline']}"
            if self.publish(job["pr"], job["head_sha"], "pending", PIPELINES[job["guard"]]["context"], url,
                            "creazione build in verifica") == 0:
                self._set(job, pending_published=1)
        try:
            found = self._builds_for(job)
        except (ApiError, TransportError) as exc:
            if self.clock() - self._started(job) > self.cfg.build_timeout_seconds[job["guard"]]:
                return self._fail(job, "error", f"creazione build non verificabile: {exc}")
            return None
        if len(found) == 1:
            return self._adopt(job, found[0])
        if not found:
            # una build puo' comparire in ritardo: l'assenza vale solo scaduta la finestra della build
            if self.clock() - self._started(job) <= self.cfg.build_timeout_seconds[job["guard"]]:
                return None
            return self._fail(job, "error", "creazione build non confermata")
        self._cancel_or_remember(job, [b.get("number") for b in found])
        return self._fail(job, "error", f"{len(found)} build per la stessa richiesta: annullate")

    def _poll(self, job: dict) -> None:
        guard = job["guard"]
        try:
            build = self.bk.get_build(guard, job["build_number"])
        except (ApiError, TransportError) as exc:
            build = None
            log(f"job {job['id']}: lettura build fallita: {exc}")
        timed_out = self.clock() - (job["dispatched_at"] or self._started(job)) > self.cfg.build_timeout_seconds[guard]
        if build is not None and not build_matches_job(build, guard, job, self.cfg.organization):
            self._cancel_quietly(job)
            return self._fail(job, "error", "build letta non corrisponde alla richiesta")
        state = build.get("state") if build is not None else None
        if build is not None and (not isinstance(state, str) or state not in BUILD_RUNNING | BUILD_TERMINAL):
            self._cancel_quietly(job)
            return self._fail(job, "error", f"stato build sconosciuto: {state!r}")
        if build is None or state in BUILD_RUNNING:
            if timed_out:
                self._cancel_quietly(job)
                return self._fail(job, "error", "timeout della build")
            if not job["pending_published"]:
                self._publish_pending(job)
            return None
        if state == "blocked":
            self._cancel_quietly(job)
        if guard == SUITE:
            outcome, description = decide_suite_outcome(build, job)
            self._to_publishing(job, outcome, description)
            return self._publish_final(job)
        artifact: Optional[bytes] = None
        if state in ("passed", "failed"):
            try:
                artifact = self.bk.fetch_result(guard, job["build_number"])
            except (ApiError, TransportError) as exc:
                log(f"job {job['id']}: risultato non scaricato: {exc}")
                if not timed_out:
                    # errore temporaneo: si ritenta entro la finestra; intanto il `pending` mancato copre un vecchio
                    # `success` sullo stesso SHA (rerun, riapertura) come nel ramo della build in corso (Sol su #569)
                    if not job["pending_published"]:
                        self._publish_pending(job)
                    return None
        outcome, description = decide_outcome(build, artifact, job)
        self._to_publishing(job, outcome, description)
        self._publish_final(job)

    def _send_final(self, job: dict) -> int:
        outcome = job["outcome"]
        if outcome not in ("success", "failure", "error"):
            outcome, job["description"] = "error", "esito interno non valido"
        url = job["build_url"] or f"{BUILDKITE_WEB}/{self.cfg.organization}/{PIPELINES[job['guard']]['pipeline']}"
        rc = self.publish(job["pr"], job["head_sha"], outcome, PIPELINES[job["guard"]]["context"], url,
                          job["description"] or outcome)
        if rc == 0:
            # consegna registrata a parte dallo stato: `canceling` non deve far dimenticare un esito non consegnato
            self._set(job, final_published=1)
        return rc

    def _publish_final(self, job: dict) -> None:
        fresh = self.store.get_job(job["id"])
        if fresh.get("superseded"):
            return self._supersede(job)
        current = self._final_still_current(job)
        if current is False:
            return None
        rc = self._send_final(job) if current else 1
        if rc == 0:
            # con build ancora da annullare `_set` lo tiene in `canceling` finche' l'annullamento e' confermato
            return self._set(job, state="done")
        # pubblicazione fallita: si ritenta a ogni giro (stesso esito, status idempotente) finche' il job non e'
        # sostituito o finisce la finestra pari al timeout della build; poi `publish_failed`, dichiarato nei log
        since = job.get("publishing_since") or job["updated_at"]
        if self.clock() - since > self.cfg.build_timeout_seconds[job["guard"]]:
            log(f"job {job['id']}: pubblicazione non riuscita entro la finestra: publish_failed")
            # con build ancora da annullare `_set` lo tiene in `canceling` (Codex P2 su #569)
            return self._set(job, state="publish_failed", publishing_since=self.clock())
        return None



# --------------------------------------------------------------------------------------------- servizio
def build_dispatcher(cfg: Config, transport: Transport = urllib_transport,
                     publisher: Optional[Callable[..., int]] = None,
                     token_runner: TokenRunner = default_token_runner,
                     clock: Callable[[], float] = time.time, dry_run: bool = False) -> Dispatcher:
    """`dry_run` (#562): stesso codice e stesse letture, ma trasporto in sola GET, publisher che rifiuta e archivio
    separato `<state_db>.dry-run`, cosi' il collaudo non lascia stato nell'archivio reale."""
    bk_bearer = read_secret_file(cfg.buildkite_token_file, _TOKEN_RE, "buildkite.token_file")
    if dry_run:
        transport, publisher = read_only_transport(transport), dry_run_publisher
    tokens = TokenProvider(cfg.github_token_command, cfg.github_token_command_timeout_seconds, clock, token_runner)
    store = Store(cfg.state_db + ".dry-run" if dry_run else cfg.state_db, targets=cfg.targets)
    return Dispatcher(cfg, store, Buildkite(cfg, bk_bearer, transport), GitHub(cfg, tokens, transport, clock),
                      publisher or default_publisher(cfg.publisher_command, cfg.publisher_timeout_seconds), clock,
                      dry_run=dry_run)


def serve(cfg: Config, dry_run: bool = False) -> int:
    """Nessun web server: un solo ciclo che ogni `poll_interval_seconds` legge GitHub e fa avanzare i job."""
    dispatcher = build_dispatcher(cfg, dry_run=dry_run)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    log(f"polling ogni {cfg.poll_interval_seconds} s" + (" - DRY-RUN: nessuna build, nessuno status" if dry_run else ""))
    try:
        while not stop.is_set():
            for step in (dispatcher.poll, dispatcher.tick):
                try:
                    step()
                except Exception as exc:  # un giro fallito non ferma il servizio; lo stato salvato resta valido
                    log(f"{step.__name__} fallito: {type(exc).__name__}")
            stop.wait(cfg.poll_interval_seconds)
    except KeyboardInterrupt:
        pass
    finally:
        dispatcher.store.close()
    return EXIT_OK


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="pr_trusted_dispatcher.py", description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("check-config", "serve", "jobs", "reruns"))
    parser.add_argument("--config", required=False)
    parser.add_argument("--check-secrets", action="store_true",
                        help="check-config: legge il token Buildkite ed esegue una volta il comando del token GitHub")
    parser.add_argument("--dry-run", action="store_true",
                        help="serve: collaudo senza build, annullamenti ne' status (solo GET, archivio separato)")
    args = parser.parse_args(argv)
    if args.dry_run and args.command != "serve":
        parser.error("--dry-run vale solo con serve")
    try:
        cfg = load_config(args.config)
        if args.command == "check-config":
            if args.check_secrets:
                read_secret_file(cfg.buildkite_token_file, _TOKEN_RE, "buildkite.token_file")
                try:
                    TokenProvider(cfg.github_token_command, cfg.github_token_command_timeout_seconds,
                                  time.time).get()
                except CredentialError as exc:
                    raise ConfigError([f"github.token_command: {exc}"]) from None
            print("CONFIGURED")
            return EXIT_OK
        if args.command in ("jobs", "reruns"):
            store = Store(cfg.state_db, read_only=True)
            try:
                rows = store.recent_jobs() if args.command == "jobs" else store.recent_reruns()
                print(json.dumps(rows, indent=1))
            finally:
                store.close()
            return EXIT_OK
        return serve(cfg, dry_run=args.dry_run)
    except ConfigError as exc:
        print("NOT CONFIGURED", file=sys.stderr)
        for problem in exc.problems:
            print(f"  - {problem}", file=sys.stderr)
        return EXIT_NOT_CONFIGURED


if __name__ == "__main__":
    raise SystemExit(main())
