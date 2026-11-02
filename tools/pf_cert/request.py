"""Formato e verifica della richiesta trusted ``/pf-cert v2`` (workflow GitHub Actions -> orchestratore VPS).

Il commento con la richiesta NON e' una prova di autorizzazione: chiunque, compresi gli agenti che pubblicano
dall'account dell'owner (#472), puo' scrivere quel testo. La prova e' il token OIDC di GitHub Actions che la
richiesta porta con se': firmato da GitHub, legato al repository, al workflow ``pf-cert-request.yml`` del branch
``main`` (``job_workflow_ref``), all'evento ``pull_request_target`` e al ``run_id``. Una PR non puo' cambiare quel
workflow per la propria esecuzione (``pull_request_target`` usa la versione del branch base) e un workflow
``pull_request`` o di un altro branch ha un ``job_workflow_ref`` diverso.

La VPS (``verify_request``):
1. valida la riga con una regex stretta;
2. verifica la firma RS256 del token con le chiavi pubbliche di GitHub (JWKS) e tutti i claim;
3. rifiuta un ``jti`` o un ``run`` gia' visti (replay);
4. rilegge dall'API GitHub la PR e il suo head: la richiesta vale solo se ``sha`` e' ANCORA l'head (nuovo push =>
   richiesta superata) e se la PR non viene da un fork;
5. ricalcola gli scenari con il classificatore di ``main``; quelli della richiesta sono solo informativi.
Ogni condizione non dimostrata => rifiuto con codice, mai esecuzione.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Set

from tools.pf_cert.crypto_verify import b64url_decode, rs256_verify

PROTOCOL_VERSION = "v2"
OIDC_ISSUER = "https://token.actions.githubusercontent.com"
WORKFLOW_PATH = ".github/workflows/pf-cert-request.yml"
TRUSTED_EVENT = "pull_request_target"
TRUSTED_REF = "refs/heads/main"
MAX_CLOCK_SKEW_SEC = 60
MAX_TOKEN_AGE_SEC = 15 * 60
MAX_JWT_LEN = 4096

_SCENARIO = r"[a-z0-9][a-z0-9-]{0,47}"
_LINE_RE = re.compile(
    r"/pf-cert v2"
    r" pr=(?P<pr>[1-9][0-9]{0,8})"
    r" sha=(?P<sha>[0-9a-f]{40})"
    r" run=(?P<run>[1-9][0-9]{0,19})\.(?P<attempt>[1-9][0-9]{0,3})"
    r" catalog=(?P<catalog>[0-9]+\.[0-9]+\.[0-9]+)"
    rf" scenarios=(?P<scenarios>(?:{_SCENARIO}(?:,{_SCENARIO}){{0,15}})?|-)"
    r" oidc=(?P<oidc>[A-Za-z0-9_-]{10,1500}\.[A-Za-z0-9_-]{10,2500}\.[A-Za-z0-9_-]{10,1000})"
)


@dataclass(frozen=True)
class Request:
    pr: int
    sha: str
    run_id: int
    run_attempt: int
    catalog: str
    scenarios: tuple
    oidc: str


class Rejected(Exception):
    """Richiesta rifiutata: ``code`` e' un codice stabile, mai un dettaglio con dati riservati."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def format_request(*, pr: int, sha: str, run_id: int, run_attempt: int, catalog: str,
                   scenarios: Iterable[str], oidc: str) -> str:
    line = (f"/pf-cert v2 pr={int(pr)} sha={sha} run={int(run_id)}.{int(run_attempt)} catalog={catalog} "
            f"scenarios={','.join(scenarios) or '-'} oidc={oidc}")
    parse_request(line)                     # il workflow non pubblica mai una riga che la VPS rifiuterebbe
    return line


def parse_request(text: Any) -> Request:
    """La riga deve essere TUTTO il testo (nessun carattere prima o dopo, nessuna seconda riga)."""
    if not isinstance(text, str) or len(text) > MAX_JWT_LEN + 600:
        raise Rejected("MALFORMED")
    m = _LINE_RE.fullmatch(text.strip("\n"))
    if not m or "\n" in text.strip("\n"):
        raise Rejected("MALFORMED")
    scen = () if m["scenarios"] == "-" else tuple(m["scenarios"].split(","))
    if len(set(scen)) != len(scen):
        raise Rejected("MALFORMED")
    return Request(pr=int(m["pr"]), sha=m["sha"], run_id=int(m["run"]), run_attempt=int(m["attempt"]),
                   catalog=m["catalog"], scenarios=scen, oidc=m["oidc"])


def _json_part(part: str) -> Dict[str, Any]:
    try:
        value = json.loads(b64url_decode(part))
    except (ValueError, UnicodeDecodeError):
        raise Rejected("TOKEN_MALFORMED") from None
    if not isinstance(value, dict):
        raise Rejected("TOKEN_MALFORMED")
    return value


def verify_oidc_token(token: str, *, jwks: Mapping[str, Any], audience: str, now: float) -> Dict[str, Any]:
    """Firma RS256 con una chiave del JWKS di GitHub e claim temporali; ritorna i claim."""
    parts = token.split(".") if isinstance(token, str) else []
    if len(parts) != 3 or len(token) > MAX_JWT_LEN:
        raise Rejected("TOKEN_MALFORMED")
    header, claims = _json_part(parts[0]), _json_part(parts[1])
    if header.get("alg") != "RS256" or header.get("typ") not in (None, "JWT") or "crit" in header:
        raise Rejected("TOKEN_ALG_NOT_ALLOWED")
    kid = header.get("kid")
    keys = jwks.get("keys") if isinstance(jwks, Mapping) else None
    key = next((k for k in keys or [] if isinstance(k, Mapping) and k.get("kid") == kid and isinstance(kid, str)),
               None)
    if key is None or key.get("kty") != "RSA" or key.get("use", "sig") != "sig":
        raise Rejected("TOKEN_KEY_UNKNOWN")
    try:
        n = int.from_bytes(b64url_decode(key["n"]), "big")
        e = int.from_bytes(b64url_decode(key["e"]), "big")
        signature = b64url_decode(parts[2])
    except (KeyError, ValueError, TypeError):
        raise Rejected("TOKEN_KEY_UNKNOWN") from None
    if not rs256_verify(n, e, f"{parts[0]}.{parts[1]}".encode("ascii"), signature):
        raise Rejected("TOKEN_BAD_SIGNATURE")
    if claims.get("iss") != OIDC_ISSUER:
        raise Rejected("TOKEN_WRONG_ISSUER")
    aud = claims.get("aud")
    if not (aud == audience or (isinstance(aud, list) and aud == [audience])):
        raise Rejected("TOKEN_WRONG_AUDIENCE")
    exp, iat, nbf = claims.get("exp"), claims.get("iat"), claims.get("nbf", claims.get("iat"))
    if not all(isinstance(v, int) and not isinstance(v, bool) for v in (exp, iat, nbf)):
        raise Rejected("TOKEN_MALFORMED")
    if now > exp + MAX_CLOCK_SKEW_SEC or now + MAX_CLOCK_SKEW_SEC < nbf:
        raise Rejected("TOKEN_EXPIRED")
    if now - iat > MAX_TOKEN_AGE_SEC:
        raise Rejected("TOKEN_EXPIRED")
    return claims


def verify_request(text: Any, *, jwks: Mapping[str, Any], repository: str, repository_id: str, now: float,
                   seen: Set[str], pr_lookup: Callable[[int], Optional[Mapping[str, Any]]],
                   run_lookup: Callable[[int], Optional[Mapping[str, Any]]],
                   classify: Callable[[Iterable[str]], Mapping[str, Any]],
                   changed_files: Callable[[int], Optional[Iterable[str]]]) -> Dict[str, Any]:
    """Decisione completa della VPS su una richiesta. Ritorna ``{pr, sha, scenarios, mandatory}`` o solleva Rejected.

    ``pr_lookup``/``run_lookup``/``changed_files`` leggono l'API GitHub (installation token della app); ``None`` o
    un'eccezione => la condizione non e' dimostrata => rifiuto. ``seen`` e' il registro persistente anti-replay.
    """
    req = parse_request(text)
    audience = f"pf-cert:{repository}"
    claims = verify_oidc_token(req.oidc, jwks=jwks, audience=audience, now=now)
    expected = {
        "repository": repository,
        "repository_id": str(repository_id),
        "job_workflow_ref": f"{repository}/{WORKFLOW_PATH}@{TRUSTED_REF}",
        "event_name": TRUSTED_EVENT,
        "ref": TRUSTED_REF,
        "run_id": str(req.run_id),
        "run_attempt": str(req.run_attempt),
    }
    for claim, value in expected.items():
        if str(claims.get(claim)) != value:
            raise Rejected(f"TOKEN_CLAIM_MISMATCH:{claim}")
    jti = claims.get("jti")
    if not isinstance(jti, str) or not jti:
        raise Rejected("TOKEN_MALFORMED")
    for key in (f"jti:{jti}", f"run:{req.run_id}.{req.run_attempt}"):
        if key in seen:
            raise Rejected("REPLAY")
    try:
        run = run_lookup(req.run_id)
        pr = pr_lookup(req.pr)
        files = changed_files(req.pr)
    except Exception:  # noqa: BLE001 - API non leggibile => nulla e' dimostrato
        raise Rejected("GITHUB_API_UNAVAILABLE") from None
    if not isinstance(run, Mapping) or not isinstance(pr, Mapping) or files is None:
        raise Rejected("GITHUB_API_UNAVAILABLE")
    if run.get("path") != WORKFLOW_PATH or run.get("event") != TRUSTED_EVENT:
        raise Rejected("RUN_NOT_TRUSTED_WORKFLOW")
    head = pr.get("head") if isinstance(pr.get("head"), Mapping) else {}
    head_repo = head.get("repo") if isinstance(head.get("repo"), Mapping) else {}
    if pr.get("state") != "open":
        raise Rejected("PR_NOT_OPEN")
    if head_repo.get("full_name") != repository:
        raise Rejected("FORK_PR_REFUSED")
    if head.get("sha") != req.sha:
        raise Rejected("SHA_NOT_PR_HEAD")
    run_prs = [p.get("number") for p in run.get("pull_requests") or [] if isinstance(p, Mapping)]
    if req.pr not in run_prs:
        raise Rejected("RUN_NOT_FOR_THIS_PR")
    decision = classify(list(files))
    seen.update({f"jti:{jti}", f"run:{req.run_id}.{req.run_attempt}"})
    return {"pr": req.pr, "sha": req.sha, "scenarios": list(decision.get("scenarios") or []),
            "mandatory": list(decision.get("mandatory") or []),
            "catalog_version": decision.get("catalog_version")}


def main(argv: list, env: Mapping[str, str]) -> int:
    """CLI del workflow: ``request.py <scenarios.json>``; PR, SHA, run e token da variabili d'ambiente.

    Il token arriva da ``PF_CERT_OIDC`` (mai da argv). Stampa la riga ``/pf-cert v2``; dati mancanti o non validi =>
    codice d'uscita non zero e nessuna riga (il workflow fallisce, nessuna richiesta parziale).
    """
    if len(argv) != 2:
        return 2
    try:
        with open(argv[1], encoding="utf-8") as fh:
            decision = json.load(fh)
        line = format_request(pr=int(env["PR"]), sha=env["HEAD_SHA"], run_id=int(env["RUN_ID"]),
                              run_attempt=int(env["RUN_ATTEMPT"]), catalog=decision["catalog_version"],
                              scenarios=decision["scenarios"], oidc=env["PF_CERT_OIDC"])
    except (OSError, ValueError, KeyError, TypeError, Rejected):
        return 1
    print(line)
    return 0


if __name__ == "__main__":
    import os
    import sys
    sys.exit(main(sys.argv, os.environ))
