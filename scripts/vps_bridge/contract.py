#!/usr/bin/env python3
"""VPS Autonomy Bridge — contratto unico GitHub ↔ VPS (BRIDGE-02), versione preparatoria offline.

Funzioni pure, senza rete e senza I/O di sistema: le usano sia il lato GitHub (validazione prima di firmare) sia il
lato VPS (controllore delle richieste, ruolo separato). Nessuna shell, nessun argomento libero.

Regole (decisioni owner #586, GO-PARALLEL-PREP-01, WP4):
- ``suite_id`` immutabile per versione del catalogo; ``catalog_sha256`` deve coincidere col catalogo letto da una
  revisione fidata di ``main``; suite sconosciuta => rifiuto;
- richiesta firmata (la firma Sigstore la verifica il chiamante; qui si controlla il contenuto firmato);
- anti-replay: ``nonce`` di 32 hex mai visto prima e TTL massimo ``MAX_TTL_S``;
- corsie: ``development`` sempre consentita entro catalogo; ``certification`` solo con ``owner_go`` registrato;
  ``live`` NON esiste nel contratto (rifiuto per costruzione);
- ogni errore => rifiuto con motivo esplicito (fail-closed), mai un default permissivo.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

SCHEMA = "pf-test-request/2"
CATALOG_SCHEMA = "pf-test-catalog/2"
MAX_TTL_S = 1800
LANES = ("development", "certification")
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
NONCE_RE = re.compile(r"^[0-9a-f]{32}$")
SUITE_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,63}$")
REQUEST_KEYS = {"schema", "repo", "pr", "head_sha", "lane", "suites", "catalog_sha256", "nonce", "requested_by",
                "created_at_epoch", "run_id", "run_attempt"}
FORBIDDEN_ENVIRONMENTS = {"LIVE"}


class Rejected(Exception):
    """Richiesta rifiutata: ``reason`` e' un codice stabile, senza dati della richiesta."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def canonical(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def catalog_sha256(catalog: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical(catalog)).hexdigest()


def suite_index(catalog: Mapping[str, Any]) -> Dict[str, Mapping[str, Any]]:
    """Indice delle suite del catalogo v2. Catalogo malformato, id duplicati o suite LIVE => rifiuto."""
    if not isinstance(catalog, Mapping) or catalog.get("schema") != CATALOG_SCHEMA:
        raise Rejected("CATALOG_SCHEMA")
    suites = catalog.get("suites")
    if not isinstance(suites, list) or not suites:
        raise Rejected("CATALOG_EMPTY")
    out: Dict[str, Mapping[str, Any]] = {}
    for s in suites:
        sid = s.get("id") if isinstance(s, Mapping) else None
        if not isinstance(sid, str) or not SUITE_RE.match(sid):
            raise Rejected("CATALOG_SUITE_ID")
        if sid in out:
            raise Rejected("CATALOG_DUPLICATE_ID")
        if str(s.get("environment", "")).upper() in FORBIDDEN_ENVIRONMENTS:
            raise Rejected("CATALOG_LIVE_SUITE")
        args = s.get("args")
        if not isinstance(args, list) or not all(isinstance(a, str) for a in args):
            raise Rejected("CATALOG_ARGS")
        out[sid] = s
    return out


def verify_request(req: Mapping[str, Any], *, catalog: Mapping[str, Any], trusted_catalog_sha256: str,
                   repo: str, now_epoch: int, seen_nonces: Iterable[str],
                   owner_go_certification: bool = False) -> Tuple[str, Tuple[str, ...]]:
    """Valida una richiesta gia' autenticata (firma verificata altrove). Ritorna (lane, suite ammesse) o solleva."""
    if not isinstance(req, Mapping):
        raise Rejected("MALFORMED")
    if set(req) != REQUEST_KEYS:
        raise Rejected("FIELDS")                     # campi mancanti o in piu': nessuna estensione silenziosa
    if req["schema"] != SCHEMA:
        raise Rejected("SCHEMA")                     # richieste legacy (pf-test-request/1) rifiutate
    if req["repo"] != repo:
        raise Rejected("REPO")
    if not isinstance(req["pr"], int) or isinstance(req["pr"], bool) or req["pr"] < 1:
        raise Rejected("PR")
    if not isinstance(req["head_sha"], str) or not SHA_RE.match(req["head_sha"]):
        raise Rejected("HEAD_SHA")
    if not isinstance(req["nonce"], str) or not NONCE_RE.match(req["nonce"]):
        raise Rejected("NONCE")
    if req["nonce"] in set(seen_nonces):
        raise Rejected("REPLAY")
    created = req["created_at_epoch"]
    if not isinstance(created, int) or isinstance(created, bool):
        raise Rejected("CREATED_AT")
    if created > now_epoch + 60 or now_epoch - created > MAX_TTL_S:
        raise Rejected("EXPIRED")                    # tolleranza di orologio di 60 s nel futuro, mai oltre il TTL
    if not isinstance(trusted_catalog_sha256, str) or not HEX64_RE.match(trusted_catalog_sha256):
        raise Rejected("TRUSTED_CATALOG_UNKNOWN")
    if catalog_sha256(catalog) != trusted_catalog_sha256 or req["catalog_sha256"] != trusted_catalog_sha256:
        raise Rejected("CATALOG_MISMATCH")
    index = suite_index(catalog)
    lane = req["lane"]
    if lane not in LANES:
        raise Rejected("LANE")                       # nessuna corsia live nel contratto
    if lane == "certification" and owner_go_certification is not True:
        raise Rejected("CERTIFICATION_REQUIRES_OWNER_GO")
    suites = req["suites"]
    if not isinstance(suites, list) or not suites or len(suites) > 64:
        raise Rejected("SUITES")
    if len(set(suites)) != len(suites):
        raise Rejected("SUITES_DUPLICATE")
    for sid in suites:
        if not isinstance(sid, str) or sid not in index:
            raise Rejected("SUITE_UNKNOWN")
    if not isinstance(req["requested_by"], str) or not req["requested_by"]:
        raise Rejected("REQUESTER")
    for k in ("run_id", "run_attempt"):
        if not isinstance(req[k], int) or isinstance(req[k], bool) or req[k] < 1:
            raise Rejected(k.upper())
    return lane, tuple(suites)


def idempotency_key(req: Mapping[str, Any]) -> str:
    """Chiave unica di idempotenza (sostituisce le due divergenti di certd e #606)."""
    parts = [req["repo"], str(req["pr"]), req["head_sha"], req["lane"], ",".join(sorted(req["suites"])),
             req["catalog_sha256"]]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def check_run_name(lane: str, suite_id: str) -> str:
    """Nome unico del check (sostituisce le tre convenzioni divergenti)."""
    if lane not in LANES or not SUITE_RE.match(suite_id):
        raise Rejected("CHECK_NAME")
    return ("pf-test/" if lane == "certification" else "pf-dev-test/") + suite_id


def compat_report(repo_catalog: Mapping[str, Any], vps_catalog: Mapping[str, Any],
                  equivalences: Optional[Mapping[str, str]] = None) -> Dict[str, Any]:
    """Confronto offline catalogo GitHub (v2) ↔ catalogo certd della VPS: nessuna suite inventata, solo copertura
    dei percorsi di test. ``equivalences`` mappa id GitHub → id VPS per le campagne con nome diverso."""
    def repo_units(s):
        return {a.rstrip("/") for a in s.get("args", []) if not a.startswith("-") and (a.startswith("tests/") or a.endswith(".py"))}

    def vps_units(e):
        sel = e.get("selection") or []
        return {str(x).split("::")[0].rstrip("/") for x in (sel if isinstance(sel, list) else [sel]) if x}

    def covered(u, pool):
        return any(u == p or u.startswith(p + "/") or p.startswith(u + "/") for p in pool)

    vps = {e["id"]: vps_units(e) for e in vps_catalog.get("entries", [])}
    eq = dict(equivalences or {})
    rows, missing = [], []
    for s in repo_catalog.get("suites", []):
        units = repo_units(s)
        hits = sorted(i for i, vu in vps.items() if any(covered(u, vu) for u in units)) if units else []
        if s["id"] in eq:
            hits = sorted(set(hits) | {eq[s["id"]]})
        # un'equivalenza dichiarata (campagna/composito con nome diverso) vale come copertura solo se l'id VPS esiste
        if s["id"] in eq and eq[s["id"]] in vps:
            uncovered = []
        else:
            uncovered = sorted(u for u in units if not any(covered(u, vps[h]) for h in hits if h in vps))
        if not hits or uncovered:
            missing.append(s["id"])
        rows.append({"suite": s["id"], "vps": hits, "uncovered": uncovered})
    used = {h for r in rows for h in r["vps"]}
    return {"rows": rows, "only_vps": sorted(set(vps) - used), "missing": missing,
            "verdict": "COMPATIBLE" if not missing else "INCOMPATIBLE"}
