"""Verifica dei risultati firmati dall'orchestratore VPS e verdetto di certificazione di un head.

Ogni scenario eseguito produce un manifest JSON firmato ed25519 dall'orchestratore (chiave privata solo sulla VPS,
in ``/etc/pf-orch``; chiave pubblica pinnata qui). Il check ``pf-betfair-certification`` pubblicato dalla GitHub App
riporta, per ogni scenario, i BYTE ESATTI firmati e la firma (base64), cosi' la verifica non dipende da una
ri-serializzazione.

``certification_verdict`` e' ``CERTIFIED`` solo se ogni scenario OBBLIGATORIO per quell'head ha un manifest valido
con ``result == "PASS"``. Un manifest mancante, non valido, scaduto, di un altro SHA o di un altro repository, un
dataset non verificato, un FAIL, BLOCKED o NOT_RUN di uno scenario obbligatorio => mai ``CERTIFIED``.
"""
from __future__ import annotations

import base64
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional

from tools.pf_cert.classify import CATALOG, CATALOG_VERSION
from tools.pf_cert.crypto_verify import ed25519_public_key_from_spki_b64, ed25519_verify

#: chiave pubblica di firma dei risultati dell'orchestratore pf-orch (c.6575246644 su #586)
ORCHESTRATOR_RESULT_KEY_SPKI_B64 = "MCowBQYDK2VwAyEAGOCShUVxqBgNHHwlwN4JTGfSVBemrL7WByXgNXNx8yI="
RESULTS = ("PASS", "FAIL", "BLOCKED", "NOT_RUN")
#: scenari che usano dati Betfair autentici: il dataset deve risultare verificato nel manifest
DATA_SCENARIOS = frozenset({"sim-replay-microstake"})
_EVIDENCE_RE = re.compile(r"^pf-cert-evidence (?P<scenario>[a-z0-9-]{1,48}) (?P<m>[A-Za-z0-9+/=]{8,24000}) "
                          r"(?P<s>[A-Za-z0-9+/=]{86,90})$", re.M)


def _parse_time(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else None


def verify_manifest(manifest_bytes: bytes, signature: bytes, *, repository: str, head_sha: str, now: datetime,
                    public_key_spki_b64: str = ORCHESTRATOR_RESULT_KEY_SPKI_B64) -> Dict[str, Any]:
    """``{ok, code, scenario, result}``. ``ok`` e' True solo se il manifest e' autentico e vale per questo head."""
    def bad(code: str, scenario: str = "") -> Dict[str, Any]:
        return {"ok": False, "code": code, "scenario": scenario, "result": None}

    key = ed25519_public_key_from_spki_b64(public_key_spki_b64)
    if key is None:
        return bad("PUBLIC_KEY_INVALID")
    if not ed25519_verify(key, manifest_bytes, signature):
        return bad("BAD_SIGNATURE")
    try:
        m = json.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return bad("MANIFEST_MALFORMED")
    if not isinstance(m, dict):
        return bad("MANIFEST_MALFORMED")
    scenario = m.get("scenario") if isinstance(m.get("scenario"), str) else ""
    if m.get("repo") != repository:
        return bad("WRONG_REPOSITORY", scenario)
    if m.get("sha") != head_sha:
        return bad("SHA_MISMATCH", scenario)
    if scenario not in CATALOG:
        return bad("SCENARIO_NOT_IN_CATALOG", scenario)
    if m.get("catalog_version") != CATALOG_VERSION:
        return bad("CATALOG_VERSION_MISMATCH", scenario)
    issued, expires = _parse_time(m.get("issued_at")), _parse_time(m.get("expires_at"))
    if issued is None or expires is None or not issued <= now <= expires:
        return bad("EXPIRED_OR_NOT_YET_VALID", scenario)
    result = m.get("result")
    if result not in RESULTS:
        return bad("RESULT_UNKNOWN", scenario)
    if scenario in DATA_SCENARIOS and result == "PASS":
        dataset = m.get("dataset") if isinstance(m.get("dataset"), dict) else {}
        if dataset.get("signature") != "VERIFIED":
            return bad("DATASET_NOT_VERIFIED", scenario)
    return {"ok": True, "code": "VERIFIED", "scenario": scenario, "result": result}


def parse_evidence(check_output: Any) -> List[Dict[str, Any]]:
    """Righe ``pf-cert-evidence <scenario> <manifest_b64> <signature_b64>`` del testo del check."""
    if not isinstance(check_output, str):
        return []
    out = []
    for m in _EVIDENCE_RE.finditer(check_output):
        try:
            out.append({"scenario": m["scenario"], "manifest": base64.b64decode(m["m"], validate=True),
                        "signature": base64.b64decode(m["s"], validate=True)})
        except ValueError:
            out.append({"scenario": m["scenario"], "manifest": b"", "signature": b""})
    return out


def certification_verdict(evidence: Iterable[Mapping[str, Any]], *, mandatory: Iterable[str], repository: str,
                          head_sha: str, now: Optional[datetime] = None,
                          public_key_spki_b64: str = ORCHESTRATOR_RESULT_KEY_SPKI_B64) -> Dict[str, Any]:
    """``{verdict, per_scenario}``: ``CERTIFIED`` | ``NOT_CERTIFIED`` (con il motivo per ogni obbligatorio)."""
    now = now or datetime.now(timezone.utc)
    per: Dict[str, Dict[str, Any]] = {}
    for item in evidence:
        v = verify_manifest(item.get("manifest", b""), item.get("signature", b""), repository=repository,
                            head_sha=head_sha, now=now, public_key_spki_b64=public_key_spki_b64)
        claimed = item.get("scenario")
        if v["ok"] and v["scenario"] != claimed:
            v = {"ok": False, "code": "SCENARIO_LABEL_MISMATCH", "scenario": claimed, "result": None}
        key = v["scenario"] or str(claimed)
        # due prove per lo stesso scenario: vale la peggiore (nessun PASS «scelto» fra prove contraddittorie)
        if key in per and not (per[key]["ok"] and per[key]["result"] == "PASS"):
            continue
        per[key] = v
    missing = []
    for scenario in sorted(set(mandatory)):
        v = per.get(scenario)
        if v is None:
            missing.append(f"{scenario}:NOT_RUN")
        elif not v["ok"]:
            missing.append(f"{scenario}:{v['code']}")
        elif v["result"] != "PASS":
            missing.append(f"{scenario}:{v['result']}")
    return {"verdict": "NOT_CERTIFIED" if missing else "CERTIFIED", "reasons": missing, "per_scenario": per}
